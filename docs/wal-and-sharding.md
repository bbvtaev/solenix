# WAL и шардирование в Pulse Core

## Содержание

1. [Обзор архитектуры](#обзор-архитектуры)
2. [Шардирование in-memory хранилища](#шардирование-in-memory-хранилища)
3. [Metric Index](#metric-index)
4. [Write-Ahead Log](#write-ahead-log)
   - [Формат записи](#формат-записи)
   - [Сегменты и ротация](#сегменты-и-ротация)
   - [Flush и fsync](#flush-и-fsync)
   - [Replay при старте](#replay-при-старте)
5. [Путь записи (Write Path)](#путь-записи-write-path)
6. [Жизненный цикл данных](#жизненный-цикл-данных)
7. [Background Loop](#background-loop)
8. [Гарантии надёжности](#гарантии-надёжности)
9. [Конфигурация](#конфигурация)

---

## Обзор архитектуры

Pulse Core хранит данные в двух уровнях:

```
                        Write()
                           │
                    ┌──────▼──────┐
                    │ WAL Manager │  ← durability: сначала диск
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  128 шардов │  ← скорость: чтение из памяти
                    │  (in-memory)│
                    └──────┬──────┘
                           │ (bgLoop каждые FlushInterval)
                    ┌──────▼──────┐
                    │   Chunks    │  ← сжатые исторические данные
                    │  (Gorilla)  │
                    └─────────────┘
```

**WAL** гарантирует, что записанные данные не потеряются при сбое — даже если процесс упал до flush в chunks.
**Шарды** обеспечивают параллельный доступ: разные серии блокируют разные мьютексы.
**Chunks** — долгосрочное хранилище в формате Gorilla-сжатия (подробнее в [gorilla-compression.md](gorilla-compression.md)).

---

## Шардирование in-memory хранилища

### Структура

```go
// internal/storage/tsdb.go

const numShards = 128

type seriesShard struct {
    mu     sync.RWMutex
    series map[seriesID]*series
}

type DB struct {
    shards    [numShards]seriesShard
    metricIdx *metricIndex
    // ...
}
```

Все серии делятся на 128 независимых шардов. Каждый шард — это отдельная `map[seriesID]*series` с собственным `sync.RWMutex`.

### Выбор шарда

```go
func (db *DB) shardFor(id seriesID) *seriesShard {
    return &db.shards[uint64(id)%numShards]
}
```

Принадлежность серии к шарду определяется по её `seriesID` — стабильному 64-битному хешу пары `metric + labels`. Один и тот же набор меток всегда попадает в один шард.

### Идентификация серии: HashSeries

```go
// internal/model/record.go

func HashSeries(metric string, labels map[string]string) uint64 {
    keys := make([]string, 0, len(labels))
    for k := range labels {
        keys = append(keys, k)
    }
    sort.Strings(keys)  // сортировка для детерминированности

    h := fnv.New64a()
    h.Write([]byte(metric))
    h.Write([]byte{0})  // разделитель
    for _, k := range keys {
        h.Write([]byte(k))
        h.Write([]byte("="))
        h.Write([]byte(labels[k]))
        h.Write([]byte{0})  // разделитель
    }
    return h.Sum64()
}
```

Ключевые свойства:
- **Детерминированность** — ключи лейблов сортируются перед хешированием, поэтому порядок передачи лейблов в `Write()` не влияет на результат.
- **FNV-64a** — некриптографический хеш, оптимизированный под строки. Быстрее MD5/SHA при минимальных коллизиях для типичных метрик.
- **Разделитель `\0`** — нулевые байты между полями предотвращают коллизии вида `{"ab": "cd"}` ≠ `{"a": "bcd"}`.

### Параллелизм

Операции над разными сериями не блокируют друг друга, если они попадают в разные шарды.

| Операция | Блокировка |
|---|---|
| `Write()` | `sh.mu.Lock()` (один шард) |
| `Query()` | `sh.mu.RLock()` (каждый шард по очереди) |
| `Delete()` | `sh.mu.Lock()` (каждый совпадающий шард) |
| Retention | `sh.mu.Lock()` (последовательно, все шарды) |

`Query()` и `Write()` в разные шарды выполняются параллельно. Множественные `Query()` в один шард — тоже (RLock).

---

## Metric Index

Без индекса `Query("cpu.usage", ...)` пришлось бы перебирать все 128 шардов и все серии внутри. Metric Index решает эту проблему.

```go
// internal/storage/index.go

type metricIndex struct {
    mu  sync.RWMutex
    idx map[string]map[seriesID]struct{}
}
```

Структура: `metric name → set of seriesID`.

```
"cpu.usage" → { 0xA1B2C3..., 0xD4E5F6..., 0x11223344... }
"mem.free"  → { 0x99AABB... }
```

**Операции:**

| Метод | Когда вызывается |
|---|---|
| `add(metric, id)` | При первом Write для новой серии |
| `remove(metric, id)` | При Delete или Retention, когда серия стала пустой |
| `lookup(metric)` | В начале каждого `Query()` |
| `list()` | В `db.Metrics()` |

Индекс имеет собственный `sync.RWMutex`, независимый от шардов.

**Путь Query:**
```
Query("cpu.usage", labels, from, to)
    │
    ├─ metricIdx.lookup("cpu.usage")  → [id1, id2, id3]
    │
    └─ для каждого id:
           shardFor(id).mu.RLock()
           проверить labelsMatch()
           filterPoints() с бинарным поиском
           shardFor(id).mu.RUnlock()
```

Без индекса сложность Query — O(все серии). С индексом — O(серии этой метрики).

---

## Write-Ahead Log

WAL обеспечивает **durability**: данные считаются записанными только после того, как они попали в WAL-файл. Если процесс упадёт сразу после `Write()`, данные восстановятся при следующем старте из WAL.

### Формат записи

Каждая WAL-запись состоит из заголовка и payload:

```
┌─────────────────────────────────────────────────┐
│ Header (8 байт)                                 │
│   payload_len : uint32  (4 байта, little-endian) │
│   crc32       : uint32  (4 байта, CRC-32/IEEE)   │
├─────────────────────────────────────────────────┤
│ Payload (payload_len байт)                      │
│   metric_len  : uint16                           │
│   metric      : []byte                           │
│   labels_count: uint16                           │
│   per label:                                     │
│     key_len   : uint16                           │
│     key       : []byte                           │
│     val_len   : uint16                           │
│     val       : []byte                           │
│   points_count: uint16                           │
│   per point:                                     │
│     timestamp : int64   (8 байт, наносекунды)    │
│     value     : float64 (8 байт, IEEE 754)       │
└─────────────────────────────────────────────────┘
```

**CRC-32** вычисляется от payload и записывается в заголовок. При Replay каждая запись верифицируется — повреждённый файл даёт ошибку, а не молча вернёт неверные данные.

### Сегменты и ротация

WAL состоит из нумерованных файлов-сегментов:

```
data/wal/
├── 000001.wal  ← удалён после flush в chunks
├── 000002.wal  ← удалён после flush в chunks
└── 000003.wal  ← текущий активный сегмент
```

Ротация (смена сегмента) происходит в двух случаях:

1. **По таймеру** — каждые `FlushInterval` (default: 2 минуты) `bgLoop` вызывает `flushToChunks()`.
2. **По размеру** — каждые 100 мс WAL проверяет `ShouldRotate()`: если текущий сегмент ≥ `WALMaxSize` (default: 32 MiB), инициируется ротация.

Процесс ротации ([manager.go](../internal/wal/manager.go)):

```
Rotate()
    │
    ├─ закрыть текущий сегмент (flush + close)
    │
    ├─ seq++, открыть новый сегмент (000004.wal)
    │
    └─ вернуть путь запечатанного сегмента (000003.wal)
```

После ротации запечатанный сегмент передаётся в `flushToChunks()`, который:
1. Читает все записи из сегмента через `wal.Replay()`
2. Группирует по метрике → серии
3. Записывает в chunk-файлы (Gorilla-сжатие)
4. Удаляет запечатанный WAL-сегмент

### Flush и fsync

```go
// internal/wal/wal.go

func (w *wal) flush() {
    w.mu.Lock()
    defer w.mu.Unlock()
    _ = w.buf.Flush()  // сброс 1 MiB буфера в OS page cache
    _ = w.f.Sync()     // fsync: OS → диск
}
```

WAL использует двухуровневую буферизацию:

| Уровень | Размер | Управление |
|---|---|---|
| `bufio.Writer` (userspace) | 1 MiB | сбрасывается при `Flush()` |
| OS page cache | kernel | сбрасывается при `Sync()` (fsync) |

`bgLoop` вызывает `wm.Flush()` каждые 100 мс. Это означает, что в худшем случае при сбое теряются данные за последние ~100 мс — стандартный компромисс между производительностью и надёжностью.

### Replay при старте

`Open()` восстанавливает состояние в определённом порядке:

```go
// internal/storage/tsdb.go : Open()

// 1. Загрузить исторические данные из chunks
chunkRecords, _ := chunk.ReadAllChunks(chunksDir)
for _, rec := range chunkRecords {
    db.applyRecord(rec)
}

// 2. Поверх — replay оставшихся WAL сегментов
walPaths, _ := wal.ListSegmentPaths(walDir)
for _, path := range walPaths {
    records, _ := wal.Replay(path)
    for _, rec := range records {
        db.applyRecord(rec)
    }
}

// 3. Открыть WAL manager для новых записей
db.wm, _ = wal.Open(walDir, config.WALMaxSize)
```

Порядок важен: chunks → WAL. WAL всегда содержит данные новее chunks (старые сегменты удаляются после flush). Если сегмент не был удалён (процесс упал во время flush), его данные будут применены повторно через `applyRecord` — это идемпотентно для уже существующих точек.

---

## Путь записи (Write Path)

Полная последовательность для `db.Write("cpu.usage", labels, 0.75)`:

```
Write("cpu.usage", {"host": "web-1"}, 0.75)
│
├─ 1. Создать []Point{} с ts = time.Now().UnixNano()
│
├─ 2. writeBatch()
│   │
│   ├─ 2a. cloneLabels() + copy(points)  ← защита от мутации caller'ом
│   │
│   ├─ 2b. wm.Write(rec)                ← WAL FIRST
│   │       │
│   │       └─ wal.write(rec)
│   │           ├─ encodeRecord() → payload
│   │           ├─ crc32(payload) → header
│   │           └─ buf.Write(header + payload)  ← в 1 MiB буфер
│   │
│   ├─ 2c. applyRecord(rec)             ← обновить память
│   │       │
│   │       ├─ HashSeries(metric, labels) → seriesID
│   │       ├─ shardFor(seriesID).mu.Lock()
│   │       ├─ insertPointSorted(&ser.points, p)
│   │       ├─ shardFor(seriesID).mu.Unlock()
│   │       └─ metricIdx.add(metric, id)  [если новая серия]
│   │
│   ├─ 2d. notify()                     ← pub/sub подписчики
│   │
│   └─ 2e. broadcastWatchers()          ← HTTP SSE / Watch()
│
└─ return nil
```

**Инвариант**: данные попадают в WAL до обновления памяти. Если процесс упадёт между шагами 2b и 2c, данные восстановятся при следующем старте.

---

## Жизненный цикл данных

```
Write()
  │
  ▼
WAL сегмент N  (например, 000003.wal)
  │
  │  каждые FlushInterval (2m) или при достижении WALMaxSize (32 MiB)
  ▼
Rotate(): запечатать 000003.wal, открыть 000004.wal
  │
  ▼
flushToChunks(000003.wal):
  ├─ Replay записей из сегмента
  ├─ Gorilla-сжать по сериям
  ├─ Записать в data/chunks/<metric>/<seriesID>.chunk
  └─ os.Remove(000003.wal)
  │
  │  при RetentionDuration (если задан)
  ▼
enforceRetention():
  ├─ Удалить точки старше cutoff из памяти
  └─ metricIdx.remove() для опустевших серий
```

Данные в памяти никогда не удаляются принудительно — шарды держат все серии, загруженные с момента старта. Retention работает только со старыми точками, но не выгружает данные из RAM на диск.

---

## Background Loop

`bgLoop` — единственная фоновая горутина БД:

```go
// internal/storage/tsdb.go

func (db *DB) bgLoop() {
    walSyncTicker    := time.NewTicker(100 * time.Millisecond)
    chunkFlushTicker := time.NewTicker(db.config.FlushInterval)
    retentionTicker  := ...  // RetentionDuration / 10, минимум 1 минута

    for {
        select {
        case <-db.closeCh:
            db.wm.Flush()   // финальный flush перед выходом
            return
        case <-walSyncTicker.C:
            db.wm.Flush()
            if db.wm.ShouldRotate() {
                db.flushToChunks()
            }
        case <-chunkFlushTicker.C:
            db.flushToChunks()
        case <-retentionC:
            db.enforceRetention()
        }
    }
}
```

Тикеры и их роли:

| Тикер | Интервал | Действие |
|---|---|---|
| `walSyncTicker` | 100 мс | fsync WAL буфера; ротация если WAL ≥ MaxSize |
| `chunkFlushTicker` | `FlushInterval` (2m) | принудительная ротация + flush в chunks |
| `retentionTicker` | `RetentionDuration / 10` | удаление устаревших точек |

`Close()` закрывает `closeCh`, ждёт завершения `bgLoop` через `walDone`, затем закрывает WAL.

---

## Гарантии надёжности

| Сценарий | Поведение |
|---|---|
| Процесс убит сразу после `Write()` | Данные восстановятся из WAL при следующем старте (потеря ≤ 100 мс) |
| Повреждение WAL-файла | `Replay()` вернёт ошибку CRC, старт завершится с ошибкой |
| Повреждение chunk-файла | `ReadAllChunks()` вернёт ошибку, старт завершится с ошибкой |
| Сбой во время `flushToChunks()` | Запечатанный сегмент не удалён → будет replayed повторно при старте |
| Гонка данных | Устранена: WAL Manager имеет собственный мьютекс, каждый шард — свой |

---

## Конфигурация

Параметры WAL и шардирования задаются в `config.yaml`:

```yaml
data_dir: ~/pulse/data      # директория для data/wal/ и data/chunks/
wal_max_size: 33554432      # 32 MiB — ротация по размеру
flush_interval: 2m          # принудительный flush в chunks
retention: 720h             # 30 дней; 0 — без ограничения
```

Значения по умолчанию (`internal/config/config.go`):

| Параметр | Default | Описание |
|---|---|---|
| `DataDir` | `~/pulse/data` | корневая директория данных |
| `WALMaxSize` | 32 MiB | максимальный размер WAL сегмента |
| `FlushInterval` | 2 минуты | интервал flush in chunks |
| `RetentionDuration` | 0 (нет) | срок хранения точек |
| WAL fsync | 100 мс | hardcoded в `walSyncInterval` |
| Кол-во шардов | 128 | hardcoded в `numShards` |
