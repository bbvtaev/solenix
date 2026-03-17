package pulse

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const numShards = 128

type seriesID uint64

type series struct {
	metric string
	labels map[string]string
	points []Point
}

type seriesShard struct {
	mu     sync.RWMutex
	series map[seriesID]*series
}

type subscription struct {
	metric string
	labels map[string]string
	ch     chan Point
}

type DB struct {
	shards    [numShards]seriesShard
	metricIdx *metricIndex

	w      *wal
	path   string
	config Config

	closeCh chan struct{}
	walDone chan struct{}

	subsMu sync.RWMutex
	subs   map[uint64]*subscription
	subSeq atomic.Uint64

	watchersMu sync.RWMutex
	watchers   map[uint64]chan struct{}
	watcherSeq atomic.Uint64
}

// Open открывает (или создаёт) БД согласно конфигу.
// Незаполненные поля получают значения из DefaultConfig().
func Open(cfg Config) (*DB, error) {
	def := DefaultConfig()
	if cfg.WALPath == "" {
		cfg.WALPath = def.WALPath
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = def.FlushInterval
	}
	if cfg.Mode == "" {
		cfg.Mode = def.Mode
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = def.GRPCAddr
	}

	if err := os.MkdirAll(filepath.Dir(cfg.WALPath), 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		metricIdx: newMetricIndex(),
		path:      cfg.WALPath,
		config:    cfg,
		closeCh:   make(chan struct{}),
		walDone:   make(chan struct{}),
		subs:      make(map[uint64]*subscription),
		watchers:  make(map[uint64]chan struct{}),
	}
	for i := range db.shards {
		db.shards[i].series = make(map[seriesID]*series)
	}

	// Сначала replay WAL в память
	records, err := replayWAL(cfg.WALPath)
	if err != nil {
		return nil, fmt.Errorf("replay WAL: %w", err)
	}
	for _, rec := range records {
		db.applyRecord(rec)
	}

	// Затем открываем WAL для записи (O_APPEND)
	w, err := openWAL(cfg.WALPath)
	if err != nil {
		return nil, err
	}
	db.w = w

	go db.bgLoop()

	return db, nil
}

func (db *DB) bgLoop() {
	defer close(db.walDone)

	flushTicker := time.NewTicker(db.config.FlushInterval)
	defer flushTicker.Stop()

	var retentionC <-chan time.Time
	if db.config.RetentionDuration > 0 {
		interval := db.config.RetentionDuration / 10
		if interval < time.Minute {
			interval = time.Minute
		}
		rt := time.NewTicker(interval)
		defer rt.Stop()
		retentionC = rt.C
	}

	for {
		select {
		case <-db.closeCh:
			db.w.flush()
			return
		case <-flushTicker.C:
			db.w.flush()
		case <-retentionC:
			db.enforceRetention()
		}
	}
}

func (db *DB) Close() error {
	close(db.closeCh)
	<-db.walDone
	return db.w.close()
}

// Write записывает одно или несколько значений для метрики с текущим timestamp.
// WAL пишется синхронно (в буфер) до обновления памяти — гарантирует порядок durability.
func (db *DB) Write(metric string, labels map[string]string, value ...float64) error {
	if metric == "" {
		return errors.New("metric is required")
	}
	if len(value) == 0 {
		return errors.New("at least one value is required")
	}

	ts := time.Now().UnixNano()
	points := make([]Point, len(value))
	for i, v := range value {
		points[i] = Point{Timestamp: ts, Value: v}
	}

	return db.writeBatch(metric, labels, points)
}

// WriteBatch записывает точки с произвольными timestamp (используется gRPC-сервером).
func (db *DB) WriteBatch(metric string, labels map[string]string, points []Point) error {
	if metric == "" {
		return errors.New("metric is required")
	}
	if len(points) == 0 {
		return errors.New("at least one point is required")
	}
	return db.writeBatch(metric, labels, points)
}

func (db *DB) writeBatch(metric string, labels map[string]string, points []Point) error {
	labelsCopy := cloneLabels(labels)
	pointsCopy := make([]Point, len(points))
	copy(pointsCopy, points)

	rec := walRecord{Metric: metric, Labels: labelsCopy, Points: pointsCopy}

	// WAL first — гарантирует durability ordering
	if err := db.w.write(rec); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	// Затем in-memory
	db.applyRecord(rec)

	// Уведомляем подписчиков
	db.notify(metric, labelsCopy, pointsCopy)
	db.broadcastWatchers()

	return nil
}

func (db *DB) applyRecord(rec walRecord) {
	id := hashSeries(rec.Metric, rec.Labels)
	sh := db.shardFor(id)

	sh.mu.Lock()
	ser, exists := sh.series[id]
	if !exists {
		ser = &series{
			metric: rec.Metric,
			labels: cloneLabels(rec.Labels),
			points: make([]Point, 0, len(rec.Points)),
		}
		sh.series[id] = ser
	}
	for _, p := range rec.Points {
		insertPointSorted(&ser.points, p)
	}
	sh.mu.Unlock()

	// Добавляем в индекс только для новых серий (после освобождения шард-мьютекса)
	if !exists {
		db.metricIdx.add(rec.Metric, id)
	}
}

func (db *DB) enforceRetention() {
	cutoff := time.Now().Add(-db.config.RetentionDuration).UnixNano()

	for i := range db.shards {
		sh := &db.shards[i]
		sh.mu.Lock()

		var toRemove []struct {
			id     seriesID
			metric string
		}
		for id, ser := range sh.series {
			start := sort.Search(len(ser.points), func(j int) bool {
				return ser.points[j].Timestamp >= cutoff
			})
			if start > 0 {
				ser.points = ser.points[start:]
			}
			if len(ser.points) == 0 {
				delete(sh.series, id)
				toRemove = append(toRemove, struct {
					id     seriesID
					metric string
				}{id, ser.metric})
			}
		}
		sh.mu.Unlock()

		for _, r := range toRemove {
			db.metricIdx.remove(r.metric, r.id)
		}
	}
}

// Subscribe возвращает id подписки и канал, в который будут приходить новые точки.
func (db *DB) Subscribe(metric string, labels map[string]string) (uint64, <-chan Point) {
	ch := make(chan Point, 256)
	sub := &subscription{metric: metric, labels: cloneLabels(labels), ch: ch}
	id := db.subSeq.Add(1)

	db.subsMu.Lock()
	db.subs[id] = sub
	db.subsMu.Unlock()

	return id, ch
}

// Unsubscribe закрывает подписку и канал.
func (db *DB) Unsubscribe(id uint64) {
	db.subsMu.Lock()
	if sub, ok := db.subs[id]; ok {
		delete(db.subs, id)
		close(sub.ch)
	}
	db.subsMu.Unlock()
}

func (db *DB) notify(metric string, labels map[string]string, points []Point) {
	db.subsMu.RLock()
	defer db.subsMu.RUnlock()

	for _, sub := range db.subs {
		if sub.metric != metric || !labelsMatch(sub.labels, labels) {
			continue
		}
		for _, p := range points {
			select {
			case sub.ch <- p:
			default: // дроп если подписчик медленный
			}
		}
	}
}

// Metrics возвращает список всех метрик хранящихся в БД.
func (db *DB) Metrics() []string {
	return db.metricIdx.list()
}

// Watch возвращает канал, который получает сигнал при каждом Write.
// Канал буферизован на 1 — множественные быстрые записи сливаются в один сигнал.
func (db *DB) Watch() (uint64, <-chan struct{}) {
	ch := make(chan struct{}, 1)
	id := db.watcherSeq.Add(1)
	db.watchersMu.Lock()
	db.watchers[id] = ch
	db.watchersMu.Unlock()
	return id, ch
}

// Unwatch отменяет подписку на Write-события.
func (db *DB) Unwatch(id uint64) {
	db.watchersMu.Lock()
	if ch, ok := db.watchers[id]; ok {
		delete(db.watchers, id)
		close(ch)
	}
	db.watchersMu.Unlock()
}

func (db *DB) broadcastWatchers() {
	db.watchersMu.RLock()
	defer db.watchersMu.RUnlock()
	for _, ch := range db.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// DrainWAL принудительно сбрасывает WAL на диск (fsync).
func (db *DB) DrainWAL() {
	db.w.flush()
}

func (db *DB) shardFor(id seriesID) *seriesShard {
	return &db.shards[uint64(id)%numShards]
}

func hashSeries(metric string, labels map[string]string) seriesID {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := fnv.New64a()
	_, _ = h.Write([]byte(metric))
	_, _ = h.Write([]byte{0})
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte("="))
		_, _ = h.Write([]byte(labels[k]))
		_, _ = h.Write([]byte{0})
	}
	return seriesID(h.Sum64())
}

func labelsMatch(filter, actual map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func insertPointSorted(points *[]Point, p Point) {
	ps := *points
	n := len(ps)

	if n == 0 || ps[n-1].Timestamp <= p.Timestamp {
		*points = append(ps, p)
		return
	}

	i := sort.Search(n, func(i int) bool {
		return ps[i].Timestamp >= p.Timestamp
	})
	if i == n {
		*points = append(ps, p)
		return
	}

	ps = append(ps, Point{})
	copy(ps[i+1:], ps[i:])
	ps[i] = p
	*points = ps
}

func cloneLabels(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
