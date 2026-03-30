package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbvtaev/pulse-core/internal/chunk"
	cfg "github.com/bbvtaev/pulse-core/internal/config"
	"github.com/bbvtaev/pulse-core/internal/model"
	"github.com/bbvtaev/pulse-core/internal/wal"
)

const walSyncInterval = 100 * time.Millisecond

const numShards = 128

type seriesID uint64

type series struct {
	metric string
	labels map[string]string
	points []model.Point
}

type seriesShard struct {
	mu     sync.RWMutex
	series map[seriesID]*series
}

type subscription struct {
	metric string
	labels map[string]string
	ch     chan model.Point
}

// DB — основной объект базы данных.
type DB struct {
	shards    [numShards]seriesShard
	metricIdx *metricIndex

	wm     *wal.Manager
	cw     *chunk.Writer
	config cfg.Config

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
func Open(config cfg.Config) (*DB, error) {
	def := cfg.DefaultConfig()
	if config.DataDir == "" {
		config.DataDir = def.DataDir
	}
	if config.WALMaxSize == 0 {
		config.WALMaxSize = def.WALMaxSize
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = def.FlushInterval
	}
	if config.Mode == "" {
		config.Mode = def.Mode
	}
	if config.GRPCAddr == "" {
		config.GRPCAddr = def.GRPCAddr
	}

	walDir := filepath.Join(config.DataDir, "wal")
	chunksDir := filepath.Join(config.DataDir, "chunks")

	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(chunksDir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		metricIdx: newMetricIndex(),
		config:    config,
		closeCh:   make(chan struct{}),
		walDone:   make(chan struct{}),
		subs:      make(map[uint64]*subscription),
		watchers:  make(map[uint64]chan struct{}),
	}
	for i := range db.shards {
		db.shards[i].series = make(map[seriesID]*series)
	}

	// 1. Загружаем chunks (исторические данные)
	chunkRecords, err := chunk.ReadAllChunks(chunksDir)
	if err != nil {
		return nil, fmt.Errorf("load chunks: %w", err)
	}
	for _, rec := range chunkRecords {
		db.applyRecord(rec)
	}

	// 2. Replay всех WAL сегментов
	walPaths, err := wal.ListSegmentPaths(walDir)
	if err != nil {
		return nil, fmt.Errorf("list wal segments: %w", err)
	}
	for _, path := range walPaths {
		records, err := wal.Replay(path)
		if err != nil {
			return nil, fmt.Errorf("replay WAL %s: %w", path, err)
		}
		for _, rec := range records {
			db.applyRecord(rec)
		}
	}

	// 3. Открываем WAL manager
	wm, err := wal.Open(walDir, config.WALMaxSize)
	if err != nil {
		return nil, err
	}
	db.wm = wm
	db.cw = chunk.NewWriter(chunksDir)

	go db.bgLoop()

	return db, nil
}

func (db *DB) bgLoop() {
	defer close(db.walDone)

	walSyncTicker := time.NewTicker(walSyncInterval)
	defer walSyncTicker.Stop()

	chunkFlushTicker := time.NewTicker(db.config.FlushInterval)
	defer chunkFlushTicker.Stop()

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
			db.wm.Flush()
			return
		case <-walSyncTicker.C:
			db.wm.Flush()
			if db.wm.ShouldRotate() {
				_ = db.flushToChunks()
			}
		case <-chunkFlushTicker.C:
			_ = db.flushToChunks()
		case <-retentionC:
			db.enforceRetention()
		}
	}
}

// flushToChunks ротирует WAL, пишет sealed сегмент в chunks и удаляет его.
func (db *DB) flushToChunks() error {
	sealedPath, err := db.wm.Rotate()
	if err != nil {
		return fmt.Errorf("rotate WAL: %w", err)
	}

	records, err := wal.Replay(sealedPath)
	if err != nil {
		return fmt.Errorf("read sealed WAL %s: %w", sealedPath, err)
	}
	if len(records) == 0 {
		_ = os.Remove(sealedPath)
		return nil
	}

	// Группируем записи по метрике → series
	metricSeries := make(map[string]map[seriesID]*series)
	for _, rec := range records {
		id := seriesID(model.HashSeries(rec.Metric, rec.Labels))
		if metricSeries[rec.Metric] == nil {
			metricSeries[rec.Metric] = make(map[seriesID]*series)
		}
		ser := metricSeries[rec.Metric][id]
		if ser == nil {
			ser = &series{
				metric: rec.Metric,
				labels: cloneLabels(rec.Labels),
				points: make([]model.Point, 0, len(rec.Points)),
			}
			metricSeries[rec.Metric][id] = ser
		}
		for _, p := range rec.Points {
			insertPointSorted(&ser.points, p)
		}
	}

	for metric, serMap := range metricSeries {
		serSlice := make([]*model.SeriesResult, 0, len(serMap))
		for _, ser := range serMap {
			serSlice = append(serSlice, &model.SeriesResult{
				Metric: ser.metric,
				Labels: ser.labels,
				Points: ser.points,
			})
		}
		if err := db.cw.Write(metric, serSlice); err != nil {
			return fmt.Errorf("write chunk for %s: %w", metric, err)
		}
	}

	return os.Remove(sealedPath)
}

// Close закрывает БД и сбрасывает WAL.
func (db *DB) Close() error {
	close(db.closeCh)
	<-db.walDone
	return db.wm.Close()
}

// Write записывает одно или несколько значений для метрики с текущим timestamp.
func (db *DB) Write(metric string, labels map[string]string, value ...float64) error {
	if metric == "" {
		return errors.New("metric is required")
	}
	if len(value) == 0 {
		return errors.New("at least one value is required")
	}

	ts := time.Now().UnixNano()
	points := make([]model.Point, len(value))
	for i, v := range value {
		points[i] = model.Point{Timestamp: ts, Value: v}
	}

	return db.writeBatch(metric, labels, points)
}

// WriteBatch записывает точки с произвольными timestamp (используется gRPC-сервером).
func (db *DB) WriteBatch(metric string, labels map[string]string, points []model.Point) error {
	if metric == "" {
		return errors.New("metric is required")
	}
	if len(points) == 0 {
		return errors.New("at least one point is required")
	}
	return db.writeBatch(metric, labels, points)
}

func (db *DB) writeBatch(metric string, labels map[string]string, points []model.Point) error {
	labelsCopy := cloneLabels(labels)
	pointsCopy := make([]model.Point, len(points))
	copy(pointsCopy, points)

	rec := model.Record{Metric: metric, Labels: labelsCopy, Points: pointsCopy}

	// WAL first — гарантирует durability ordering
	if err := db.wm.Write(rec); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	// Затем in-memory
	db.applyRecord(rec)

	// Уведомляем подписчиков
	db.notify(metric, labelsCopy, pointsCopy)
	db.broadcastWatchers()

	return nil
}

func (db *DB) applyRecord(rec model.Record) {
	id := seriesID(model.HashSeries(rec.Metric, rec.Labels))
	sh := db.shardFor(id)

	sh.mu.Lock()
	ser, exists := sh.series[id]
	if !exists {
		ser = &series{
			metric: rec.Metric,
			labels: cloneLabels(rec.Labels),
			points: make([]model.Point, 0, len(rec.Points)),
		}
		sh.series[id] = ser
	}
	for _, p := range rec.Points {
		insertPointSorted(&ser.points, p)
	}
	sh.mu.Unlock()

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

// Subscribe возвращает id подписки и канал с новыми точками.
func (db *DB) Subscribe(metric string, labels map[string]string) (uint64, <-chan model.Point) {
	ch := make(chan model.Point, 256)
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

func (db *DB) notify(metric string, labels map[string]string, points []model.Point) {
	db.subsMu.RLock()
	defer db.subsMu.RUnlock()

	for _, sub := range db.subs {
		if sub.metric != metric || !labelsMatch(sub.labels, labels) {
			continue
		}
		for _, p := range points {
			select {
			case sub.ch <- p:
			default:
			}
		}
	}
}

// Metrics возвращает список всех метрик в БД.
func (db *DB) Metrics() []string {
	return db.metricIdx.list()
}

// Watch возвращает канал, который получает сигнал при каждом Write.
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
	db.wm.Flush()
}

func (db *DB) shardFor(id seriesID) *seriesShard {
	return &db.shards[uint64(id)%numShards]
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

func insertPointSorted(points *[]model.Point, p model.Point) {
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

	ps = append(ps, model.Point{})
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
