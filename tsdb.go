package synthetis

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const Version = "1.3.0-alpha"

type Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type WriteSeries struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type SeriesResult struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type seriesID uint64

type series struct {
	metric string
	labels map[string]string
	points []Point
}

type walRecord struct {
	Metric string
	Labels map[string]string
	Points []Point
}

const numShards = 128

type seriesShard struct {
	mu     sync.RWMutex
	series map[seriesID]*series
}

type DB struct {
	shards [numShards]seriesShard

	walMu  sync.Mutex
	wal    *os.File
	walBuf *bufio.Writer
	path   string

	walCh   chan walRecord
	walDone chan struct{}
}

func Open(path ...string) (*DB, error) {
	if len(path) > 1 {
		return nil, fmt.Errorf("more then 1 path string is not allowed")
	}

	var pt string
	if len(path) == 0 {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		pt = filepath.Join(home, "synthetis", "metrics.bin")
	} else {
		pt = path[0]
	}

	if err := os.MkdirAll(filepath.Dir(pt), 0o755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(pt, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	db := &DB{
		wal:     f,
		walBuf:  bufio.NewWriterSize(f, 1<<20), // 1 MiB
		path:    pt,
		walCh:   make(chan walRecord, 4096),
		walDone: make(chan struct{}),
	}

	for i := range db.shards {
		db.shards[i].series = make(map[seriesID]*series)
	}

	if err := db.replayWAL(); err != nil {
		_ = f.Close()
		return nil, err
	}

	go db.walLoop()

	return db, nil
}

func (db *DB) shardFor(id seriesID) *seriesShard {
	return &db.shards[uint64(id)%numShards]
}

func (db *DB) Close() error {
	db.walMu.Lock()
	if db.walCh != nil {
		close(db.walCh)
	}
	db.walMu.Unlock()

	if db.walDone != nil {
		<-db.walDone
	}

	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.walBuf != nil {
		_ = db.walBuf.Flush()
	}
	if db.wal == nil {
		return nil
	}

	err := db.wal.Close()
	db.wal = nil
	db.walBuf = nil
	return err
}

func (db *DB) Write(metric string, labels map[string]string, value ...float64) error {
	points := make([]Point, len(value))

	ts := time.Now().UnixNano()
	for i, v := range value {
		points[i] = Point{
			Timestamp: ts,
			Value:     v,
		}
	}

	batch := WriteSeries{
		Metric: metric,
		Labels: labels,
		Points: points,
	}

	if len(batch.Points) == 0 {
		return fmt.Errorf("points must be more than zero")
	}

	labelsCopy := cloneLabels(batch.Labels)
	pointsCopy := make([]Point, len(batch.Points))
	copy(pointsCopy, batch.Points)

	rec := walRecord{
		Metric: batch.Metric,
		Labels: labelsCopy,
		Points: pointsCopy,
	}

	db.walCh <- rec

	id := hashSeries(batch.Metric, labelsCopy)
	sh := db.shardFor(id)

	sh.mu.Lock()
	ser, ok := sh.series[id]
	if !ok {
		ser = &series{
			metric: batch.Metric,
			labels: labelsCopy,
			points: make([]Point, 0, len(batch.Points)),
		}
		sh.series[id] = ser
	}
	for _, p := range batch.Points {
		insertPointSorted(&ser.points, p)
	}
	sh.mu.Unlock()

	return nil
}

func (db *DB) Query(metric string, labels map[string]string, timeBefore int) ([]SeriesResult, error) {
	if metric == "" {
		return nil, errors.New("metric is required")
	}

	var res []SeriesResult

	for i := range db.shards {
		sh := &db.shards[i]
		sh.mu.RLock()
		for _, s := range sh.series {
			if s.metric != metric {
				continue
			}
			if !labelsMatch(labels, s.labels) {
				continue
			}

			points := filterPointsByTime(s.points, timeBefore)
			if len(points) == 0 {
				continue
			}

			res = append(res, SeriesResult{
				Metric: s.metric,
				Labels: cloneLabels(s.labels),
				Points: points,
			})
		}
		sh.mu.RUnlock()
	}

	return res, nil
}

func (db *DB) walLoop() {
	defer close(db.walDone)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-db.walCh:
			if !ok {
				db.flushWAL()
				return
			}
			db.writeWALRecord(rec)
		case <-ticker.C:
			db.flushWAL()
		}
	}
}

// encodeWALRecord serializing writes in bytes
// format:
// [Len Metric (2)] [Metric Bytes]
// [Count Labels (2)] -> ([Len Key (2)] [Key] [Len Val (2)] [Val]) * N
// [Count Points (2)] -> ([Timestamp (8)] [Value (8)]) * N
func encodeWALRecord(rec walRecord) []byte {
	// 2 + len(metric) + 2 + (len_labels * avg_label_len) + 2 + (len_points * 16)
	estimatedSize := 2 + len(rec.Metric) + 2 + len(rec.Labels)*20 + 2 + len(rec.Points)*16
	buf := make([]byte, 0, estimatedSize)

	// metric
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Metric)))
	buf = append(buf, rec.Metric...)

	// labels
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Labels)))
	for k, v := range rec.Labels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}

	// points
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Points)))
	for _, p := range rec.Points {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(p.Timestamp))
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(p.Value))
	}

	return buf
}

func decodeWALRecord(data []byte) (walRecord, error) {
	var rec walRecord
	offset := 0
	maxLen := len(data)

	checkBounds := func(n int) bool {
		return offset+n <= maxLen
	}

	// metric
	if !checkBounds(2) {
		return rec, io.ErrUnexpectedEOF
	}
	metricLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	if !checkBounds(metricLen) {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Metric = string(data[offset : offset+metricLen])
	offset += metricLen

	// labels
	if !checkBounds(2) {
		return rec, io.ErrUnexpectedEOF
	}
	labelsCount := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	rec.Labels = make(map[string]string, labelsCount)
	for i := 0; i < labelsCount; i++ {
		// key
		if !checkBounds(2) {
			return rec, io.ErrUnexpectedEOF
		}
		kLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if !checkBounds(kLen) {
			return rec, io.ErrUnexpectedEOF
		}
		key := string(data[offset : offset+kLen])
		offset += kLen

		// value
		if !checkBounds(2) {
			return rec, io.ErrUnexpectedEOF
		}
		vLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if !checkBounds(vLen) {
			return rec, io.ErrUnexpectedEOF
		}
		val := string(data[offset : offset+vLen])
		offset += vLen

		rec.Labels[key] = val
	}

	// points
	if !checkBounds(2) {
		return rec, io.ErrUnexpectedEOF
	}
	pointsCount := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	rec.Points = make([]Point, pointsCount)
	if !checkBounds(pointsCount * 16) {
		return rec, io.ErrUnexpectedEOF
	}

	for i := 0; i < pointsCount; i++ {
		ts := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		valBits := binary.LittleEndian.Uint64(data[offset:])
		offset += 8

		rec.Points[i] = Point{
			Timestamp: ts,
			Value:     math.Float64frombits(valBits),
		}
	}

	return rec, nil
}

func (db *DB) writeWALRecord(rec walRecord) {
	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.wal == nil || db.walBuf == nil {
		return
	}

	payload := encodeWALRecord(rec)
	payloadLen := uint32(len(payload))

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], payloadLen)

	if _, err := db.walBuf.Write(lenBuf[:]); err != nil {
		return
	}

	if _, err := db.walBuf.Write(payload); err != nil {
		return
	}
}

func (db *DB) flushWAL() {
	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.walBuf != nil {
		_ = db.walBuf.Flush()
	}
	if db.wal != nil {
		_ = db.wal.Sync()
	}
}

func (db *DB) replayWAL() error {
	f, err := os.Open(db.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(reader, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		payloadLen := binary.LittleEndian.Uint32(lenBuf)

		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(reader, payload)
		if err != nil {
			return fmt.Errorf("unexpected EOF while reading WAL payload")
		}

		rec, err := decodeWALRecord(payload)
		if err != nil {
			return fmt.Errorf("corrupted WAL record: %v", err)
		}

		db.applyRecord(rec)
	}

	return nil
}

func (db *DB) applyRecord(rec walRecord) {
	id := hashSeries(rec.Metric, rec.Labels)
	sh := db.shardFor(id)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	ser, ok := sh.series[id]
	if !ok {
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
}

func (db *DB) DrainWAL() {
	for len(db.walCh) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	db.flushWAL()
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

func filterPointsByTime(points []Point, timeBefore int) []Point {
	if len(points) == 0 {
		return nil
	}

	if timeBefore == 0 {
		return points
	}

	cutoff := time.Now().Add(-time.Duration(timeBefore) * time.Minute).UnixNano()

	start := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp >= cutoff
	})
	if start == len(points) {
		return nil
	}

	out := make([]Point, len(points)-start)
	copy(out, points[start:])
	return out
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
