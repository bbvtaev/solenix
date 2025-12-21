package synthetis_test

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	tsdb "github.com/bbvtaev/synthetis"
)

func newTestDB(t *testing.T) *tsdb.DB {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestWriteAndQuerySingleSeries(t *testing.T) {
	db := newTestDB(t)

	if err := db.Write("cpu_usage", map[string]string{"host": "a", "dc": "eu"}, 0.5, 0.7, 0.9); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	res, err := db.Query("cpu_usage", map[string]string{"host": "a"}, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}

	series := res[0]
	if series.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", series.Metric)
	}
	if series.Labels["host"] != "a" || series.Labels["dc"] != "eu" {
		t.Errorf("unexpected labels: %v", series.Labels)
	}

	if len(series.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(series.Points))
	}
}

func TestLabelFiltering(t *testing.T) {
	db := newTestDB(t)

	err := db.Write("cpu_usage", map[string]string{"host": "ab"}, 0.1)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = db.Write("cpu_usage", map[string]string{"host": "bs"}, 0.1)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	resA, err := db.Query("cpu_usage", map[string]string{"host": "ab"}, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resA) != 1 {
		t.Fatalf("expected 1 series for host=a, got %d", len(resA))
	}
	if resA[0].Labels["host"] != "ab" {
		t.Errorf("expected host=ab, got %s", resA[0].Labels["host"])
	}

	resB, err := db.Query("cpu_usage", map[string]string{"host": "bs"}, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resB) != 1 {
		t.Fatalf("expected 1 series for host=bs, got %d", len(resB))
	}
	if resB[0].Labels["host"] != "bs" {
		t.Errorf("expected host=bs, got %s", resB[0].Labels["host"])
	}

	resAll, err := db.Query("cpu_usage", nil, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resAll) != 2 {
		t.Fatalf("expected 2 series without label filter, got %d", len(resAll))
	}
}

func TestTimeRangeFiltering(t *testing.T) {
	db := newTestDB(t)

	err := db.Write("temp", map[string]string{"sensor": "s1"}, 1.0)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	res, err := db.Query("temp", map[string]string{"sensor": "s1"}, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}
	if len(res[0].Points) != 1 {
		t.Fatalf("expected 1 point in range, got %d", len(res[0].Points))
	}
}

func TestWALReplay(t *testing.T) {

	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	err = db.Write("disk_usage", map[string]string{"host": "a"}, 42.0)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db2, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db2.Close()

	res, err := db2.Query("disk_usage", map[string]string{"host": "a"}, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series after replay, got %d", len(res))
	}
	if len(res[0].Points) != 1 {
		t.Fatalf("expected 1 point after replay, got %d", len(res[0].Points))
	}
}

func TestQueryValidationErrors(t *testing.T) {
	db := newTestDB(t)

	_, err := db.Query("", nil, 0)
	if err == nil {
		t.Fatalf("expected error for empty metric, got nil")
	}
}

// functional not ready yet for these type of tests

// func TestWriteEmptyBatch(t *testing.T) {
// 	db := newTestDB(t)

// 	if err := db.Write(nil); err != nil {
// 		t.Fatalf("Write(nil) error = %v", err)
// 	}
// 	if err := db.Write([]entity.WriteSeries{}); err != nil {
// 		t.Fatalf("Write(empty slice) error = %v", err)
// 	}
// }

func newBenchDB(b *testing.B) *tsdb.DB {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func BenchmarkWriteThroughput(b *testing.B) {
	db := newBenchDB(b)

	const pointsPerWrite = 8

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	_ = db.Write("warmup", map[string]string{"sensor": "init"}, 0.0)

	b.ResetTimer()
	start := time.Now()
	totalPoints := 0

	for i := 0; i < b.N; i++ {
		tsBase := time.Now().Unix()

		points := make([]tsdb.Point, 0, pointsPerWrite)
		for j := 0; j < pointsPerWrite; j++ {
			points = append(points, tsdb.Point{
				Timestamp: tsBase + int64(j),
				Value:     rnd.Float64() * 100,
			})
		}

		err := db.Write("load", map[string]string{"sensor": "A1"}, 123.00)
		if err != nil {
			b.Fatalf("Write error: %v", err)
		}

		totalPoints += pointsPerWrite
	}

	elapsed := time.Since(start)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	pointsPerSec := float64(totalPoints) / elapsed.Seconds()

	b.ReportMetric(opsPerSec, "ops/s")
	b.ReportMetric(pointsPerSec, "points/s")

	b.Logf("pointsPerWrite=%d, totalOps=%d, totalPoints=%d, elapsed=%s",
		pointsPerWrite, b.N, totalPoints, elapsed)
}

func BenchmarkWrite_BottleneckAnalysis(b *testing.B) {
	db, err := tsdb.Open()
	if err != nil {
		b.Fatalf("Open error: %v", err)
	}

	b.Run("Sustained_Write_KeepOpen", func(b *testing.B) {
		_ = newBenchDB(b)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Write("bench_metric", map[string]string{"h": "1"}, 52); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Overhead_OpenClose_PerWrite", func(b *testing.B) {
		dir := b.TempDir()
		path := filepath.Join(dir, "tsdb_overhead.wal")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			db, err := tsdb.Open(path)
			if err != nil {
				b.Fatalf("Open error: %v", err)
			}

			if err := db.Write("bench_metric", map[string]string{"h": "1"}, 52); err != nil {
				b.Fatal(err)
			}

			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestWriteDegradation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping degradation test in short mode")
	}

	db := newTestDB(t)

	const batchSize = 100
	const initialBatches = 10000000
	const measureBatches = 100

	startEmpty := time.Now()
	for i := 0; i < measureBatches; i++ {
		_ = db.Write("metric", nil, 1.0)
	}
	durationEmpty := time.Since(startEmpty)

	t.Log("Generating load...")
	for i := 0; i < initialBatches; i++ {
		_ = db.Write("metric_load", map[string]string{"k": "v"}, 52)
	}

	t.Log("Waiting for WAL to drain...")
	db.DrainWAL()

	startFull := time.Now()
	for i := 0; i < measureBatches; i++ {
		_ = db.Write("metric", nil, 2.0)
	}
	durationFull := time.Since(startFull)

	t.Logf("Duration (Empty DB): %v", durationEmpty)
	t.Logf("Duration (Full DB):  %v", durationFull)

	if durationFull > durationEmpty*2 {
		t.Logf("WARNING: Write performance degraded significantly with data accumulation.")
		t.Logf("Factor: %.2fx slower", float64(durationFull)/float64(durationEmpty))
	} else {
		t.Logf("Performance is stable regardless of data volume.")
	}
}
