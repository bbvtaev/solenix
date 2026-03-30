package pulse_test

import (
	"math/rand"
	"testing"
	"time"

	pulse "github.com/bbvtaev/pulse-core"
)

func newTestDB(t *testing.T) *pulse.DB {
	t.Helper()
	db, err := pulse.Open(pulse.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestWriteAndQuerySingleSeries(t *testing.T) {
	db := newTestDB(t)

	if err := db.Write("cpu_usage", map[string]string{"host": "a", "dc": "eu"}, 0.5, 0.7, 0.9); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	res, err := db.Query("cpu_usage", map[string]string{"host": "a"}, 0, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}

	s := res[0]
	if s.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", s.Metric)
	}
	if s.Labels["host"] != "a" || s.Labels["dc"] != "eu" {
		t.Errorf("unexpected labels: %v", s.Labels)
	}
	if len(s.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(s.Points))
	}
}

func TestLabelFiltering(t *testing.T) {
	db := newTestDB(t)

	_ = db.Write("cpu_usage", map[string]string{"host": "ab"}, 0.1)
	_ = db.Write("cpu_usage", map[string]string{"host": "bs"}, 0.2)

	resA, err := db.Query("cpu_usage", map[string]string{"host": "ab"}, 0, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resA) != 1 || resA[0].Labels["host"] != "ab" {
		t.Errorf("expected 1 series host=ab, got %v", resA)
	}

	resB, err := db.Query("cpu_usage", map[string]string{"host": "bs"}, 0, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resB) != 1 || resB[0].Labels["host"] != "bs" {
		t.Errorf("expected 1 series host=bs, got %v", resB)
	}

	resAll, err := db.Query("cpu_usage", nil, 0, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resAll) != 2 {
		t.Fatalf("expected 2 series without label filter, got %d", len(resAll))
	}
}

func TestTimeRangeFiltering(t *testing.T) {
	db := newTestDB(t)

	before := time.Now().UnixNano()
	_ = db.Write("temp", map[string]string{"sensor": "s1"}, 1.0)
	after := time.Now().UnixNano()

	res, err := db.Query("temp", nil, before, after)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(res) != 1 || len(res[0].Points) != 1 {
		t.Fatalf("expected 1 series/1 point in range, got %v", res)
	}

	res2, err := db.Query("temp", nil, after+1, after+1000)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(res2) != 0 {
		t.Fatalf("expected 0 series outside range, got %d", len(res2))
	}
}

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()

	db, err := pulse.Open(pulse.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	_ = db.Write("disk_usage", map[string]string{"host": "a"}, 42.0)
	db.DrainWAL()

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db2, err := pulse.Open(pulse.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db2.Close()

	res, err := db2.Query("disk_usage", map[string]string{"host": "a"}, 0, 0)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(res) != 1 || len(res[0].Points) != 1 {
		t.Fatalf("expected 1 series/1 point after replay, got %v", res)
	}
}

func TestQueryValidationErrors(t *testing.T) {
	db := newTestDB(t)
	_, err := db.Query("", nil, 0, 0)
	if err == nil {
		t.Fatal("expected error for empty metric")
	}
}

func TestDelete(t *testing.T) {
	db := newTestDB(t)

	_ = db.Write("cpu", map[string]string{"host": "a"}, 1.0, 2.0, 3.0)

	res, _ := db.Query("cpu", nil, 0, 0)
	if len(res) != 1 || len(res[0].Points) != 3 {
		t.Fatalf("expected 3 points before delete, got %v", res)
	}

	if err := db.Delete("cpu", map[string]string{"host": "a"}, 0, 0); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	res2, _ := db.Query("cpu", nil, 0, 0)
	if len(res2) != 0 {
		t.Fatalf("expected 0 series after delete, got %d", len(res2))
	}
}

func TestQueryAgg(t *testing.T) {
	db := newTestDB(t)

	base := time.Now().Truncate(time.Minute).UnixNano()
	points := []pulse.Point{
		{Timestamp: base, Value: 10},
		{Timestamp: base + int64(10*time.Second), Value: 20},
		{Timestamp: base + int64(20*time.Second), Value: 30},
		{Timestamp: base + int64(70*time.Second), Value: 40},
	}
	if err := db.WriteBatch("metric", nil, points); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	res, err := db.QueryAgg("metric", nil, 0, 0, time.Minute, pulse.AggAvg)
	if err != nil {
		t.Fatalf("QueryAgg() error = %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 agg result, got %d", len(res))
	}
	if len(res[0].Points) < 2 {
		t.Fatalf("expected at least 2 agg buckets, got %d", len(res[0].Points))
	}
	// Первое окно: 10+20+30 / 3 = 20
	if res[0].Points[0].Value != 20.0 {
		t.Errorf("expected avg=20 in first bucket, got %f", res[0].Points[0].Value)
	}
}

func TestSubscribe(t *testing.T) {
	db := newTestDB(t)

	id, ch := db.Subscribe("events", map[string]string{"env": "test"})
	defer db.Unsubscribe(id)

	_ = db.Write("events", map[string]string{"env": "test"}, 42.0)

	select {
	case p := <-ch:
		if p.Value != 42.0 {
			t.Errorf("expected value 42, got %f", p.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribed point")
	}
}

// --- Benchmarks ---

func newBenchDB(b *testing.B) *pulse.DB {
	b.Helper()
	db, err := pulse.Open(pulse.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkWriteThroughput(b *testing.B) {
	db := newBenchDB(b)
	rnd := rand.New(rand.NewSource(0))
	_ = db.Write("warmup", map[string]string{"sensor": "init"}, 0.0)

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		if err := db.Write("load", map[string]string{"sensor": "A1"}, rnd.Float64()*100); err != nil {
			b.Fatalf("Write error: %v", err)
		}
	}

	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
}

func BenchmarkWrite_BottleneckAnalysis(b *testing.B) {
	db, err := pulse.Open(pulse.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open error: %v", err)
	}
	defer db.Close()

	b.Run("Sustained_Write_KeepOpen", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := db.Write("bench_metric", map[string]string{"h": "1"}, 52); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Overhead_OpenClose_PerWrite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db2, err := pulse.Open(pulse.Config{DataDir: b.TempDir()})
			if err != nil {
				b.Fatalf("Open error: %v", err)
			}
			if err := db2.Write("bench_metric", map[string]string{"h": "1"}, 52); err != nil {
				b.Fatal(err)
			}
			if err := db2.Close(); err != nil {
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

	const measureBatches = 100
	const initialBatches = 1_000_000

	startEmpty := time.Now()
	for i := 0; i < measureBatches; i++ {
		_ = db.Write("metric", nil, 1.0)
	}
	durationEmpty := time.Since(startEmpty)

	t.Log("Generating load...")
	for i := 0; i < initialBatches; i++ {
		_ = db.Write("metric_load", map[string]string{"k": "v"}, 52)
	}
	db.DrainWAL()

	startFull := time.Now()
	for i := 0; i < measureBatches; i++ {
		_ = db.Write("metric", nil, 2.0)
	}
	durationFull := time.Since(startFull)

	t.Logf("Duration (Empty DB): %v", durationEmpty)
	t.Logf("Duration (Full DB):  %v", durationFull)

	if durationFull > durationEmpty*2 {
		t.Logf("WARNING: Write performance degraded. Factor: %.2fx slower", float64(durationFull)/float64(durationEmpty))
	} else {
		t.Log("Performance is stable.")
	}
}
