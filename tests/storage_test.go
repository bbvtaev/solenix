package pulse_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	pulse "github.com/bbvtaev/pulse-core"
)

// TestChunkFlush проверяет полный цикл: Write → flushToChunks → restart → Query.
func TestChunkFlush(t *testing.T) {
	dir := t.TempDir()

	db, err := pulse.Open(pulse.Config{
		DataDir:       dir,
		FlushInterval: 50 * time.Millisecond, // быстрый flush для теста
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.Write("cpu_usage", map[string]string{"host": "srv1"}, 0.5, 0.7); err != nil {
		t.Fatalf("Write: %v", err)
	}
	db.DrainWAL()

	// Даём bgLoop сделать flush
	time.Sleep(200 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Проверяем что chunks появились на диске
	chunksDir := filepath.Join(dir, "chunks", "cpu_usage")
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		t.Fatalf("ReadDir chunks: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one chunk file")
	}

	// Перезапуск: данные должны загрузиться из chunks
	db2, err := pulse.Open(pulse.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer db2.Close()

	res, err := db2.Query("cpu_usage", map[string]string{"host": "srv1"}, 0, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}
	if len(res[0].Points) != 2 {
		t.Fatalf("expected 2 points after reload, got %d", len(res[0].Points))
	}
}

// TestWALRotation проверяет что после ротации WAL новые записи идут в новый сегмент.
func TestWALRotation(t *testing.T) {
	dir := t.TempDir()

	db, err := pulse.Open(pulse.Config{
		DataDir:       dir,
		WALMaxSize:    1, // 1 байт — каждая запись вызывает ротацию
		FlushInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := db.Write("metric", nil, float64(i)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	db.DrainWAL()
	time.Sleep(200 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// После перезапуска все 5 точек должны быть видны
	db2, err := pulse.Open(pulse.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer db2.Close()

	res, err := db2.Query("metric", nil, 0, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}
	if len(res[0].Points) != 5 {
		t.Fatalf("expected 5 points, got %d", len(res[0].Points))
	}
}

// TestMultiMetricChunks проверяет что для каждой метрики создаётся своя директория.
func TestMultiMetricChunks(t *testing.T) {
	dir := t.TempDir()

	db, err := pulse.Open(pulse.Config{
		DataDir:       dir,
		FlushInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_ = db.Write("cpu", nil, 1.0)
	_ = db.Write("mem", nil, 2.0)
	_ = db.Write("disk", nil, 3.0)
	db.DrainWAL()

	time.Sleep(200 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Проверяем директории chunks
	for _, metric := range []string{"cpu", "mem", "disk"} {
		metricDir := filepath.Join(dir, "chunks", metric)
		entries, err := os.ReadDir(metricDir)
		if err != nil {
			t.Fatalf("ReadDir %s: %v", metric, err)
		}
		if len(entries) == 0 {
			t.Fatalf("no chunk files for metric %s", metric)
		}
	}

	// Reload
	db2, err := pulse.Open(pulse.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer db2.Close()

	for _, tc := range []struct {
		metric string
		value  float64
	}{
		{"cpu", 1.0},
		{"mem", 2.0},
		{"disk", 3.0},
	} {
		res, err := db2.Query(tc.metric, nil, 0, 0)
		if err != nil {
			t.Fatalf("Query %s: %v", tc.metric, err)
		}
		if len(res) != 1 || len(res[0].Points) != 1 {
			t.Fatalf("%s: expected 1 series/1 point, got %v", tc.metric, res)
		}
		if res[0].Points[0].Value != tc.value {
			t.Errorf("%s: expected value %.1f, got %.1f", tc.metric, tc.value, res[0].Points[0].Value)
		}
	}
}
