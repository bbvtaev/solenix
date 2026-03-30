package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bbvtaev/pulse-core/internal/model"
)

// Manager управляет нумерованными WAL сегментами.
// Файлы: data/wal/000001.wal, 000002.wal, ...
type Manager struct {
	mu      sync.Mutex
	dir     string
	current *wal
	seq     uint64
	maxSize int64 // ротация по размеру; 0 — только по таймеру
}

// Open открывает (или создаёт) WAL manager в указанной директории.
func Open(dir string, maxSize int64) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	seq, err := latestWALSeq(dir)
	if err != nil {
		return nil, err
	}
	if seq == 0 {
		seq = 1
	}

	path := segmentPath(dir, seq)
	w, err := openWAL(path)
	if err != nil {
		return nil, fmt.Errorf("open wal segment %s: %w", path, err)
	}

	return &Manager{
		dir:     dir,
		current: w,
		seq:     seq,
		maxSize: maxSize,
	}, nil
}

// Write записывает запись в текущий WAL сегмент.
func (m *Manager) Write(rec model.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.write(rec)
}

// Flush сбрасывает буфер текущего сегмента на диск (fsync).
func (m *Manager) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current.flush()
}

// Rotate запечатывает текущий сегмент и открывает новый.
// Возвращает путь к запечатанному сегменту.
func (m *Manager) Rotate() (sealedPath string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sealedPath = segmentPath(m.dir, m.seq)

	if err := m.current.close(); err != nil {
		return "", fmt.Errorf("close wal segment: %w", err)
	}

	m.seq++
	newPath := segmentPath(m.dir, m.seq)
	w, err := openWAL(newPath)
	if err != nil {
		return "", fmt.Errorf("open new wal segment %s: %w", newPath, err)
	}
	m.current = w

	return sealedPath, nil
}

// ShouldRotate возвращает true если текущий сегмент превысил maxSize.
func (m *Manager) ShouldRotate() bool {
	if m.maxSize <= 0 {
		return false
	}
	m.mu.Lock()
	seq := m.seq
	m.mu.Unlock()

	info, err := os.Stat(segmentPath(m.dir, seq))
	if err != nil {
		return false
	}
	return info.Size() >= m.maxSize
}

// Close закрывает текущий WAL сегмент.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current.close()
}

// ListSegmentPaths возвращает пути всех *.wal файлов, отсортированных по имени.
func ListSegmentPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", seq))
}

func latestWALSeq(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var max uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "%06d.wal", &n); err == nil && n > max {
			max = n
		}
	}
	return max, nil
}
