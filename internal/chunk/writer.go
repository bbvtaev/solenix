// Package chunk реализует бинарный формат chunk-файлов для хранения исторических данных.
package chunk

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bbvtaev/pulse-core/internal/model"
)

const versionGorilla byte = 0x02

const (
	Magic      uint32 = 0x50554C53 // "PULS"
	version    byte   = versionGorilla
	headerSize        = 24 // magic(4) + version(1) + reserved(3) + min_ts(8) + max_ts(8)
	footerSize        = 8  // series_count(4) + checksum(4)
)

// Writer записывает серии одной метрики в нумерованные chunk-файлы.
type Writer struct {
	dir string // data/chunks/
}

// NewWriter создаёт Writer с корневой директорией chunks.
func NewWriter(dir string) *Writer {
	return &Writer{dir: dir}
}

// Write записывает все серии одной метрики в новый numbered chunk файл.
func (cw *Writer) Write(metric string, series []*model.SeriesResult) error {
	var nonEmpty []*model.SeriesResult
	for _, ser := range series {
		if len(ser.Points) > 0 {
			nonEmpty = append(nonEmpty, ser)
		}
	}
	if len(nonEmpty) == 0 {
		return nil
	}

	metricDir := filepath.Join(cw.dir, sanitizeMetric(metric))
	if err := os.MkdirAll(metricDir, 0o755); err != nil {
		return fmt.Errorf("mkdir chunk dir %s: %w", metricDir, err)
	}

	n, err := nextChunkNum(metricDir)
	if err != nil {
		return fmt.Errorf("next chunk num: %w", err)
	}

	path := filepath.Join(metricDir, fmt.Sprintf("%06d.chunk", n))

	var minTS int64 = math.MaxInt64
	var maxTS int64 = math.MinInt64

	var body []byte
	for _, ser := range nonEmpty {
		id := model.HashSeries(ser.Metric, ser.Labels)
		labelsBytes := encodeLabels(ser.Labels)

		body = binary.LittleEndian.AppendUint64(body, id)
		body = binary.LittleEndian.AppendUint16(body, uint16(len(labelsBytes)))
		body = append(body, labelsBytes...)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(ser.Points)))

		for _, p := range ser.Points {
			if p.Timestamp < minTS {
				minTS = p.Timestamp
			}
			if p.Timestamp > maxTS {
				maxTS = p.Timestamp
			}
		}

		compressed := EncodePoints(ser.Points)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(compressed)))
		body = append(body, compressed...)
	}

	// Заголовок: magic(4)+version(1)+reserved(3)+min_ts(8)+max_ts(8) = 24 bytes
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], Magic)
	hdr[4] = version
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(minTS))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(maxTS))

	// CRC покрывает header + body (всё кроме footer)
	h := crc32.NewIEEE()
	_, _ = h.Write(hdr)
	_, _ = h.Write(body)
	checksum := h.Sum32()

	// Footer: series_count(4) + checksum(4)
	ftr := make([]byte, footerSize)
	binary.LittleEndian.PutUint32(ftr[0:4], uint32(len(nonEmpty)))
	binary.LittleEndian.PutUint32(ftr[4:8], checksum)

	// Атомарная запись: tmp → rename
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create chunk tmp: %w", err)
	}

	var werr error
	if _, werr = f.Write(hdr); werr == nil {
		if _, werr = f.Write(body); werr == nil {
			if _, werr = f.Write(ftr); werr == nil {
				werr = f.Sync()
			}
		}
	}
	f.Close()

	if werr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write chunk: %w", werr)
	}

	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename chunk: %w", err)
	}

	return nil
}

// encodeLabels кодирует labels: [count:uint16] ([key_len:uint16][key][val_len:uint16][val])*N
func encodeLabels(labels map[string]string) []byte {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf []byte
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(labels)))
	for _, k := range keys {
		v := labels[k]
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func nextChunkNum(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1, nil
	}
	var max uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".chunk") {
			continue
		}
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "%06d.chunk", &n); err == nil && n > max {
			max = n
		}
	}
	return max + 1, nil
}

func sanitizeMetric(name string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		}
		return r
	}, name)
}
