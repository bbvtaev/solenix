// Package wal реализует Write-Ahead Log: запись, flush и replay.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/bbvtaev/pulse-core/internal/model"
)

// Формат WAL-записи на диске:
// [payload_len: uint32] [crc32: uint32] [payload: payload_len bytes]
//
// Формат payload:
// [metric_len: uint16] [metric bytes]
// [labels_count: uint16] → ([key_len: uint16] [key] [val_len: uint16] [val]) * N
// [points_count: uint16] → ([timestamp: int64] [value: float64]) * N

type wal struct {
	mu  sync.Mutex
	f   *os.File
	buf *bufio.Writer
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{
		f:   f,
		buf: bufio.NewWriterSize(f, 1<<20), // 1 MiB буфер
	}, nil
}

func (w *wal) write(rec model.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.f == nil {
		return fmt.Errorf("WAL is closed")
	}

	payload := encodeRecord(rec)
	checksum := crc32.ChecksumIEEE(payload)

	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], checksum)

	if _, err := w.buf.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	return nil
}

func (w *wal) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf != nil {
		_ = w.buf.Flush()
	}
	if w.f != nil {
		_ = w.f.Sync()
	}
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf != nil {
		_ = w.buf.Flush()
	}
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	w.buf = nil
	return err
}

// Replay читает все записи из WAL-файла и возвращает их.
// Каждая запись верифицируется по CRC-32.
func Replay(path string) ([]model.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	header := make([]byte, 8)
	var records []model.Record

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		payloadLen := binary.LittleEndian.Uint32(header[0:4])
		expectedCRC := binary.LittleEndian.Uint32(header[4:8])

		payload := make([]byte, payloadLen)
		if _, err = io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("unexpected EOF reading WAL payload")
		}

		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("WAL record CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
		}

		rec, err := decodeRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("corrupted WAL record: %w", err)
		}

		records = append(records, rec)
	}

	return records, nil
}

func encodeRecord(rec model.Record) []byte {
	estimatedSize := 2 + len(rec.Metric) + 2 + len(rec.Labels)*20 + 2 + len(rec.Points)*16
	buf := make([]byte, 0, estimatedSize)

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Metric)))
	buf = append(buf, rec.Metric...)

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Labels)))
	for k, v := range rec.Labels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rec.Points)))
	for _, p := range rec.Points {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(p.Timestamp))
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(p.Value))
	}

	return buf
}

func decodeRecord(data []byte) (model.Record, error) {
	var rec model.Record
	offset := 0
	maxLen := len(data)

	check := func(n int) bool { return offset+n <= maxLen }

	if !check(2) {
		return rec, io.ErrUnexpectedEOF
	}
	metricLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	if !check(metricLen) {
		return rec, io.ErrUnexpectedEOF
	}
	rec.Metric = string(data[offset : offset+metricLen])
	offset += metricLen

	if !check(2) {
		return rec, io.ErrUnexpectedEOF
	}
	labelsCount := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	rec.Labels = make(map[string]string, labelsCount)
	for i := 0; i < labelsCount; i++ {
		if !check(2) {
			return rec, io.ErrUnexpectedEOF
		}
		kLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if !check(kLen) {
			return rec, io.ErrUnexpectedEOF
		}
		key := string(data[offset : offset+kLen])
		offset += kLen

		if !check(2) {
			return rec, io.ErrUnexpectedEOF
		}
		vLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if !check(vLen) {
			return rec, io.ErrUnexpectedEOF
		}
		rec.Labels[key] = string(data[offset : offset+vLen])
		offset += vLen
	}

	if !check(2) {
		return rec, io.ErrUnexpectedEOF
	}
	pointsCount := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2

	rec.Points = make([]model.Point, pointsCount)
	if !check(pointsCount * 16) {
		return rec, io.ErrUnexpectedEOF
	}
	for i := 0; i < pointsCount; i++ {
		ts := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		val := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		rec.Points[i] = model.Point{Timestamp: ts, Value: val}
	}

	return rec, nil
}
