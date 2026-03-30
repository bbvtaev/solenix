package model

import (
	"hash/fnv"
	"sort"
)

// Record — одна WAL-запись: батч точек для конкретной серии metric+labels.
type Record struct {
	Metric string
	Labels map[string]string
	Points []Point
}

// HashSeries возвращает стабильный FNV-64a хеш для пары metric+labels.
// Используется для идентификации серии в памяти и в chunk-файлах.
func HashSeries(metric string, labels map[string]string) uint64 {
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
	return h.Sum64()
}
