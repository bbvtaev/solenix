package storage

import (
	"errors"
	"math"
	"sort"
	"time"

	"github.com/bbvtaev/pulse-core/internal/model"
)

// Query возвращает все серии по метрике и лейблам в диапазоне [from, to].
// from и to — Unix nanoseconds. 0 означает отсутствие ограничения.
func (db *DB) Query(metric string, labels map[string]string, from, to int64) ([]model.SeriesResult, error) {
	if metric == "" {
		return nil, errors.New("metric is required")
	}

	ids := db.metricIdx.lookup(metric)
	if len(ids) == 0 {
		return nil, nil
	}

	var res []model.SeriesResult
	for _, id := range ids {
		sh := db.shardFor(id)
		sh.mu.RLock()
		ser, ok := sh.series[id]
		if !ok {
			sh.mu.RUnlock()
			continue
		}
		if !labelsMatch(labels, ser.labels) {
			sh.mu.RUnlock()
			continue
		}
		points := filterPoints(ser.points, from, to)
		lbls := cloneLabels(ser.labels)
		met := ser.metric
		sh.mu.RUnlock()

		if len(points) == 0 {
			continue
		}
		res = append(res, model.SeriesResult{Metric: met, Labels: lbls, Points: points})
	}

	return res, nil
}

// Delete удаляет точки в диапазоне [from, to] для всех серий, совпадающих с metric+labels.
func (db *DB) Delete(metric string, labels map[string]string, from, to int64) error {
	if metric == "" {
		return errors.New("metric is required")
	}

	ids := db.metricIdx.lookup(metric)
	for _, id := range ids {
		sh := db.shardFor(id)
		sh.mu.Lock()
		ser, ok := sh.series[id]
		if !ok {
			sh.mu.Unlock()
			continue
		}
		if !labelsMatch(labels, ser.labels) {
			sh.mu.Unlock()
			continue
		}
		ser.points = deletePoints(ser.points, from, to)
		if len(ser.points) == 0 {
			delete(sh.series, id)
			sh.mu.Unlock()
			db.metricIdx.remove(metric, id)
			continue
		}
		sh.mu.Unlock()
	}

	return nil
}

// QueryAgg возвращает агрегированные данные по временным окнам.
func (db *DB) QueryAgg(metric string, labels map[string]string, from, to int64, window time.Duration, agg model.AggType) ([]model.AggResult, error) {
	raw, err := db.Query(metric, labels, from, to)
	if err != nil {
		return nil, err
	}

	results := make([]model.AggResult, 0, len(raw))
	for _, s := range raw {
		pts := aggregatePoints(s.Points, from, window, agg)
		if len(pts) == 0 {
			continue
		}
		results = append(results, model.AggResult{
			Metric: s.Metric,
			Labels: s.Labels,
			Window: window,
			Points: pts,
		})
	}

	return results, nil
}

func filterPoints(points []model.Point, from, to int64) []model.Point {
	if len(points) == 0 {
		return nil
	}

	start := 0
	if from > 0 {
		start = sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp >= from
		})
	}

	end := len(points)
	if to > 0 {
		end = sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp > to
		})
	}

	if start >= end {
		return nil
	}

	out := make([]model.Point, end-start)
	copy(out, points[start:end])
	return out
}

func deletePoints(points []model.Point, from, to int64) []model.Point {
	if len(points) == 0 {
		return nil
	}
	if from == 0 && to == 0 {
		return nil
	}

	out := points[:0]
	for _, p := range points {
		inRange := (from == 0 || p.Timestamp >= from) && (to == 0 || p.Timestamp <= to)
		if !inRange {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	result := make([]model.Point, len(out))
	copy(result, out)
	return result
}

func aggregatePoints(points []model.Point, from int64, window time.Duration, agg model.AggType) []model.AggPoint {
	if len(points) == 0 || window <= 0 {
		return nil
	}

	winNs := window.Nanoseconds()
	base := from
	if base == 0 {
		base = points[0].Timestamp
	}
	bucketStart := (base / winNs) * winNs

	last := points[len(points)-1].Timestamp
	var result []model.AggPoint

	for bucketStart <= last {
		bucketEnd := bucketStart + winNs

		lo := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= bucketStart })
		hi := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= bucketEnd })

		if lo < hi {
			vals := make([]float64, hi-lo)
			for i, p := range points[lo:hi] {
				vals[i] = p.Value
			}
			result = append(result, model.AggPoint{
				Timestamp: bucketStart,
				Value:     applyAgg(vals, agg),
			})
		}
		bucketStart = bucketEnd
	}

	return result
}

func applyAgg(vals []float64, agg model.AggType) float64 {
	switch agg {
	case model.AggAvg:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case model.AggMin:
		m := math.MaxFloat64
		for _, v := range vals {
			if v < m {
				m = v
			}
		}
		return m
	case model.AggMax:
		m := -math.MaxFloat64
		for _, v := range vals {
			if v > m {
				m = v
			}
		}
		return m
	case model.AggSum:
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		return sum
	case model.AggCount:
		return float64(len(vals))
	default:
		return 0
	}
}
