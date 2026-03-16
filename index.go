package pulse

import "sync"

// metricIndex хранит соответствие metric → множество seriesID.
// Позволяет Query() работать без full scan по всем шардам.
type metricIndex struct {
	mu  sync.RWMutex
	idx map[string]map[seriesID]struct{}
}

func newMetricIndex() *metricIndex {
	return &metricIndex{
		idx: make(map[string]map[seriesID]struct{}),
	}
}

func (mi *metricIndex) add(metric string, id seriesID) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if _, ok := mi.idx[metric]; !ok {
		mi.idx[metric] = make(map[seriesID]struct{})
	}
	mi.idx[metric][id] = struct{}{}
}

func (mi *metricIndex) remove(metric string, id seriesID) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if m, ok := mi.idx[metric]; ok {
		delete(m, id)
		if len(m) == 0 {
			delete(mi.idx, metric)
		}
	}
}

func (mi *metricIndex) lookup(metric string) []seriesID {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	m, ok := mi.idx[metric]
	if !ok {
		return nil
	}
	ids := make([]seriesID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}
