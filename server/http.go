package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"sort"
	"strconv"

	pulse "github.com/bbvtaev/pulse-core"
)

//go:embed static
var staticFiles embed.FS

// HTTPServer отдаёт embedded UI и REST API для self-hosted режима.
type HTTPServer struct {
	db      *pulse.DB
	version string
}

func NewHTTP(db *pulse.DB) *HTTPServer {
	return &HTTPServer{db: db, version: pulse.Version}
}

// ListenHTTP запускает HTTP сервер на заданном адресе (например, ":8080").
func (h *HTTPServer) ListenHTTP(addr string) error {
	mux := http.NewServeMux()

	// Static UI
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return fmt.Errorf("static fs: %w", err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// API
	mux.HandleFunc("/api/metrics", h.handleMetrics)
	mux.HandleFunc("/api/query", h.handleQuery)
	mux.HandleFunc("/api/snapshot", h.handleSnapshot)
	mux.HandleFunc("/api/stream", h.handleStream)
	mux.HandleFunc("/api/health", h.handleHealth)

	return http.ListenAndServe(addr, mux)
}

func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := h.db.Metrics()
	sort.Strings(metrics)
	writeJSON(w, map[string]any{"metrics": metrics})
}

func (h *HTTPServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, `{"error":"metric is required"}`, http.StatusBadRequest)
		return
	}

	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)

	results, err := h.db.Query(metric, nil, from, to)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}

	writeJSON(w, map[string]any{"series": results})
}

// handleSnapshot возвращает все метрики со всеми сериями (последние 200 точек на серию).
func (h *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	type snapshotSeries struct {
		Labels      map[string]string `json:"labels"`
		Points      []pulse.Point     `json:"points"`
		TotalPoints int               `json:"total_points"`
	}
	type snapshotMetric struct {
		Metric string           `json:"metric"`
		Series []snapshotSeries `json:"series"`
	}

	names := h.db.Metrics()
	sort.Strings(names)

	result := make([]snapshotMetric, 0, len(names))
	for _, name := range names {
		series, err := h.db.Query(name, nil, 0, 0)
		if err != nil {
			continue
		}
		sm := snapshotMetric{Metric: name}
		for _, s := range series {
			pts := s.Points
			total := len(pts)
			if len(pts) > 200 {
				pts = pts[len(pts)-200:]
			}
			sm.Series = append(sm.Series, snapshotSeries{
				Labels:      s.Labels,
				Points:      pts,
				TotalPoints: total,
			})
		}
		result = append(result, sm)
	}

	writeJSON(w, map[string]any{"metrics": result})
}

// handleStream — SSE эндпоинт. Отправляет "tick" при каждом новом Write в БД.
func (h *HTTPServer) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	id, ch := h.db.Watch()
	defer h.db.Unwatch(id)

	// Начальный ping — клиент сразу делает первый fetch
	fmt.Fprintf(w, "data: tick\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: tick\n\n")
			flusher.Flush()
		}
	}
}

func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"status":  "ok",
		"version": h.version,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
