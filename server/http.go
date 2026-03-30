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
	mux.HandleFunc("/api/latest", h.handleLatest)
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

// handleLatest возвращает последнюю точку каждой серии по всем метрикам.
func (h *HTTPServer) handleLatest(w http.ResponseWriter, r *http.Request) {
	type row struct {
		Metric    string            `json:"metric"`
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Timestamp int64             `json:"timestamp"`
	}

	names := h.db.Metrics()
	sort.Strings(names)

	rows := make([]row, 0)
	for _, name := range names {
		series, err := h.db.Query(name, nil, 0, 0)
		if err != nil {
			continue
		}
		for _, s := range series {
			if len(s.Points) == 0 {
				continue
			}
			last := s.Points[len(s.Points)-1]
			rows = append(rows, row{
				Metric:    name,
				Labels:    s.Labels,
				Value:     last.Value,
				Timestamp: last.Timestamp,
			})
		}
	}

	writeJSON(w, map[string]any{"rows": rows})
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
