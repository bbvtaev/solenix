package pulse

import "time"

const Version = "2.0.0-alpha"

type Point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type WriteSeries struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type SeriesResult struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type AggType string

const (
	AggAvg   AggType = "avg"
	AggMin   AggType = "min"
	AggMax   AggType = "max"
	AggSum   AggType = "sum"
	AggCount AggType = "count"
)

type AggPoint struct {
	Timestamp int64   `json:"timestamp"` // начало окна
	Value     float64 `json:"value"`
}

type AggResult struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Window time.Duration     `json:"window"`
	Points []AggPoint        `json:"points"`
}

type ServerMode string

const (
	ModeSelfHosted ServerMode = "self-hosted"
	ModeCloud      ServerMode = "cloud"
)

// AuthConfig — настройки авторизации (используются в cloud режиме).
type AuthConfig struct {
	Enabled bool     `yaml:"enabled"`
	APIKeys []string `yaml:"api_keys"`
}

// Config задаёт параметры запуска БД и gRPC сервера.
type Config struct {
	// Storage
	WALPath           string        `yaml:"wal_path"`
	RetentionDuration time.Duration `yaml:"retention"`
	FlushInterval     time.Duration `yaml:"flush_interval"`

	// Server
	Mode     ServerMode `yaml:"mode"`
	GRPCAddr string     `yaml:"grpc_addr"`
	HTTPAddr string     `yaml:"http_addr"` // UI + REST API, только в self-hosted

	// Auth
	Auth AuthConfig `yaml:"auth"`

	// Collector — встроенный сбор системных метрик хоста.
	Collector CollectorConfig `yaml:"collector"`
}

type CollectorConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"` // как часто собирать, default 15s
}
