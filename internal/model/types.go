package model

import (
	"fmt"
	"time"
)

const Version = "6.2.0-alpha"

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

func ParseAggType(s string) (AggType, error) {
	switch AggType(s) {
	case AggAvg, AggMin, AggMax, AggSum, AggCount:
		return AggType(s), nil
	default:
		return "", fmt.Errorf("unknown agg type %q, expected avg/min/max/sum/count", s)
	}
}

type AggPoint struct {
	Timestamp int64   `json:"timestamp"`
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

type AuthConfig struct {
	Enabled bool     `yaml:"enabled"`
	APIKeys []string `yaml:"api_keys"`
}

type CollectorConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
}
