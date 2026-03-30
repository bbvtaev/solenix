// Package pulse — public API for pulse-core TSDB.
// Implementation is split across internal sub-packages;
// this file re-exports everything so external consumers need no import changes.
package pulse

import (
	"github.com/bbvtaev/pulse-core/internal/config"
	"github.com/bbvtaev/pulse-core/internal/model"
	"github.com/bbvtaev/pulse-core/internal/storage"
)

// ── Types ─────────────────────────────────────────────────────────────────────

type (
	DB              = storage.DB
	Point           = model.Point
	WriteSeries     = model.WriteSeries
	SeriesResult    = model.SeriesResult
	AggType         = model.AggType
	AggPoint        = model.AggPoint
	AggResult       = model.AggResult
	ServerMode      = model.ServerMode
	AuthConfig      = model.AuthConfig
	Config          = config.Config
	CollectorConfig = model.CollectorConfig
)

// ── Constants ─────────────────────────────────────────────────────────────────

const (
	Version = model.Version

	AggAvg   AggType = model.AggAvg
	AggMin   AggType = model.AggMin
	AggMax   AggType = model.AggMax
	AggSum   AggType = model.AggSum
	AggCount AggType = model.AggCount

	ModeSelfHosted ServerMode = model.ModeSelfHosted
	ModeCloud      ServerMode = model.ModeCloud
)

// ── Functions ─────────────────────────────────────────────────────────────────

// Open открывает (или создаёт) БД согласно конфигу.
func Open(cfg Config) (*DB, error) { return storage.Open(cfg) }

// LoadConfig читает YAML файл и возвращает Config.
func LoadConfig(path string) (Config, error) { return config.LoadConfig(path) }

// DefaultConfig возвращает конфиг со значениями по умолчанию.
func DefaultConfig() Config { return config.DefaultConfig() }
