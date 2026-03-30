package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bbvtaev/pulse-core/internal/model"
	"gopkg.in/yaml.v3"
)

// Config задаёт параметры запуска БД и серверов.
type Config struct {
	// Storage
	DataDir           string        `yaml:"data_dir"`
	WALMaxSize        int64         `yaml:"wal_max_size"`  // байт; default 32 MiB
	RetentionDuration time.Duration `yaml:"retention"`
	FlushInterval     time.Duration `yaml:"flush_interval"` // интервал flush в chunks; default 2m

	// Server
	Mode     model.ServerMode `yaml:"mode"`
	GRPCAddr string           `yaml:"grpc_addr"`
	HTTPAddr string           `yaml:"http_addr"`

	// Auth
	Auth model.AuthConfig `yaml:"auth"`

	// Collector
	Collector model.CollectorConfig `yaml:"collector"`
}

// rawConfig — промежуточная структура для парсинга YAML.
// Длительности хранятся как строки и конвертируются в time.Duration.
type rawConfig struct {
	DataDir       string           `yaml:"data_dir"`
	WALMaxSize    int64            `yaml:"wal_max_size"`
	Retention     string           `yaml:"retention"`
	FlushInterval string           `yaml:"flush_interval"`
	Mode          model.ServerMode `yaml:"mode"`
	GRPCAddr      string           `yaml:"grpc_addr"`
	HTTPAddr      string           `yaml:"http_addr"`
	Auth          struct {
		Enabled bool     `yaml:"enabled"`
		APIKeys []string `yaml:"api_keys"`
	} `yaml:"auth"`
	Collector struct {
		Enabled  bool   `yaml:"enabled"`
		Interval string `yaml:"interval"`
	} `yaml:"collector"`
}

// LoadConfig читает YAML файл и возвращает Config.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg := DefaultConfig()

	if raw.DataDir != "" {
		cfg.DataDir = raw.DataDir
	}
	if raw.WALMaxSize > 0 {
		cfg.WALMaxSize = raw.WALMaxSize
	}
	if raw.Mode != "" {
		cfg.Mode = raw.Mode
	}
	if raw.GRPCAddr != "" {
		cfg.GRPCAddr = raw.GRPCAddr
	}
	if raw.HTTPAddr != "" {
		cfg.HTTPAddr = raw.HTTPAddr
	}
	if raw.Retention != "" {
		d, err := time.ParseDuration(raw.Retention)
		if err != nil {
			return Config{}, fmt.Errorf("retention %q: %w", raw.Retention, err)
		}
		cfg.RetentionDuration = d
	}
	if raw.FlushInterval != "" {
		d, err := time.ParseDuration(raw.FlushInterval)
		if err != nil {
			return Config{}, fmt.Errorf("flush_interval %q: %w", raw.FlushInterval, err)
		}
		cfg.FlushInterval = d
	}
	cfg.Auth = model.AuthConfig{
		Enabled: raw.Auth.Enabled,
		APIKeys: raw.Auth.APIKeys,
	}
	cfg.Collector.Enabled = raw.Collector.Enabled
	if raw.Collector.Interval != "" {
		d, err := time.ParseDuration(raw.Collector.Interval)
		if err != nil {
			return Config{}, fmt.Errorf("collector.interval %q: %w", raw.Collector.Interval, err)
		}
		cfg.Collector.Interval = d
	}

	return cfg, nil
}

// DefaultConfig возвращает конфиг со значениями по умолчанию.
func DefaultConfig() Config {
	return Config{
		DataDir:       defaultDataDir(),
		WALMaxSize:    32 << 20, // 32 MiB
		Mode:          model.ModeSelfHosted,
		GRPCAddr:      ":50051",
		HTTPAddr:      ":8080",
		FlushInterval: 2 * time.Minute,
		Collector: model.CollectorConfig{
			Enabled:  true,
			Interval: 15 * time.Second,
		},
	}
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "./data"
	}
	return filepath.Join(home, "pulse", "data")
}
