package pulse

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// rawConfig — промежуточная структура для парсинга YAML.
// Длительности хранятся как строки ("24h", "100ms") и конвертируются в time.Duration.
type rawConfig struct {
	WALPath       string     `yaml:"wal_path"`
	Retention     string     `yaml:"retention"`
	FlushInterval string     `yaml:"flush_interval"`
	Mode          ServerMode `yaml:"mode"`
	GRPCAddr      string     `yaml:"grpc_addr"`
	Auth          struct {
		Enabled bool     `yaml:"enabled"`
		APIKeys []string `yaml:"api_keys"`
	} `yaml:"auth"`
}

// LoadConfig читает YAML файл и возвращает Config.
// Незаполненные поля получают значения из DefaultConfig().
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

	if raw.WALPath != "" {
		cfg.WALPath = raw.WALPath
	}
	if raw.Mode != "" {
		cfg.Mode = raw.Mode
	}
	if raw.GRPCAddr != "" {
		cfg.GRPCAddr = raw.GRPCAddr
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
	cfg.Auth = AuthConfig{
		Enabled: raw.Auth.Enabled,
		APIKeys: raw.Auth.APIKeys,
	}

	return cfg, nil
}

// DefaultConfig возвращает конфиг со значениями по умолчанию.
func DefaultConfig() Config {
	return Config{
		WALPath:       defaultWALPath(),
		Mode:          ModeSelfHosted,
		GRPCAddr:      ":50051",
		FlushInterval: 100 * time.Millisecond,
	}
}

func defaultWALPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "./data/metrics.wal"
	}
	return filepath.Join(home, "pulse", "metrics.wal")
}
