package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/viper"
)

type Config struct {
	Database struct {
		URL string `mapstructure:"url"`
	} `mapstructure:"database"`
	Worker struct {
		Concurrency int           `mapstructure:"concurrency"`
		Lease       time.Duration `mapstructure:"lease"`
		Poll        time.Duration `mapstructure:"poll"`
	} `mapstructure:"worker"`
}

func Load(path string) (Config, error) {
	var cfg Config
	v := viper.New()
	v.SetConfigType("toml")
	v.AllowEmptyEnv(true)
	v.SetDefault("worker.concurrency", 4)
	v.SetDefault("worker.lease", "30s")
	v.SetDefault("worker.poll", "1s")
	for key, env := range map[string]string{
		"database.url": "DATABASE_URL", "worker.concurrency": "WORKER_CONCURRENCY",
		"worker.lease": "WORKER_LEASE", "worker.poll": "WORKER_POLL",
	} {
		if err := v.BindEnv(key, env); err != nil {
			return cfg, fmt.Errorf("bind %s: %w", key, err)
		}
	}
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return cfg, fmt.Errorf("read worker config: %w", err)
		}
	}
	if err := v.UnmarshalExact(&cfg); err != nil {
		return cfg, fmt.Errorf("decode worker config: %w", err)
	}
	if strings.TrimSpace(cfg.Database.URL) == "" {
		return cfg, fmt.Errorf("database.url is required")
	}
	if _, err := pgx.ParseConfig(cfg.Database.URL); err != nil {
		return cfg, fmt.Errorf("invalid database.url")
	}
	if cfg.Worker.Concurrency < 1 {
		return cfg, fmt.Errorf("worker.concurrency must be positive")
	}
	if cfg.Worker.Lease < 300*time.Millisecond {
		return cfg, fmt.Errorf("worker.lease must be at least 300ms")
	}
	if cfg.Worker.Poll <= 0 {
		return cfg, fmt.Errorf("worker.poll must be positive")
	}
	return cfg, nil
}
