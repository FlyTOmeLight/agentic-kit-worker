package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/FlyTOmeLight/server-go/conf"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/viper"
)

type Config struct {
	conf.Config `mapstructure:",squash"`
	Database    struct {
		URL string `mapstructure:"url"`
	} `mapstructure:"database"`
	Worker struct {
		Concurrency int           `mapstructure:"concurrency"`
		Lease       time.Duration `mapstructure:"lease"`
		Poll        time.Duration `mapstructure:"poll"`
	} `mapstructure:"worker"`
}

func Load(path string) (Config, error) {
	cfg, _, err := load(path)
	return cfg, err
}

func LoadManager(path string) (Config, *conf.Manager, error) {
	cfg, snapshot, err := load(path)
	if err != nil {
		return cfg, nil, err
	}
	manager, err := conf.NewManagerFromConfig(snapshot)
	return cfg, manager, err
}

func load(path string) (Config, *conf.Config, error) {
	var cfg Config
	v := viper.New()
	v.SetConfigType("toml")
	v.AllowEmptyEnv(true)
	v.SetDefault("AppName", "agentic-kit-worker")
	v.SetDefault("AppCode", 101)
	v.SetDefault("RunMode", "release")
	v.SetDefault("HTTPServer.Listen", "127.0.0.1:8081")
	v.SetDefault("HTTPServer.ReadTimeout", 10000)
	v.SetDefault("HTTPServer.WriteTimeout", 10000)
	v.SetDefault("HTTPServer.IdleTimeout", 60000)
	v.SetDefault("worker.concurrency", 4)
	v.SetDefault("worker.lease", "30s")
	v.SetDefault("worker.poll", "1s")
	for key, env := range map[string]string{
		"HTTPServer.Listen": "LISTEN_ADDR", "RunMode": "GIN_MODE",
		"database.url": "DATABASE_URL", "worker.concurrency": "WORKER_CONCURRENCY",
		"worker.lease": "WORKER_LEASE", "worker.poll": "WORKER_POLL",
	} {
		if err := v.BindEnv(key, env); err != nil {
			return cfg, nil, fmt.Errorf("bind %s: %w", key, err)
		}
	}
	if path == "" {
		path = os.Getenv(conf.EnvServerConfigPath)
	}
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return cfg, nil, fmt.Errorf("read worker config: %w", err)
		}
	}
	if err := v.UnmarshalExact(&cfg, conf.DecodeHook()); err != nil {
		return cfg, nil, fmt.Errorf("decode worker config: %w", err)
	}
	if strings.TrimSpace(cfg.Database.URL) == "" {
		return cfg, nil, errors.New("database.url is required")
	}
	if _, err := pgx.ParseConfig(cfg.Database.URL); err != nil {
		return cfg, nil, errors.New("invalid database.url")
	}
	if cfg.Worker.Concurrency < 1 {
		return cfg, nil, errors.New("worker.concurrency must be positive")
	}
	if cfg.Worker.Lease < 300*time.Millisecond {
		return cfg, nil, errors.New("worker.lease must be at least 300ms")
	}
	if cfg.Worker.Poll <= 0 {
		return cfg, nil, errors.New("worker.poll must be positive")
	}
	_, port, err := net.SplitHostPort(cfg.HTTPServer.Listen)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 0 || number > 65535 {
		return cfg, nil, errors.New("invalid HTTPServer.Listen")
	}
	snapshot, err := conf.FromSettings(v.AllSettings())
	if err != nil {
		return cfg, nil, err
	}
	cfg.Config = *snapshot
	return cfg, snapshot, nil
}
