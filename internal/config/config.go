package config

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/FlyTOmeLight/server-go/conf"
	"github.com/FlyTOmeLight/server-go/db"
	"github.com/jackc/pgx/v5"
)

type Config struct {
	RunMode    string
	DB         db.Config
	Worker     WorkerConfig
	Evaluator  EvaluatorConfig
	Pricing    PricingConfig
	Compaction CompactionConfig
	HTTPServer conf.HTTPServerConfig
}

type PricingConfig map[string]any

func (c PricingConfig) Validate() error {
	if c != nil {
		return errors.New(
			"worker Pricing configuration is no longer accepted; configure pricing on the API and use frozen Run snapshots",
		)
	}
	return nil
}

type CompactionConfig struct {
	MaxInputBytes int
}

type WorkerConfig struct {
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
}

// EvaluatorConfig configures the optional sandbox-backed goal evaluator.
// Leave BaseURL empty to keep the fail-closed UnavailableEvaluator. TokenEnv
// names the environment variable holding the sandbox-center credential; the
// secret value never enters configuration.
type EvaluatorConfig struct {
	BaseURL  string
	TokenEnv string
}

func (cfg Config) Validate() error {
	if cfg.DB.Driver != db.DriverPostgreSQL {
		return errors.New("DB.Driver must be PostgreSQL")
	}
	if cfg.DB.AutoMigrate {
		return errors.New(
			"DB.AutoMigrate must be false; schema initialization is managed outside the worker lifecycle",
		)
	}
	if strings.TrimSpace(cfg.DB.DSN) == "" {
		return errors.New("DB.DSN is required")
	}
	if _, err := pgx.ParseConfig(cfg.DB.DSN); err != nil {
		return errors.New("invalid DB.DSN")
	}
	if cfg.Worker.Concurrency < 1 {
		return errors.New("Worker.Concurrency must be positive")
	}
	if cfg.Worker.Lease < 300*time.Millisecond {
		return errors.New("Worker.Lease must be at least 300ms")
	}
	if cfg.Worker.Poll <= 0 {
		return errors.New("Worker.Poll must be positive")
	}
	if cfg.Evaluator.BaseURL != "" && cfg.Evaluator.TokenEnv == "" {
		return errors.New("Evaluator.TokenEnv is required when BaseURL is set")
	}
	if cfg.Compaction.MaxInputBytes < 0 {
		return errors.New("Compaction.MaxInputBytes must not be negative")
	}
	if err := cfg.Pricing.Validate(); err != nil {
		return err
	}
	_, port, err := net.SplitHostPort(cfg.HTTPServer.Listen)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 0 || number > 65535 {
		return errors.New("invalid HTTPServer.Listen")
	}
	return nil
}
