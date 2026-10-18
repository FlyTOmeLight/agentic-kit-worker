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
	HTTPServer conf.HTTPServerConfig
}

type WorkerConfig struct {
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
}

func (cfg Config) Validate() error {
	if cfg.DB.Driver != db.DriverPostgreSQL {
		return errors.New("DB.Driver must be PostgreSQL")
	}
	if cfg.DB.AutoMigrate {
		return errors.New("DB.AutoMigrate must be false; schema initialization is managed outside the worker lifecycle")
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
	_, port, err := net.SplitHostPort(cfg.HTTPServer.Listen)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 0 || number > 65535 {
		return errors.New("invalid HTTPServer.Listen")
	}
	return nil
}
