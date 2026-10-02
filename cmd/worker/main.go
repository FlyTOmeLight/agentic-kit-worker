package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/config"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	path := flag.String("config", os.Getenv("CONFIG_FILE"), "TOML configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	s, err := durable.Open(ctx, cfg.Database.URL)
	if err != nil {
		return err
	}
	defer s.Close()
	w := engine.Worker{Engine: &engine.Engine{Store: s, Build: adapters.Build}, ID: durable.NewID(), Concurrency: cfg.Worker.Concurrency, Lease: cfg.Worker.Lease, Poll: cfg.Worker.Poll}
	return w.Run(ctx)
}
