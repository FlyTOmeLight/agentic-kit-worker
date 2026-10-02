package main

import (
	"context"
	"errors"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
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
	s, err := durable.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer s.Close()
	n := 4
	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		n, err = strconv.Atoi(raw)
		if err != nil {
			return err
		}
	}
	lease := 30 * time.Second
	if raw := os.Getenv("WORKER_LEASE"); raw != "" {
		lease, err = time.ParseDuration(raw)
		if err != nil {
			return err
		}
	}
	w := engine.Worker{Engine: &engine.Engine{Store: s, Build: adapters.Build}, ID: durable.NewID(), Concurrency: n, Lease: lease, Poll: time.Second}
	return w.Run(ctx)
}
