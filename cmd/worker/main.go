package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/FlyTOmeLight/server-go/app"
	"github.com/FlyTOmeLight/server-go/logger"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/service"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	path := flag.String("config", os.Getenv("CONFIG_FILE"), "TOML configuration file")
	logPath := flag.String("log-config", os.Getenv("LOG_CONFIG_FILE"), "logger TOML configuration file")
	flag.Parse()
	options := app.Options{ConfigPath: *path}
	if *logPath != "" {
		cfg, err := logger.ParseConfigFile(*logPath)
		if err != nil {
			return fmt.Errorf("read logger config: %w", err)
		}
		options.LogConfig = cfg
	}
	return app.Run(ctx, options, service.Build)
}
