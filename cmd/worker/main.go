package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	server "github.com/FlyTOmeLight/server-go"
	"github.com/FlyTOmeLight/server-go/host"
	"github.com/FlyTOmeLight/server-go/logger"
	"github.com/gin-gonic/gin"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/config"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/service"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (result error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	path := flag.String("config", os.Getenv("CONFIG_FILE"), "TOML configuration file")
	flag.Parse()
	cfg, manager, err := config.LoadManager(*path)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, manager.Close(context.Background())) }()
	log, err := logger.NewLogger(&logger.Config{Outputs: []logger.Output{{Type: "Console", Level: "Info"}}})
	if err != nil {
		return err
	}
	defer log.Close()
	gin.SetMode(cfg.RunMode)
	worker := service.New(cfg, log)
	bootstrap := server.NewBootstrap(worker)
	if err := bootstrap.Initialize(ctx); err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, bootstrap.Close(closeCtx))
	}()
	h, err := host.NewHTTPHost(manager, log, nil, worker)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, h.Close()) }()
	return h.Run(ctx)
}
