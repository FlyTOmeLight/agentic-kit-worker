package service

import (
	"context"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/server-go/app"
	"github.com/FlyTOmeLight/server-go/db"
	"github.com/gin-gonic/gin"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/config"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func Build(ctx context.Context, runtime *app.Runtime, cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	gin.SetMode(cfg.RunMode)
	database := &db.Module{Config: &cfg.DB}
	if err := runtime.Initialize(ctx, database); err != nil {
		return err
	}
	store, err := durable.New(database.DB)
	if err != nil {
		return err
	}
	worker := &engine.Worker{
		Engine:      &engine.Engine{Store: store, Build: adapters.Build},
		Log:         runtime.Log,
		ID:          durable.NewID(),
		Concurrency: cfg.Worker.Concurrency,
		Lease:       cfg.Worker.Lease,
		Poll:        cfg.Worker.Poll,
	}
	if err := worker.Validate(); err != nil {
		return err
	}
	return runtime.AddService(New(worker))
}
