package service

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/FlyTOmeLight/server-go/app"
	"github.com/FlyTOmeLight/server-go/db"
	"github.com/gin-gonic/gin"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
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
		Engine: &engine.Engine{
			Store: store, Build: adapters.Build,
			Summarizer:         adapters.Summarizer{},
			CompactionMaxBytes: cfg.Compaction.MaxInputBytes,
		},
		Log:         runtime.Log,
		ID:          durable.NewID(),
		Concurrency: cfg.Worker.Concurrency,
		Lease:       cfg.Worker.Lease,
		Poll:        cfg.Worker.Poll,
	}
	if cfg.Evaluator.BaseURL != "" {
		token := os.Getenv(cfg.Evaluator.TokenEnv)
		if token == "" {
			return errors.New("sandbox-center credential is not configured")
		}
		client, clientErr := sdk.NewClient(
			cfg.Evaluator.BaseURL,
			token,
			&http.Client{Timeout: 65 * time.Second},
		)
		if clientErr != nil {
			return clientErr
		}
		worker.Evaluator = engine.SandboxEvaluator{
			Runner: engine.HTTPSandboxRunner{Client: client.Sandboxes},
			Store:  store,
		}
		worker.ResumeEvaluationEnvironment = func(ctx context.Context, run *durable.Run) (engine.EvaluationEnvironment, error) {
			return sandboxAdapter.ResumeEvaluation(ctx, store, client.Sandboxes, run)
		}
	}
	if err := worker.Validate(); err != nil {
		return err
	}
	return runtime.AddService(New(worker))
}
