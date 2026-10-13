package service

import (
	"context"
	"errors"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/server-go/logger"
	"github.com/gin-gonic/gin"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/config"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// Worker owns the execution goroutine and its database connection.
// Bootstrap and HTTPHost invoke its lifecycle methods in sequence.
type Worker struct {
	cfg    config.Config
	log    *logger.Logger
	store  *durable.Store
	worker *engine.Worker
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func New(cfg config.Config, log *logger.Logger) *Worker { return &Worker{cfg: cfg, log: log} }

func (s *Worker) Initialize(ctx context.Context) error {
	store, err := durable.Open(ctx, s.cfg.Database.URL)
	if err != nil {
		return err
	}
	w := &engine.Worker{
		Engine:      &engine.Engine{Store: store, Build: adapters.Build},
		Log:         s.log,
		ID:          durable.NewID(),
		Concurrency: s.cfg.Worker.Concurrency,
		Lease:       s.cfg.Worker.Lease,
		Poll:        s.cfg.Worker.Poll,
	}
	if err := w.Validate(); err != nil {
		store.Close()
		return err
	}
	s.store, s.worker = store, w
	return nil
}

func (s *Worker) RegisterRoutes(*gin.RouterGroup) error { return nil }

func (s *Worker) Start(ctx context.Context) error {
	if s.worker == nil {
		return errors.New("worker is not initialized")
	}
	if s.done != nil {
		return errors.New("worker already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		s.err = s.worker.Run(runCtx)
	}()
	return nil
}

func (s *Worker) Stop(ctx context.Context) error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	select {
	case <-s.done:
		if errors.Is(s.err, context.Canceled) {
			return nil
		}
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Worker) Close(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	if s.store != nil {
		s.store.Close()
		s.store = nil
		s.worker = nil
	}
	return nil
}
