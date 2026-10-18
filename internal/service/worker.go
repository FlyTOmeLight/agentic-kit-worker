package service

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// Worker owns the execution goroutine and borrows its initialized dependencies.
type Worker struct {
	worker *engine.Worker
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func New(worker *engine.Worker) *Worker { return &Worker{worker: worker} }

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
