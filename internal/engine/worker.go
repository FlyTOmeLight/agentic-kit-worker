package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

type Worker struct {
	Engine      *Engine
	ID          string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Engine == nil || w.Engine.Store == nil || w.ID == "" || w.Concurrency < 1 || w.Lease < 300*time.Millisecond || w.Poll <= 0 {
		return fmt.Errorf("invalid worker configuration")
	}
	var wg sync.WaitGroup
	for i := 0; i < w.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.loop(ctx) }()
	}
	wg.Wait()
	return ctx.Err()
}
func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		run, err := w.Engine.Store.Claim(ctx, w.ID, w.Lease)
		if err != nil {
			if !errors.Is(err, durable.ErrNoWork) && ctx.Err() == nil {
				slog.Error("claim failed", "error", err)
			}
			t := time.NewTimer(w.Poll)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			continue
		}
		w.execute(ctx, run)
	}
}
func (w *Worker) execute(parent context.Context, run *durable.Run) {
	ctx, cancel := context.WithDeadline(parent, run.Deadline)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(ctx, w.Lease/3)
				err := w.Engine.Store.Renew(renewCtx, run.Lease(), w.Lease)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := w.Engine.Execute(ctx, run)
	cancel()
	<-heartbeatDone
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, durable.ErrLeaseLost) {
		slog.Error("run interrupted; lease expiry will trigger recovery", "runID", run.ID, "error", err)
	}
	// On process shutdown or lost lease, never overwrite a newer owner's state.
}
