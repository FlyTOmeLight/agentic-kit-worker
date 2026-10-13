package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/server-go/logger"
)

type Worker struct {
	Engine      *Engine
	Log         *logger.Logger
	ID          string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
}

func (w *Worker) Validate() error {
	if w.Engine == nil || w.Engine.Store == nil || w.ID == "" || w.Concurrency < 1 ||
		w.Lease < 300*time.Millisecond ||
		w.Poll <= 0 {
		return errors.New("invalid worker configuration")
	}
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.Validate(); err != nil {
		return err
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
			if !errors.Is(err, durable.ErrNoWork) && ctx.Err() == nil && w.Log != nil {
				w.Log.Errorf(ctx, "claim failed: %v", err)
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
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, durable.ErrLeaseLost) && w.Log != nil {
		w.Log.Errorf(parent, "run %s interrupted; lease expiry will trigger recovery: %v", run.ID, err)
	}
	// On process shutdown or lost lease, never overwrite a newer owner's state.
}
