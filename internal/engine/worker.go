package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/FlyTOmeLight/server-go/logger"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

type Worker struct {
	Engine                      *Engine
	Evaluator                   Evaluator
	Log                         *logger.Logger
	ID                          string
	Concurrency                 int
	Lease                       time.Duration
	Poll                        time.Duration
	ResumeEvaluationEnvironment func(context.Context, *durable.Run) (EvaluationEnvironment, error)
}

type EvaluationEnvironment interface {
	Close(context.Context) error
	Detach()
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
	if w.Evaluator == nil {
		w.Evaluator = UnavailableEvaluator{}
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
		evaluation, evaluationErr := w.Engine.Store.ClaimEvaluation(ctx, w.ID, w.Lease, "")
		if evaluationErr == nil {
			w.recoverEvaluation(ctx, evaluation)
			continue
		}
		if !errors.Is(evaluationErr, durable.ErrNoWork) && ctx.Err() == nil {
			w.logf("claim evaluation failed: %v", evaluationErr)
		}
		if _, iterationErr := w.Engine.Store.QueueGoalIteration(ctx); iterationErr != nil &&
			!errors.Is(iterationErr, durable.ErrNoWork) && ctx.Err() == nil {
			w.logf("queue goal iteration failed: %v", iterationErr)
		}
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
	defer cancel()
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
	err := w.Engine.execute(ctx, run, func() bool {
		cancel()
		<-heartbeatDone
		return w.advanceGoal(parent, run)
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, durable.ErrLeaseLost) && w.Log != nil {
		w.Log.Errorf(parent, "run %s interrupted; lease expiry will trigger recovery: %v", run.ID, err)
	}
	// On process shutdown or lost lease, never overwrite a newer owner's state.
}
