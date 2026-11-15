package engine

import (
	"context"
	"errors"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

const (
	evaluationTimeout = 10 * time.Minute
	goalCommitTimeout = 30 * time.Second
)

// advanceGoal evaluates a terminal run and lets the control plane apply the
// verdict. The run passed to execute is the claimed snapshot; its status is
// stale after CommitBoundary, so the durable row is re-read first.
func (w *Worker) advanceGoal(ctx context.Context, run *durable.Run) bool {
	current, err := w.Engine.Store.Get(ctx, run.OwnerID, run.ID)
	if err != nil {
		w.logf("run %s reload for goal progress failed: %v", run.ID, err)
		return false
	}
	if current.GoalID == "" || !current.Status.Terminal() {
		return true
	}
	if current.EvaluationDeadline != nil {
		claimed, claimErr := w.Engine.Store.ClaimEvaluation(ctx, w.ID, w.Lease, current.ID)
		if claimErr != nil {
			w.logf("run %s claim evaluation failed: %v", current.ID, claimErr)
			return false
		}
		return w.withEvaluationLease(ctx, claimed, func(evalCtx context.Context) bool {
			return w.advanceGoalState(evalCtx, claimed)
		})
	}
	return w.advanceGoalState(ctx, current)
}

func (w *Worker) advanceGoalState(ctx context.Context, current *durable.Run) bool {
	goal, err := w.Engine.Store.GetGoal(ctx, current.OwnerID, current.GoalID)
	if err != nil {
		w.logf("run %s load goal %s failed: %v", current.ID, current.GoalID, err)
		return false
	}
	if goal.Status != durable.GoalActive || goal.ActiveRunID != current.ID {
		return true
	}
	// Acceptance only applies to work that completed. Failed or canceled runs
	// carry no artifact worth checking and count as no progress.
	if current.Status == durable.Completed && goal.Evaluator.Kind != durable.EvaluatorNone {
		receipt, receiptErr := w.Engine.Store.GetEvaluation(ctx, current.OwnerID, goal.ID, current.ID)
		if receiptErr == nil {
			progressed := w.checkGoalProgress(ctx, current, goal)
			return progressed && receipt.Status != durable.EvaluationError
		}
		if !errors.Is(receiptErr, durable.ErrNotFound) {
			w.logf("run %s load evaluation failed: %v", current.ID, receiptErr)
			return false
		}
		if goal.Budget.MaxWallClockSeconds > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(
				ctx,
				goal.CreatedAt.Add(time.Duration(goal.Budget.MaxWallClockSeconds)*time.Second),
			)
			defer cancel()
		}
		status, output, evalErr := w.evaluate(ctx, goal.Evaluator, current)
		if current.EvaluationEpoch > 0 && ctx.Err() != nil {
			return false
		}
		if evalErr != nil {
			status = durable.EvaluationError
			if output == "" {
				output = evalErr.Error()
			} else {
				output += "\n" + evalErr.Error()
			}
		}
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), goalCommitTimeout)
		if current.EvaluationEpoch > 0 {
			_, err = w.Engine.Store.RecordClaimedEvaluation(commitCtx, current, status, output)
		} else {
			_, err = w.Engine.Store.RecordEvaluation(
				commitCtx,
				current.OwnerID,
				goal.ID,
				current.ID,
				status,
				output,
			)
		}
		cancel()
		if err != nil {
			w.logf("run %s record evaluation failed: %v", current.ID, err)
			return false
		}
		progressed := w.checkGoalProgress(ctx, current, goal)
		return progressed && status != durable.EvaluationError
	}
	return w.checkGoalProgress(ctx, current, goal)
}

func (w *Worker) checkGoalProgress(ctx context.Context, run *durable.Run, goal *durable.Goal) bool {
	progressCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), goalCommitTimeout)
	defer cancel()
	if _, err := w.Engine.Store.CheckGoalProgress(progressCtx, run.OwnerID, goal.ID); err != nil {
		w.logf("run %s goal %s progress check failed: %v", run.ID, goal.ID, err)
		return false
	}
	return true
}

func (w *Worker) evaluate(
	ctx context.Context,
	spec durable.EvaluatorSpec,
	run *durable.Run,
) (durable.EvaluationStatus, string, error) {
	evalCtx, cancel := context.WithTimeout(ctx, evaluationTimeout)
	defer cancel()
	evalCtx, cancel = context.WithDeadline(evalCtx, run.Deadline)
	defer cancel()
	if run.EvaluationDeadline != nil {
		evalCtx, cancel = context.WithDeadline(evalCtx, *run.EvaluationDeadline)
		defer cancel()
	}
	if run.EvaluationEpoch > 0 {
		evalCtx = withEvaluationClaim(evalCtx, run)
	}
	if w.Evaluator == nil {
		return UnavailableEvaluator{}.Evaluate(evalCtx, spec, run)
	}
	return w.Evaluator.Evaluate(evalCtx, spec, run)
}

func (w *Worker) withEvaluationLease(ctx context.Context, run *durable.Run, work func(context.Context) bool) bool {
	ctx, cancel := context.WithDeadline(ctx, *run.EvaluationDeadline)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(ctx, w.Lease/3)
				err := w.Engine.Store.RenewEvaluation(renewCtx, run, w.Lease)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	release := work(ctx)
	cancel()
	<-done
	return release
}

func (w *Worker) recoverEvaluation(ctx context.Context, run *durable.Run) {
	w.withEvaluationLease(ctx, run, func(evalCtx context.Context) bool {
		receipt, receiptErr := w.Engine.Store.GetEvaluation(evalCtx, run.OwnerID, run.GoalID, run.ID)
		switch {
		case receiptErr == nil:
			if receipt.Status == durable.EvaluationError || !w.advanceGoalState(evalCtx, run) {
				return false
			}
		case !errors.Is(receiptErr, durable.ErrNotFound):
			w.logf("run %s load evaluation for recovery failed: %v", run.ID, receiptErr)
			return false
		}
		if w.ResumeEvaluationEnvironment == nil {
			w.logf("run %s cannot resume evaluation environment", run.ID)
			return false
		}
		env, err := w.ResumeEvaluationEnvironment(evalCtx, run)
		if err != nil {
			w.logf("run %s resume evaluation environment failed: %v", run.ID, err)
			return false
		}
		if receiptErr != nil && !w.advanceGoalState(evalCtx, run) {
			env.Detach()
			return false
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(evalCtx), goalCommitTimeout)
		defer cancel()
		if err = env.Close(releaseCtx); err != nil {
			w.logf("run %s release evaluation environment failed: %v", run.ID, err)
			return false
		}
		return true
	})
}

func (w *Worker) logf(format string, args ...any) {
	if w.Log != nil {
		w.Log.Errorf(context.Background(), format, args...)
	}
}
