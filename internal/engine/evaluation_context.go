package engine

import (
	"context"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

type evaluationClaimKey struct{}

func withEvaluationClaim(ctx context.Context, run *durable.Run) context.Context {
	return context.WithValue(ctx, evaluationClaimKey{}, run)
}

// WithEvaluationClaim binds a claimed evaluation to the context so dispatch
// fencing headers reach the sandbox center. Exported for the cross-process
// takeover e2e; production flows stay inside the worker.
func WithEvaluationClaim(ctx context.Context, run *durable.Run) context.Context {
	return withEvaluationClaim(ctx, run)
}

func evaluationClaim(ctx context.Context) (*durable.Run, bool) {
	run, ok := ctx.Value(evaluationClaimKey{}).(*durable.Run)
	return run, ok
}
