package engine

import (
	"context"
	"errors"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

// Evaluator runs the deterministic acceptance check after a run completes.
// Implementations must execute inside the configured sandbox/tool runtime;
// the control plane and the worker host never run goal-supplied commands.
type Evaluator interface {
	Evaluate(
		ctx context.Context,
		spec durable.EvaluatorSpec,
		run *durable.Run,
	) (durable.EvaluationStatus, string, error)
}

// ErrEvaluatorUnavailable means no sandbox-backed runner is wired. Callers
// must record an Error receipt and leave the goal Active rather than treating
// infrastructure absence as a failed acceptance.
var ErrEvaluatorUnavailable = errors.New("no sandbox evaluator backend configured")

// UnavailableEvaluator is the fail-closed default. It never executes a
// command; it only reports that acceptance could not be performed.
type UnavailableEvaluator struct{}

func (UnavailableEvaluator) Evaluate(
	_ context.Context,
	spec durable.EvaluatorSpec,
	_ *durable.Run,
) (durable.EvaluationStatus, string, error) {
	if spec.Kind == durable.EvaluatorNone {
		return durable.EvaluationPassed, "", nil
	}
	return durable.EvaluationError, "", ErrEvaluatorUnavailable
}
