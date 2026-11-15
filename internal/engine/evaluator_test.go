package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestUnavailableEvaluatorFailsClosed(t *testing.T) {
	status, output, err := engine.UnavailableEvaluator{}.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "rm -rf /"},
		nil,
	)
	if !errors.Is(err, engine.ErrEvaluatorUnavailable) {
		t.Fatalf("expected ErrEvaluatorUnavailable, got %v", err)
	}
	if status != durable.EvaluationError || output != "" {
		t.Fatalf("expected Error with no output, got %s %q", status, output)
	}
}

func TestUnavailableEvaluatorPassesWithoutAcceptance(t *testing.T) {
	status, _, err := engine.UnavailableEvaluator{}.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
		nil,
	)
	if err != nil || status != durable.EvaluationPassed {
		t.Fatalf("expected no-op pass, got %s %v", status, err)
	}
}
