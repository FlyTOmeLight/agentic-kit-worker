package engine_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type iterationEvaluator func(context.Context, durable.EvaluatorSpec, *durable.Run) (durable.EvaluationStatus, string, error)

func (evaluate iterationEvaluator) Evaluate(
	ctx context.Context,
	spec durable.EvaluatorSpec,
	run *durable.Run,
) (durable.EvaluationStatus, string, error) {
	return evaluate(ctx, spec, run)
}

func TestWorkerGoalIteration(t *testing.T) {
	cases := []struct {
		name       string
		verdicts   []durable.EvaluationStatus
		goalStatus durable.GoalStatus
	}{
		{
			name:       "passes second run",
			verdicts:   []durable.EvaluationStatus{durable.EvaluationFailed, durable.EvaluationPassed},
			goalStatus: durable.GoalComplete,
		},
		{
			name: "stops after three stalled runs",
			verdicts: []durable.EvaluationStatus{
				durable.EvaluationFailed,
				durable.EvaluationFailed,
				durable.EvaluationFailed,
			},
			goalStatus: durable.GoalBlocked,
		},
		{
			name:       "does not retry infrastructure error",
			verdicts:   []durable.EvaluationStatus{durable.EvaluationError},
			goalStatus: durable.GoalActive,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			first := submit(t, store, "submit", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: first.SessionID, RequestKey: "iteration",
				RequestHash: "iteration", Objective: "include the required field",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "check"},
				Budget:    durable.BudgetSpec{MaxWallClockSeconds: 60},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, first.ID); err != nil {
				t.Fatal(err)
			}
			var evaluations atomic.Int32
			inputs := make(chan []*schema.Message, 10)
			builds := make(chan durable.Spec, 10)
			worker := engine.Worker{
				ID:          "iteration-worker",
				Concurrency: 2,
				Lease:       time.Second,
				Poll:        10 * time.Millisecond,
				Evaluator: iterationEvaluator(
					func(context.Context, durable.EvaluatorSpec, *durable.Run) (durable.EvaluationStatus, string, error) {
						index := int(evaluations.Add(1)) - 1
						if index >= len(test.verdicts) {
							return durable.EvaluationError, "", errors.New(
								"unexpected extra evaluation",
							)
						}
						return test.verdicts[index], "missing required field", nil
					},
				),
				Engine: &engine.Engine{
					Store: store,
					Build: func(_ context.Context, _ *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
						builds <- spec
						return &fakeModel{
							generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
								inputs <- messages
								return schema.AssistantMessage("same answer", nil), nil
							},
						}, map[string]engine.Binding{
							"submit": {
								Policy: durable.ReadOnly,
								Tool:   &fakeTool{name: "submit"},
							},
						}, nil
					},
				},
			}
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("worker did not stop")
				}
			}()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				current, readErr := store.GetGoal(ctx, "owner", goal.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if current.Status == test.goalStatus &&
					evaluations.Load() >= int32(len(test.verdicts)) {
					_, receiptErr := store.GetEvaluation(ctx, "owner", goal.ID, current.ActiveRunID)
					if receiptErr == nil {
						break
					}
					if !errors.Is(receiptErr, durable.ErrNotFound) {
						t.Fatal(receiptErr)
					}
				}
				select {
				case <-ctx.Done():
					t.Fatalf("goal did not reach %s: %+v", test.goalStatus, current)
				case <-ticker.C:
				}
			}
			runs, err := store.SessionRuns(ctx, "owner", first.SessionID, "", 100)
			if err != nil || len(runs) != len(test.verdicts) ||
				int(evaluations.Load()) != len(test.verdicts) {
				t.Fatalf("runs=%+v evaluations=%d err=%v", runs, evaluations.Load(), err)
			}
			for index := range test.verdicts {
				spec := <-builds
				messages := <-inputs
				if spec.ConfigVersion != first.Spec.ConfigVersion || spec.Model != first.Spec.Model {
					t.Fatalf("iteration lost the frozen configuration: %+v", spec)
				}
				if index == 0 {
					continue
				}
				if len(spec.History) != 2*index || spec.History[0].Content != "do work" ||
					!strings.Contains(
						spec.Query,
						goal.Objective,
					) || !strings.Contains(spec.Query, "missing required field") {
					t.Fatalf("iteration lost conversation or acceptance feedback: %+v", spec)
				}
				var found bool
				for _, message := range messages {
					if message.Role == schema.User && message.Content == spec.Query {
						found = true
					}
				}
				if !found {
					t.Fatalf("model did not receive acceptance feedback: %+v", messages)
				}
			}
			if _, err = store.QueueGoalIteration(ctx); !errors.Is(err, durable.ErrNoWork) {
				t.Fatalf("stopped goal queued another iteration: %v", err)
			}
		})
	}
}
