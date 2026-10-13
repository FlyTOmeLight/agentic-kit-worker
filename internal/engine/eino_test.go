package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func claimAfterExpiry(t *testing.T, s *durable.Store) *durable.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		r, err := s.Claim(ctx, "replacement", time.Second)
		if err == nil {
			return r
		}
		if !errors.Is(err, durable.ErrNoWork) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("run was not claimable before deadline")
	return nil
}

func TestEinoCheckpointPrecedesToolAndUncertainOutcomeNeedsReview(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "submit", durable.Unsafe)
	interrupted, cancel := context.WithCancel(ctx)
	defer cancel()
	var toolCalls, modelCalls int
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			modelCalls++
			if msgs[len(msgs)-1].Role == schema.Tool {
				current, err := s.Get(ctx, "owner", r.ID)
				if err != nil || current.WaitingStep != "" {
					t.Errorf("resolved review still marked waiting: %+v, %v", current, err)
				}
				return schema.AssistantMessage("confirmed: "+msgs[len(msgs)-1].Content, nil), nil
			}
			return toolReply("submit"), nil
		}}, map[string]engine.Binding{"submit": {Policy: durable.Unsafe, Tool: &fakeTool{name: "submit", invoke: func(ctx context.Context, _ string) (string, error) {
			toolCalls++
			saved, err := s.Get(ctx, "owner", r.ID)
			if err != nil {
				return "", err
			}
			var state engine.State
			if err = json.Unmarshal(saved.State, &state); err != nil {
				return "", err
			}
			if state.Version != 2 || len(state.Checkpoint) == 0 {
				t.Error("side effect started without Eino checkpoint")
			}
			messages, err := s.Messages(ctx, "owner", r.SessionID, 0, 200)
			if err != nil {
				return "", err
			}
			if len(messages) != 2 || len(messages[1].ToolCalls) != 1 {
				t.Error("tool decision not stored before invocation")
			}
			cancel() // External side effect may have happened; no result reaches the store.
			return "", ctx.Err()
		}}}}, nil
	}
	if err := (&engine.Engine{Store: s, Build: build}).Execute(
		interrupted,
		getClaim(t, s),
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("cancel: %v", err)
	}
	if err := (&engine.Engine{Store: s, Build: build}).Execute(ctx, claimAfterExpiry(t, s)); err != nil {
		t.Fatal(err)
	}
	paused, err := s.Get(ctx, "owner", r.ID)
	if err != nil || paused.Status != durable.NeedsReview || toolCalls != 1 || modelCalls != 1 {
		t.Fatalf("paused=%+v err=%v tools=%d models=%d", paused, err, toolCalls, modelCalls)
	}
	if err = s.SupplyResult(ctx, "owner", r.ID, paused.WaitingStep, "receipt", durable.NeedsReview); err != nil {
		t.Fatal(err)
	}
	if err = (&engine.Engine{Store: s, Build: build}).Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "confirmed: receipt" || toolCalls != 1 ||
		modelCalls != 2 {
		t.Fatalf("done=%+v err=%v tools=%d models=%d", done, err, toolCalls, modelCalls)
	}
}

func TestEinoSerialToolsStopAtReviewAndResumeRemainingCalls(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "submit", durable.Unsafe)
	var inputs []string
	var modelCalls int
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			modelCalls++
			if msgs[len(msgs)-1].Role == schema.Tool {
				if msgs[len(msgs)-2].Role != schema.Tool {
					t.Error("Eino lost a tool result")
				}
				return schema.AssistantMessage("done", nil), nil
			}
			// Duplicate provider IDs must still produce separate, stable operations.
			return &schema.Message{Role: schema.Assistant, ToolCalls: []schema.ToolCall{
				{
					ID:       "duplicate",
					Type:     "function",
					Function: schema.FunctionCall{Name: "submit", Arguments: `{"n":1}`},
				},
				{
					ID:       "duplicate",
					Type:     "function",
					Function: schema.FunctionCall{Name: "submit", Arguments: `{"n":2}`},
				},
			}}, nil
		}}, map[string]engine.Binding{"submit": {Policy: durable.Unsafe, Tool: &fakeTool{name: "submit", invoke: func(_ context.Context, in string) (string, error) {
			inputs = append(inputs, in)
			if len(inputs) == 1 {
				return "", errors.New("unknown receipt")
			}
			return "second receipt", nil
		}}}}, nil
	}
	if err := (&engine.Engine{Store: s, Build: build}).Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	paused, err := s.Get(ctx, "owner", r.ID)
	if err != nil || paused.Status != durable.NeedsReview || len(inputs) != 1 {
		t.Fatalf("paused=%+v inputs=%v err=%v", paused, inputs, err)
	}
	if err = s.SupplyResult(
		ctx,
		"owner",
		r.ID,
		paused.WaitingStep,
		"first receipt",
		durable.NeedsReview,
	); err != nil {
		t.Fatal(err)
	}
	if err = (&engine.Engine{Store: s, Build: build}).Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || modelCalls != 2 || len(inputs) != 2 ||
		inputs[1] != `{"n":2}` {
		t.Fatalf("done=%+v inputs=%v modelCalls=%d err=%v", done, inputs, modelCalls, err)
	}
	messages, err := s.Messages(ctx, "owner", r.SessionID, 0, 200)
	if err != nil || len(messages) != 5 || messages[2].ToolCallID == messages[3].ToolCallID {
		t.Fatalf("messages=%+v err=%v", messages, err)
	}
}

func TestEinoIterationLimitSurvivesAutomaticCheckpoints(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)
	var calls int
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				calls++
				return toolReply("read"), nil
			}}, map[string]engine.Binding{"read": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "read", invoke: func(context.Context, string) (string, error) { return "result", nil }}}}, nil
		},
	}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Failed || calls != r.Spec.MaxTurns {
		t.Fatalf("done=%+v calls=%d err=%v", done, calls, err)
	}
}

func TestEinoPreservesLiteralPromptAndHistory(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)
	claimed := getClaim(t, s)
	claimed.Spec.SystemPrompt = `Return JSON like {"answer":true}; keep {braces}.`
	claimed.Spec.History = []durable.Message{
		{Role: "user", Content: "old question"},
		{Role: "assistant", Content: "old answer"},
	}
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
					if len(msgs) != 4 || msgs[0].Content != claimed.Spec.SystemPrompt ||
						msgs[1].Content != "old question" ||
						msgs[2].Content != "old answer" ||
						msgs[3].Content != "do work" {
						t.Errorf("input changed: %+v", msgs)
					}
					return schema.AssistantMessage("done", nil), nil
				},
			}, map[string]engine.Binding{
				"read": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "read"}},
			}, nil
		},
	}
	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed {
		t.Fatalf("done=%+v err=%v", done, err)
	}
}

func TestEinoDoesNotRestartLegacyExecution(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)
	claimed := getClaim(t, s)
	claimed.State = json.RawMessage(`{"version":1,"turn":1,"waitingStep":"old"}`)
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			t.Fatal("legacy state must not start a new agent")
			return nil, nil, nil
		},
	}
	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Failed || !strings.Contains(done.Error, "legacy") {
		t.Fatalf("done=%+v err=%v", done, err)
	}
}
