package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func sampleSnapshot(t *testing.T, budget contextbuilder.Budget) durable.ContextSnapshot {
	t.Helper()
	envelope, err := contextbuilder.EncodeSnapshot(contextbuilder.Snapshot{
		MemoryMode: contextbuilder.MemoryRecall,
		Materials: contextbuilder.FrozenMaterials{
			SystemInstruction: "You are a helpful agent.",
			Tools: []contextbuilder.ToolDefinition{{
				Name:        "lookup",
				Description: "query something",
				Parameters:  json.RawMessage(`{"type":"object"}`),
			}},
			Memory: []contextbuilder.ContextBlock{
				{
					Source: contextbuilder.Source{
						Kind: contextbuilder.SourceMemory,
						ID:   "mem-low",
					},
					Content:  "Low priority reference info.",
					Priority: 10,
				},
				{
					Source: contextbuilder.Source{
						Kind: contextbuilder.SourceMemory,
						ID:   "mem-high",
					},
					Content:  "High priority reference info.",
					Priority: 100,
				},
			},
			History: []contextbuilder.MessageGroup{
				{
					Source: contextbuilder.Source{Kind: contextbuilder.SourceHistory, ID: "turn-1"},
					Messages: []contextbuilder.Message{
						{Role: contextbuilder.RoleUser, Content: "turn 1 question"},
						{
							Role:    contextbuilder.RoleAssistant,
							Content: "searching",
							ToolCalls: []contextbuilder.ToolCall{
								{ID: "c1", Name: "lookup", Arguments: "{}"},
							},
						},
						{
							Role:       contextbuilder.RoleTool,
							ToolCallID: "c1",
							Content:    "turn 1 result",
						},
						{Role: contextbuilder.RoleAssistant, Content: "turn 1 answer"},
					},
				},
			},
		},
		Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV1},
		Budget: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	return durable.ContextSnapshot{
		Format:        envelope.Format,
		PolicyVersion: envelope.PolicyVersion,
		Digest:        envelope.Digest,
		Payload:       envelope.Payload,
	}
}

func TestEinoExecutesWithContextSnapshot(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)

	claimed := getClaim(t, s)
	claimed.ContextSnapshot = sampleSnapshot(t, contextbuilder.Budget{})

	var modelCalls int
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
					modelCalls++
					// First message must be the rendered system instruction including memory blocks
					if len(msgs) == 0 || msgs[0].Role != schema.System {
						t.Errorf("missing system message: %+v", msgs)
					}
					if !strings.Contains(msgs[0].Content, "You are a helpful agent.") ||
						!strings.Contains(msgs[0].Content, "High priority reference info.") ||
						!strings.Contains(msgs[0].Content, "Low priority reference info.") {
						t.Errorf("system message missing rendered memory: %s", msgs[0].Content)
					}
					// Must include complete previous turn history
					hasHistory := false
					for _, m := range msgs {
						if m.Content == "turn 1 question" {
							hasHistory = true
							break
						}
					}
					if !hasHistory {
						t.Errorf("history turn was omitted unexpectedly: %+v", msgs)
					}
					// Must end with the current turn query
					if msgs[len(msgs)-1].Role != schema.User ||
						msgs[len(msgs)-1].Content != "do work" {
						t.Errorf("current turn query mismatch: %+v", msgs[len(msgs)-1])
					}
					return schema.AssistantMessage("finished successfully", nil), nil
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
	if err != nil || done.Status != durable.Completed || done.Output != "finished successfully" || modelCalls != 1 {
		t.Fatalf("done=%+v err=%v calls=%d", done, err, modelCalls)
	}
}

func TestEinoTrimsContextUnderBudget(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)

	claimed := getClaim(t, s)
	// Set byte budget small enough that lowest-priority memory must be trimmed
	claimed.ContextSnapshot = sampleSnapshot(t, contextbuilder.Budget{
		MaxInputBytes: 550,
	})

	var modelCalls int
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
					modelCalls++
					// High priority memory must be kept
					if !strings.Contains(msgs[0].Content, "High priority reference info.") {
						t.Errorf("high priority memory missing: %s", msgs[0].Content)
					}
					// Low priority memory should have been trimmed to fit the budget.
					if strings.Contains(msgs[0].Content, "Low priority reference info.") {
						t.Errorf("low priority memory was not trimmed: %s", msgs[0].Content)
					}
					return schema.AssistantMessage("trimmed successfully", nil), nil
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
	if err != nil || done.Status != durable.Completed || done.Output != "trimmed successfully" || modelCalls != 1 {
		t.Fatalf("done=%+v err=%v calls=%d", done, err, modelCalls)
	}
}

func TestEinoFailsImmediatelyWhenContextBudgetExceeded(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)

	claimed := getClaim(t, s)
	// Budget is impossible for required inputs (system prompt + query + tool defs)
	claimed.ContextSnapshot = sampleSnapshot(t, contextbuilder.Budget{
		MaxInputBytes: 20,
	})

	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, _ []*schema.Message) (*schema.Message, error) {
					t.Fatal("model must not be called when budget is exceeded")
					return nil, nil
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
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Failed {
		t.Fatalf("expected run status Failed, got %s", done.Status)
	}
	if !strings.Contains(done.Error, "budget") {
		t.Fatalf("expected error detail to mention budget, got: %s", done.Error)
	}
}

func TestEinoFailsWhenToolOutputExceedsBudgetDuringExecution(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)

	claimed := getClaim(t, s)
	// Budget allows initial prompt (~300 bytes), but will not tolerate an enormous tool response
	claimed.ContextSnapshot = sampleSnapshot(t, contextbuilder.Budget{
		MaxInputBytes: 450,
	})

	var modelCalls int
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
					modelCalls++
					if msgs[len(msgs)-1].Role == schema.Tool {
						t.Fatal(
							"second model call must not happen if tool output exceeded budget",
						)
					}
					// First call: emit tool call
					return &schema.Message{
						Role: schema.Assistant,
						ToolCalls: []schema.ToolCall{{
							ID:       "c1",
							Type:     "function",
							Function: schema.FunctionCall{Name: "read", Arguments: "{}"},
						}},
					}, nil
				},
			}, map[string]engine.Binding{
				"read": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "read",
						invoke: func(_ context.Context, _ string) (string, error) {
							// Return gigantic tool result that blows through the 450-byte budget
							return strings.Repeat("huge tool output block ", 50), nil
						},
					},
				},
			}, nil
		},
	}

	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}

	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Failed {
		t.Fatalf("expected run status Failed, got %s", done.Status)
	}
	if !strings.Contains(done.Error, "budget") {
		t.Fatalf("expected error detail to mention budget, got: %s", done.Error)
	}
	if modelCalls != 1 {
		t.Fatalf("expected exactly 1 model call, got %d", modelCalls)
	}
}

func TestEinoCorruptSnapshotFailsCleanly(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "read", durable.ReadOnly)

	claimed := getClaim(t, s)
	snapshot := sampleSnapshot(t, contextbuilder.Budget{})
	snapshot.Digest = "tampered_digest"
	claimed.ContextSnapshot = snapshot

	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			t.Fatal("must not build model for corrupt snapshot")
			return nil, nil, nil
		},
	}

	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}

	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Failed {
		t.Fatalf("expected run status Failed, got %s", done.Status)
	}
	if !strings.Contains(done.Error, "snapshot") {
		t.Fatalf("expected error to mention snapshot, got: %s", done.Error)
	}
}
