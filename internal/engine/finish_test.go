package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestFinishBatchRejectsSideEffectsThenExits(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	a, err := s.CreateAgent(
		ctx,
		"owner",
		"finish-test",
		durable.AgentConfig{
			Model:    durable.ModelSpec{Name: "test", BaseURL: "http://localhost/v1"},
			MaxTurns: 5,
			Tools: []durable.ToolSpec{
				{
					Name:       "finish",
					EndRun:     true,
					Policy:     durable.Idempotent,
					URL:        "http://localhost/finish",
					Parameters: json.RawMessage(`{"type":"object"}`),
				},
				{
					Name:       "write",
					Policy:     durable.Unsafe,
					URL:        "http://localhost/write",
					Parameters: json.RawMessage(`{"type":"object"}`),
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PublishAgent(ctx, "owner", a.ID, a.Revision); err != nil {
		t.Fatal(err)
	}
	session, err := s.CreateSession(ctx, "owner", a.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Submit(ctx, "owner", session.ID, durable.NewID(), "finish", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	calls, finishes, writes := 0, 0, 0
	e := engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			llm := &fakeModel{
				generate: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
					calls++
					msg := toolReply("finish")
					if calls == 1 {
						msg.ToolCalls = append(
							msg.ToolCalls,
							schema.ToolCall{
								Type: "function",
								Function: schema.FunctionCall{
									Name:      "write",
									Arguments: `{}`,
								},
							},
						)
					}
					if calls > 2 {
						t.Fatal("model invoked after successful finish")
					}
					return msg, nil
				},
			}
			return llm, map[string]engine.Binding{
				"finish": {
					Policy: durable.Idempotent,
					EndRun: true,
					Tool: &fakeTool{
						name:   "finish",
						invoke: func(context.Context, string) (string, error) { finishes++; return `{"accepted":true}`, nil },
					},
				},
				"write": {
					Policy: durable.Unsafe,
					Tool: &fakeTool{
						name:   "write",
						invoke: func(context.Context, string) (string, error) { writes++; return "written", nil },
					},
				},
			}, nil
		},
	}
	claimed, err := s.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "owner", r.ID)
	if err != nil || got.Status != durable.Completed || calls != 2 || finishes != 1 || writes != 0 {
		t.Fatalf("status=%s calls=%d finishes=%d writes=%d err=%v", got.Status, calls, finishes, writes, err)
	}
}
