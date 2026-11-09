package engine_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestEinoPreviewsToolOutputAndStoresOriginal(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "read", durable.ReadOnly)
	claimed := getClaim(t, store)
	claimed.ContextSnapshot = sampleSnapshot(t, contextbuilder.Budget{
		ContextWindow: 2000, OutputReserve: 200, ProtocolOverheadTokens: 256,
		CounterVersion: contextbuilder.ByteEstimateVersion, MaxToolResultBytes: 256,
	})
	original := strings.Repeat("完整工具结果αβγ ", 500)
	modelCalls := 0
	worker := &engine.Engine{
		Store: store,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					modelCalls++
					if modelCalls == 1 {
						return toolReply("read"), nil
					}
					last := messages[len(messages)-1]
					if last.Role != schema.Tool || len(last.Content) > 256 ||
						!utf8.ValidString(last.Content) ||
						!strings.Contains(
							last.Content,
							"Tool result preview: middle omitted;",
						) {
						t.Errorf("unexpected model tool projection: %+v", last)
					}
					if modelCalls == 2 {
						return toolReply("read"), nil
					}
					return schema.AssistantMessage("finished", nil), nil
				},
			}, map[string]engine.Binding{
				"read": {Policy: durable.ReadOnly, Tool: &fakeTool{
					name:   "read",
					invoke: func(context.Context, string) (string, error) { return original, nil },
				}},
			}, nil
		},
	}
	if err := worker.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := store.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed || modelCalls != 3 {
		t.Fatalf("run=%+v calls=%d error=%v", done, modelCalls, err)
	}
	var state engine.State
	if err := json.Unmarshal(done.State, &state); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state.Checkpoint), original) {
		t.Fatal("checkpoint lost the original tool result before the next tool boundary")
	}
	messages, err := store.Messages(ctx, "owner", run.SessionID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, message := range messages {
		if message.Role == "tool" {
			found = true
			if message.Content != original {
				t.Fatal("persisted tool result was truncated")
			}
		}
	}
	if !found {
		t.Fatal("missing persisted tool result")
	}
}

func TestEinoReducesHistoryInTwoLevels(t *testing.T) {
	for _, test := range []struct {
		name       string
		limit      int64
		keepAnswer bool
	}{
		{name: "retain question and final answer", limit: 220, keepAnswer: true},
		{name: "remove whole oldest turn", limit: 120},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "read", durable.ReadOnly)
			claimed := getClaim(t, store)
			snapshot := sampleSnapshot(t, contextbuilder.Budget{MaxInputBytes: test.limit})
			decoded, err := contextbuilder.DecodeSnapshot(contextbuilder.Envelope{
				Format: snapshot.Format, PolicyVersion: snapshot.PolicyVersion,
				Digest: snapshot.Digest, Payload: snapshot.Payload,
			})
			if err != nil {
				t.Fatal(err)
			}
			decoded.Policy.Version = contextbuilder.PolicyVersionV2
			envelope, err := contextbuilder.EncodeSnapshot(decoded)
			if err != nil {
				t.Fatal(err)
			}
			claimed.ContextSnapshot = durable.ContextSnapshot{
				Format: envelope.Format, PolicyVersion: envelope.PolicyVersion,
				Digest: envelope.Digest, Payload: envelope.Payload,
			}
			modelCalls := 0
			worker := &engine.Engine{
				Store: store,
				Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					return &fakeModel{
						generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
							modelCalls++
							var hasQuestion, hasAnswer bool
							for _, message := range messages {
								hasQuestion = hasQuestion ||
									message.Content == "turn 1 question"
								hasAnswer = hasAnswer ||
									message.Content == "turn 1 answer"
								if message.Role == schema.Tool ||
									len(message.ToolCalls) != 0 {
									t.Errorf(
										"historical tool process survived reduction: %+v",
										message,
									)
								}
							}
							if hasQuestion != test.keepAnswer ||
								hasAnswer != test.keepAnswer {
								t.Errorf(
									"question=%v answer=%v want both %v",
									hasQuestion,
									hasAnswer,
									test.keepAnswer,
								)
							}
							if last := messages[len(messages)-1]; last.Role != schema.User ||
								last.Content != "do work" {
								t.Errorf("lost current turn: %+v", last)
							}
							return schema.AssistantMessage("finished", nil), nil
						},
					}, map[string]engine.Binding{
						"read": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "read"}},
					}, nil
				},
			}
			if err := worker.Execute(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			done, err := store.Get(ctx, "owner", run.ID)
			if err != nil || done.Status != durable.Completed || modelCalls != 1 {
				t.Fatalf("run=%+v calls=%d error=%v", done, modelCalls, err)
			}
		})
	}
}
