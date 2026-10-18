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

// Delivery closes the Coding round. Asking the model for another tool after it
// succeeds reproduces the user's late read_file -> STALE_EXECUTION failure.
func TestReturnDirectDeliveryDoesNotInvokeAnotherModelOrTool(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "deliver", durable.Unsafe)
	claimed := getClaim(t, s)
	claimed.Spec.MaxTurns = 1
	var configured durable.ToolSpec
	if err := json.Unmarshal(
		[]byte(
			`{"name":"deliver","returnDirectly":true,"policy":"Unsafe","url":"http://localhost/deliver","parameters":{"type":"object"}}`,
		),
		&configured,
	); err != nil {
		t.Fatal(err)
	}
	claimed.Spec.Tools = []durable.ToolSpec{
		configured,
		{
			Name:       "read_file",
			Policy:     durable.ReadOnly,
			URL:        "http://localhost/read_file",
			Parameters: json.RawMessage(`{"type":"object"}`),
		},
	}
	var modelCalls, deliveries, lateReads int
	const delivered = `{"status":"Passed","head":"0123456789012345678901234567890123456789"}`
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				modelCalls++
				if deliveries > 0 {
					return toolReply("read_file"), nil
				}
				return toolReply("deliver"), nil
			}}, map[string]engine.Binding{
				"deliver": {
					Policy: durable.Unsafe,
					Tool: &fakeTool{
						name:   "deliver",
						invoke: func(context.Context, string) (string, error) { deliveries++; return delivered, nil },
					},
				},
				"read_file": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "read_file",
						invoke: func(context.Context, string) (string, error) {
							lateReads++
							return "", errors.New(
								"tool read_file returned HTTP 409: STALE_EXECUTION: 本轮执行已变化",
							)
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
	if err != nil || done.Status != durable.Completed || done.Output != delivered || modelCalls != 1 ||
		deliveries != 1 ||
		lateReads != 0 {
		t.Fatalf(
			"status=%s output=%s error=%s modelCalls=%d deliveries=%d lateReads=%d err=%v",
			done.Status,
			done.Output,
			done.Error,
			modelCalls,
			deliveries,
			lateReads,
			err,
		)
	}
}

type correctableTestError string

func (e correctableTestError) Error() string     { return string(e) }
func (e correctableTestError) Correctable() bool { return true }

func TestReturnDirectErrorsResumeModelWithinTurnBudget(t *testing.T) {
	for _, tc := range []struct {
		name        string
		correctable bool
		maxTurns    int
		wantStatus  durable.Status
		wantOutput  string
		wantModels  int
	}{
		{name: "correctable", correctable: true, maxTurns: 2, wantStatus: durable.Completed, wantOutput: "corrected", wantModels: 2},
		{name: "error result", maxTurns: 2, wantStatus: durable.Completed, wantOutput: "corrected", wantModels: 2},
		{name: "last turn error", maxTurns: 1, wantStatus: durable.Failed, wantModels: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testDB(t)
			ctx := context.Background()
			r := submit(t, s, "deliver", durable.ReadOnly)
			claimed := getClaim(t, s)
			claimed.Spec.Tools[0].ReturnDirectly = true
			claimed.Spec.MaxTurns = tc.maxTurns
			const errorResult = `{"error":"bad input"}`
			modelCalls, toolCalls := 0, 0
			e := &engine.Engine{
				Store: s,
				Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
						modelCalls++
						if modelCalls == 1 {
							return toolReply("deliver"), nil
						}
						if modelCalls > 2 {
							t.Fatal("direct tool error exceeded the model turn budget")
						}
						last := msgs[len(msgs)-1]
						if last.Role != schema.Tool {
							t.Errorf("model did not receive tool feedback: %+v", last)
						}
						if tc.correctable {
							if !strings.Contains(last.Content, "INVALID_TOOL_INPUT") || !strings.Contains(last.Content, "bad input") {
								t.Errorf("missing correctable feedback: %s", last.Content)
							}
						} else if last.Content != errorResult {
							t.Errorf("error result changed: %s", last.Content)
						}
						return schema.AssistantMessage("corrected", nil), nil
					}}, map[string]engine.Binding{
						"deliver": {
							Policy: durable.ReadOnly,
							Tool: &fakeTool{name: "deliver", invoke: func(context.Context, string) (string, error) {
								toolCalls++
								if tc.correctable {
									return "", correctableTestError("bad input")
								}
								return errorResult, nil
							}},
						},
					}, nil
				},
			}
			if err := e.Execute(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			done, err := s.Get(ctx, "owner", r.ID)
			if err != nil || done.Status != tc.wantStatus || done.Output != tc.wantOutput ||
				modelCalls != tc.wantModels || toolCalls != 1 {
				t.Fatalf("status=%s output=%q error=%q models=%d tools=%d err=%v", done.Status, done.Output, done.Error, modelCalls, toolCalls, err)
			}
		})
	}
}

func TestReturnDirectUsesFirstSelectedEmptyResult(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "first", durable.ReadOnly)
	claimed := getClaim(t, s)
	claimed.Spec.Tools[0].ReturnDirectly = true
	claimed.Spec.Tools = append(claimed.Spec.Tools, durable.ToolSpec{
		Name:           "second",
		ReturnDirectly: true,
		Policy:         durable.ReadOnly,
		URL:            "http://localhost/second",
		Parameters:     json.RawMessage(`{"type":"object"}`),
	})
	modelCalls, firstCalls, secondCalls := 0, 0, 0
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				modelCalls++
				if modelCalls > 1 {
					t.Fatal("successful direct tools invoked another model")
				}
				msg := toolReply("first")
				msg.ToolCalls = append(msg.ToolCalls, schema.ToolCall{
					Type:     "function",
					Function: schema.FunctionCall{Name: "second", Arguments: `{}`},
				})
				return msg, nil
			}}, map[string]engine.Binding{
				"first": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "first", invoke: func(context.Context, string) (string, error) {
					firstCalls++
					return "", nil
				}}},
				"second": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "second", invoke: func(context.Context, string) (string, error) {
					secondCalls++
					return "second", nil
				}}},
			}, nil
		},
	}
	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "" ||
		modelCalls != 1 || firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("status=%s output=%q error=%q models=%d first=%d second=%d err=%v", done.Status, done.Output, done.Error, modelCalls, firstCalls, secondCalls, err)
	}
}

func TestReturnDirectPauseKeepsLaterReviewResult(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	a, err := s.CreateAgent(ctx, "owner", "pause-review", durable.AgentConfig{
		Model:    durable.ModelSpec{Name: "test", BaseURL: "http://localhost/v1"},
		MaxTurns: 5,
		Tools: []durable.ToolSpec{
			{
				Name:           "deliver",
				ReturnDirectly: true,
				Policy:         durable.ReadOnly,
				URL:            "http://localhost/deliver",
				Parameters:     json.RawMessage(`{"type":"object"}`),
			},
			{
				Name:       "submit",
				Policy:     durable.Unsafe,
				URL:        "http://localhost/submit",
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	})
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
	r, _, err := s.Submit(ctx, "owner", session.ID, durable.NewID(), "do work", time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	claimed := getClaim(t, s)
	modelCalls, deliverCalls, submitCalls := 0, 0, 0
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			modelCalls++
			if modelCalls > 1 {
				last := msgs[len(msgs)-1]
				if last.Role != schema.Tool || last.Content != "confirmed receipt" {
					t.Errorf("later review result was bypassed: %+v", last)
				}
				return schema.AssistantMessage("reviewed", nil), nil
			}
			msg := toolReply("deliver")
			msg.ToolCalls = append(msg.ToolCalls, schema.ToolCall{
				Type:     "function",
				Function: schema.FunctionCall{Name: "submit", Arguments: `{}`},
			})
			return msg, nil
		}}, map[string]engine.Binding{
			"deliver": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "deliver", invoke: func(context.Context, string) (string, error) {
				deliverCalls++
				return "delivered", nil
			}}},
			"submit": {Policy: durable.Unsafe, Tool: &fakeTool{name: "submit", invoke: func(context.Context, string) (string, error) {
				submitCalls++
				return "", errors.New("receipt unknown")
			}}},
		}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	paused, err := s.Get(ctx, "owner", r.ID)
	if err != nil || paused.Status != durable.NeedsReview || deliverCalls != 1 || submitCalls != 1 {
		t.Fatalf("paused=%+v deliver=%d submit=%d err=%v", paused, deliverCalls, submitCalls, err)
	}
	if err = s.SupplyResult(ctx, "owner", r.ID, paused.WaitingStep, "confirmed receipt", durable.NeedsReview); err != nil {
		t.Fatal(err)
	}
	if err = e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "reviewed" ||
		modelCalls != 2 || deliverCalls != 1 || submitCalls != 1 {
		t.Fatalf("status=%s output=%q error=%q models=%d deliver=%d submit=%d err=%v", done.Status, done.Output, done.Error, modelCalls, deliverCalls, submitCalls, err)
	}
}

func TestReturnDirectCachedResultDoesNotRepeatTool(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "deliver", durable.Unsafe)
	claimed := getClaim(t, s)
	claimed.Spec.Tools[0].ReturnDirectly = true
	interrupted, cancel := context.WithCancel(ctx)
	defer cancel()
	modelCalls, toolCalls := 0, 0
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
			modelCalls++
			return toolReply("deliver"), nil
		}}, map[string]engine.Binding{
			"deliver": {Policy: durable.Unsafe, Tool: &fakeTool{name: "deliver", invoke: func(ctx context.Context, _ string) (string, error) {
				toolCalls++
				if toolCalls > 1 {
					return "", errors.New("tool repeated")
				}
				cancel()
				return "", ctx.Err()
			}}},
		}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err := e.Execute(interrupted, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := e.Execute(ctx, claimAfterExpiry(t, s)); err != nil {
		t.Fatal(err)
	}
	paused, err := s.Get(ctx, "owner", r.ID)
	if err != nil || paused.Status != durable.NeedsReview || toolCalls != 1 {
		t.Fatalf("paused=%+v tools=%d err=%v", paused, toolCalls, err)
	}
	if err = s.SupplyResult(ctx, "owner", r.ID, paused.WaitingStep, "receipt", durable.NeedsReview); err != nil {
		t.Fatal(err)
	}
	if err = e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "receipt" ||
		modelCalls != 1 || toolCalls != 1 {
		t.Fatalf("status=%s output=%q error=%q models=%d tools=%d err=%v", done.Status, done.Output, done.Error, modelCalls, toolCalls, err)
	}
}
