package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

type usageModel struct {
	generate         func() (*schema.Message, error)
	stream           func() (*schema.StreamReader[*schema.Message], error)
	disableCallbacks bool
}

func (base *usageModel) IsCallbacksEnabled() bool {
	return !base.disableCallbacks
}

func (base *usageModel) Generate(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if base.disableCallbacks {
		return base.generate()
	}
	ctx = callbacks.OnStart(ctx, &model.CallbackInput{Messages: input})
	message, err := base.generate()
	if err != nil {
		callbacks.OnError(ctx, err)
	} else {
		callbacks.OnEnd(ctx, &model.CallbackOutput{Message: message})
	}
	return message, err
}

func (base *usageModel) Stream(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	if base.disableCallbacks {
		return base.stream()
	}
	ctx = callbacks.OnStart(ctx, &model.CallbackInput{Messages: input})
	reader, err := base.stream()
	if err != nil {
		callbacks.OnError(ctx, err)
		return nil, err
	}
	_, wrapped := callbacks.OnEndWithStreamOutput(ctx, schema.StreamReaderWithConvert(reader, func(message *schema.Message) (*model.CallbackOutput, error) {
		return &model.CallbackOutput{Message: message}, nil
	}))
	return schema.StreamReaderWithConvert(wrapped, func(output *model.CallbackOutput) (*schema.Message, error) {
		return output.Message, nil
	}), nil
}

func (base *usageModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return base, nil
}

func invokeUsageModel(ctx context.Context, boundary model.BaseChatModel, streaming bool) (*schema.Message, error) {
	input := []*schema.Message{schema.UserMessage("do work")}
	if !streaming {
		return boundary.Generate(ctx, input)
	}
	reader, err := boundary.Stream(ctx, input)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	message, err := reader.Recv()
	if err != nil {
		return nil, err
	}
	if _, err = reader.Recv(); !errors.Is(err, io.EOF) {
		return nil, errors.New("merged stream did not end")
	}
	return message, nil
}

func TestModelUsageBoundaries(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		mode := "generate"
		if streaming {
			mode = "stream"
		}
		for _, scenario := range []string{"success", "missing usage", "budget before", "budget after", "record failure", "response error"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				store, _ := testDB(t)
				ctx := context.Background()
				run := submit(t, store, "lookup", durable.ReadOnly)
				goal, _, err := store.CreateGoal(ctx, durable.Goal{
					OwnerID: "owner", SessionID: run.SessionID,
					RequestKey: "usage", RequestHash: "usage", Objective: "finish",
					Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
					Budget:    durable.BudgetSpec{MaxTokens: 20},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
					t.Fatal(err)
				}
				claimed := getClaim(t, store)
				if scenario == "budget before" {
					if err = store.RecordUsage(ctx, claimed.Lease(), 0, "summary", 20, claimed.State); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "record failure" {
					if err = store.DB().Exec("ALTER TABLE durable_usage ADD CONSTRAINT reject_usage CHECK (tokens < 0)").Error; err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				responseErr := errors.New("provider interrupted after usage")
				response := func() (*schema.Message, error) {
					calls++
					message := schema.AssistantMessage("answer", nil)
					if scenario != "missing usage" {
						message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{
							PromptTokens: 3, CompletionTokens: 4,
						}}
					}
					if scenario == "budget after" {
						message.ResponseMeta.Usage.TotalTokens = 20
						message.ToolCalls = toolReply("lookup").ToolCalls
					}
					if scenario == "response error" {
						return message, responseErr
					}
					return message, nil
				}
				base := &usageModel{generate: response}
				base.stream = func() (*schema.StreamReader[*schema.Message], error) {
					message, callErr := response()
					reader, writer := schema.Pipe[*schema.Message](3)
					writer.Send(schema.AssistantMessage("ans", nil), nil)
					message.Content = "wer"
					writer.Send(message, nil)
					if callErr != nil {
						writer.Send(nil, callErr)
					}
					writer.Close()
					return reader, nil
				}
				boundary := engine.ModelBoundaryForTest(t, store, claimed, base)
				message, callErr := invokeUsageModel(ctx, boundary, streaming)
				wantTokens, wantCalls := int64(7), 1
				switch scenario {
				case "success":
					if callErr != nil || message.Content != "answer" {
						t.Fatalf("response = %+v, %v", message, callErr)
					}
				case "missing usage":
					wantTokens = 0
					if !errors.Is(callErr, engine.ErrModelUsageUnavailable) {
						t.Fatalf("missing usage error = %v", callErr)
					}
				case "budget before", "budget after":
					wantTokens = 20
					if scenario == "budget before" {
						wantCalls = 0
					}
					if !errors.Is(callErr, durable.ErrUsageBudgetExceeded) || message != nil {
						t.Fatalf("budget response = %+v, %v", message, callErr)
					}
				case "record failure":
					wantTokens = 0
					if callErr == nil || message != nil {
						t.Fatalf("unrecorded response = %+v, %v", message, callErr)
					}
				case "response error":
					if !errors.Is(callErr, responseErr) {
						t.Fatalf("provider error = %v", callErr)
					}
				}
				got, err := store.Get(ctx, "owner", run.ID)
				if err != nil {
					t.Fatal(err)
				}
				gotGoal, err := store.GetGoal(ctx, "owner", goal.ID)
				if err != nil {
					t.Fatal(err)
				}
				if calls != wantCalls || got.TokensUsed != wantTokens || gotGoal.TokensUsed != wantTokens {
					t.Fatalf("calls=%d run=%d goal=%d, want %d/%d/%d", calls, got.TokensUsed, gotGoal.TokensUsed, wantCalls, wantTokens, wantTokens)
				}
				var state engine.State
				if err = json.Unmarshal(got.State, &state); err != nil {
					t.Fatal(err)
				}
				if scenario == "success" || scenario == "response error" || scenario == "budget after" {
					if state.Turn != 1 {
						t.Fatalf("recorded turn = %d", state.Turn)
					}
				}
			})
		}
	}
}

func TestModelUsageStopsToolsAndRetriesKnownFailure(t *testing.T) {
	for _, scenario := range []string{"budget", "missing usage", "provider error", "provider error without callbacks"} {
		t.Run(scenario, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: run.SessionID,
				RequestKey: "usage", RequestHash: "usage", Objective: "finish",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
				Budget:    durable.BudgetSpec{MaxTokens: 7},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			toolCalls := 0
			executor := &engine.Engine{Store: store, Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
				base := &usageModel{disableCallbacks: scenario == "provider error without callbacks", generate: func() (*schema.Message, error) {
					message := toolReply("lookup")
					if scenario == "missing usage" {
						return message, nil
					}
					message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 7}}
					if strings.HasPrefix(scenario, "provider error") {
						message.ResponseMeta.Usage.TotalTokens = 2
						return message, errors.New("provider interrupted")
					}
					return message, nil
				}}
				base.stream = func() (*schema.StreamReader[*schema.Message], error) {
					message, callErr := base.generate()
					reader, writer := schema.Pipe[*schema.Message](2)
					writer.Send(message, nil)
					if callErr != nil {
						writer.Send(nil, callErr)
					}
					writer.Close()
					return reader, nil
				}
				return base, map[string]engine.Binding{"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
						toolCalls++
						return "unexpected", nil
					}},
				}}, nil
			}}
			if err = executor.Execute(ctx, getClaim(t, store)); err != nil {
				t.Fatal(err)
			}
			got, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus := durable.Failed
			wantTokens := int64(7)
			if scenario == "missing usage" || scenario == "provider error without callbacks" {
				wantStatus = durable.Blocked
				wantTokens = 0
			} else if scenario == "provider error" {
				wantStatus = durable.Queued
				wantTokens = 2
			}
			if got.Status != wantStatus || got.TokensUsed != wantTokens || toolCalls != 0 {
				t.Fatalf("status=%s tokens=%d tools=%d error=%q, want %s/%d/0", got.Status, got.TokensUsed, toolCalls, got.Error, wantStatus, wantTokens)
			}
			if scenario == "missing usage" && !strings.Contains(got.Error, engine.ErrModelUsageUnavailable.Error()) {
				t.Fatalf("missing usage failure = %+v", got)
			}
			var state engine.State
			if err = json.Unmarshal(got.State, &state); err != nil {
				t.Fatal(err)
			}
			if (state.InflightModel != nil) != (wantStatus == durable.Blocked) {
				t.Fatalf("status=%s inflight=%+v", got.Status, state.InflightModel)
			}
			gotGoal, err := store.GetGoal(ctx, "owner", goal.ID)
			if err != nil {
				t.Fatal(err)
			}
			if gotGoal.TokensUsed != wantTokens {
				t.Fatalf("goal tokens=%d, want %d", gotGoal.TokensUsed, wantTokens)
			}
			if strings.HasPrefix(scenario, "provider error") {
				if !strings.Contains(got.Error, "provider interrupted") || state.PendingModel != nil {
					t.Fatalf("provider failure changed: error=%q pending=%+v", got.Error, state.PendingModel)
				}
				wantTurn, wantRecords, wantRetries := 0, 0, 0
				if scenario == "provider error" {
					wantTurn, wantRecords, wantRetries = 1, 1, 1
				}
				events, err := store.Events(ctx, "owner", run.ID, 0)
				if err != nil {
					t.Fatal(err)
				}
				records, retries := 0, 0
				for _, event := range events {
					switch event.Kind {
					case "UsageRecorded":
						records++
					case "RunRetry":
						retries++
					}
				}
				if state.Turn != wantTurn || records != wantRecords || retries != wantRetries {
					t.Fatalf("turn=%d usage records=%d retries=%d, want %d/%d/%d", state.Turn, records, retries, wantTurn, wantRecords, wantRetries)
				}
			}
		})
	}
}

func TestRecordedModelResponseSurvivesTakeover(t *testing.T) {
	for _, toolDecision := range []bool{false, true} {
		name := "final"
		if toolDecision {
			name = "tool decision"
		}
		t.Run(name, func(t *testing.T) {
			store, dsn := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			modelCalls, toolCalls := 0, 0
			base := &usageModel{generate: func() (*schema.Message, error) {
				modelCalls++
				message := schema.AssistantMessage("answer", nil)
				if toolDecision {
					message = toolReply("lookup")
				}
				message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 7}}
				return message, nil
			}}
			boundary := engine.ModelBoundaryForTest(t, store, getClaim(t, store), base)
			if _, err := invokeUsageModel(ctx, boundary, false); err != nil {
				t.Fatal(err)
			}
			saved, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			var state engine.State
			if err = json.Unmarshal(saved.State, &state); err != nil {
				t.Fatal(err)
			}
			if state.PendingModel == nil || len(state.Checkpoint) != 0 || saved.TokensUsed != 7 {
				t.Fatalf("response was not saved before decision checkpoint: state=%+v tokens=%d", state, saved.TokensUsed)
			}
			if err = store.DB().Exec("UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID).Error; err != nil {
				t.Fatal(err)
			}
			replacement, err := durable.Open(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer replacement.Close()
			claimed, err := replacement.Claim(ctx, "replacement", 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			executor := &engine.Engine{Store: replacement, Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
				return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
					modelCalls++
					if !toolDecision || input[len(input)-1].Role != schema.Tool {
						t.Error("recorded model call was repeated")
					}
					message := schema.AssistantMessage("answer", nil)
					message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 3}}
					return message, nil
				}}, map[string]engine.Binding{"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
						toolCalls++
						return "found", nil
					}},
				}}, nil
			}}
			if err = executor.Execute(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			done, err := replacement.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantModels, wantTools, wantTokens := 1, 0, int64(7)
			if toolDecision {
				wantModels, wantTools, wantTokens = 2, 1, 10
			}
			if done.Status != durable.Completed || done.Output != "answer" || done.TokensUsed != wantTokens ||
				modelCalls != wantModels || toolCalls != wantTools {
				t.Fatalf("run=%+v models=%d tools=%d; want tokens=%d models=%d tools=%d", done, modelCalls, toolCalls, wantTokens, wantModels, wantTools)
			}
		})
	}
}

func TestModelUsageChargesFrozenPricing(t *testing.T) {
	for _, scenario := range []string{"priced", "model mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
				pricedSpec(2, 10)(spec)
				spec.PricingSnapshot.CachedInputPerMillion = pricePointer(1)
			})
			claimed := getClaim(t, store)
			if scenario == "model mismatch" {
				claimed.Spec.Model.Name = "unpriced-model"
			}
			calls := 0
			base := &usageModel{generate: func() (*schema.Message, error) {
				calls++
				message := schema.AssistantMessage("answer", nil)
				message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{
					PromptTokens:       1_000_000,
					PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 500_000},
					CompletionTokens:   100_000,
					TotalTokens:        1_100_000,
				}}
				return message, nil
			}}
			boundary := engine.ModelBoundaryForTest(t, store, claimed, base)
			message, callErr := invokeUsageModel(ctx, boundary, false)
			got, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "model mismatch" {
				if !errors.Is(callErr, engine.ErrModelPricingUnavailable) || message != nil ||
					calls != 0 || got.TokensUsed != 0 || got.CostUSD != 0 {
					t.Fatalf("err=%v message=%+v calls=%d run=%+v", callErr, message, calls, got)
				}
				return
			}
			wantCost := 500_000.0/1e6*2 + 500_000.0/1e6*1 + 100_000.0/1e6*10
			if callErr != nil || message.Content != "answer" || calls != 1 ||
				got.TokensUsed != 1_100_000 || got.CostUSD != wantCost {
				t.Fatalf("err=%v calls=%d run=%+v, want cost %v", callErr, calls, got, wantCost)
			}
		})
	}
}
