package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func TestUnpricedModelSurvivesTakeover(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		mode := "generate"
		if streaming {
			mode = "stream"
		}
		for _, scenario := range []string{"total only", "provider error"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				store, dsn := testDB(t)
				ctx := context.Background()
				run := submit(t, store, "lookup", durable.ReadOnly, pricedSpec(1, 2))
				claimed := getClaim(t, store)
				calls := 0
				providerErr := errors.New("provider interrupted after response")
				response := func() (*schema.Message, error) {
					calls++
					message := toolReply("lookup")
					message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7}}
					message.ResponseMeta.Usage = &schema.TokenUsage{TotalTokens: 7}
					if scenario == "provider error" {
						return message, providerErr
					}
					return message, nil
				}
				base := &usageModel{generate: response}
				base.stream = func() (*schema.StreamReader[*schema.Message], error) {
					message, err := response()
					reader, writer := schema.Pipe[*schema.Message](2)
					writer.Send(message, nil)
					if err != nil {
						writer.Send(nil, err)
					}
					writer.Close()
					return reader, nil
				}
				boundary := engine.ModelBoundaryForTest(t, store, claimed, base)
				message, callErr := invokeUsageModel(ctx, boundary, streaming)
				if message != nil || !errors.Is(callErr, engine.ErrModelPricingUnavailable) {
					t.Fatalf("response=%+v error=%v", message, callErr)
				}
				if scenario == "provider error" && !errors.Is(callErr, providerErr) {
					t.Fatalf("provider error lost: %v", callErr)
				}
				saved, err := store.Get(ctx, "owner", run.ID)
				if err != nil {
					t.Fatal(err)
				}
				var state engine.State
				if err = json.Unmarshal(saved.State, &state); err != nil {
					t.Fatal(err)
				}
				if saved.TokensUsed != 7 || saved.CostUSD != 0 || state.PricingFailure == "" || state.Turn != 1 {
					t.Fatalf("run=%+v state=%+v", saved, state)
				}
				if err = store.DB().Exec("UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID).Error; err != nil {
					t.Fatal(err)
				}
				replacement, err := durable.Open(ctx, dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer replacement.Close()
				reclaimed, err := replacement.Claim(ctx, "replacement", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				recovered := engine.ModelBoundaryForTest(t, replacement, reclaimed, base)
				if message, err = invokeUsageModel(ctx, recovered, streaming); message != nil || !errors.Is(err, engine.ErrModelPricingUnavailable) {
					t.Fatalf("takeover response=%+v error=%v", message, err)
				}
				executor := &engine.Engine{Store: replacement,
					Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
						t.Error("unpriced call reached adapter build")
						return nil, nil, errors.New("unexpected build")
					},
				}
				if err = executor.Execute(ctx, reclaimed); err != nil {
					t.Fatal(err)
				}
				done, err := replacement.Get(ctx, "owner", run.ID)
				if err != nil {
					t.Fatal(err)
				}
				var count int64
				if err = replacement.DB().Table("durable_usage").Where("run_id = ?", run.ID).Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				if calls != 1 || count != 1 || done.Status != durable.Failed || done.TokensUsed != 7 || done.CostUSD != 0 {
					t.Fatalf("calls=%d usage=%d run=%+v", calls, count, done)
				}
			})
		}
	}
}

func TestUnpricedSummarySurvivesTakeover(t *testing.T) {
	for _, scenario := range []string{"total only"} {
		t.Run(scenario, func(t *testing.T) {
			store, dsn := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly, pricedSpec(1, 2))
			claimed := getClaim(t, store)
			claimed.Spec.History = []durable.Message{{Role: "user", Content: "previous work"}}
			attemptCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			models, summaries := 0, 0
			executor := &engine.Engine{Store: store, CompactionMaxBytes: 1,
				Summarizer: summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
					summaries++
					usage := &schema.TokenUsage{PromptTokens: 5, CompletionTokens: 2, TotalTokens: 7}
					if scenario == "total only" {
						usage = &schema.TokenUsage{TotalTokens: 7}
					}
					err := input.RecordUsage(summaryCtx, usage, 7)
					if !errors.Is(err, engine.ErrModelPricingUnavailable) {
						t.Errorf("summary pricing error=%v", err)
					}
					cancel()
					return "", err
				}),
				Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
						models++
						return schema.AssistantMessage("unexpected", nil), nil
					}}, map[string]engine.Binding{"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}}}, nil
				},
			}
			if err := executor.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel after accounting: %v", err)
			}
			saved, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			var state engine.State
			if err = json.Unmarshal(saved.State, &state); err != nil {
				t.Fatal(err)
			}
			if state.SummaryCalls != 1 || state.PricingFailure == "" || saved.TokensUsed != 7 || saved.CostUSD != 0 || saved.Status != durable.Running {
				t.Fatalf("run=%+v state=%+v", saved, state)
			}
			if err = store.DB().Exec("UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID).Error; err != nil {
				t.Fatal(err)
			}
			replacement, err := durable.Open(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer replacement.Close()
			reclaimed, err := replacement.Claim(ctx, "replacement", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			executor.Store = replacement
			if err = executor.Execute(ctx, reclaimed); err != nil {
				t.Fatal(err)
			}
			done, err := replacement.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if done.Status != durable.Failed || !strings.Contains(done.Error, engine.ErrModelPricingUnavailable.Error()) ||
				models != 0 || summaries != 1 || done.TokensUsed != 7 || done.CostUSD != 0 {
				t.Fatalf("run=%+v summaries=%d models=%d", done, summaries, models)
			}
		})
	}
}

func TestCostBudgetRequiresPricing(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "missing table"
		if configured {
			name = "explicit free model"
		}
		t.Run(name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: run.SessionID, RequestKey: "cost", RequestHash: "cost", Objective: "finish",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone}, Budget: durable.BudgetSpec{MaxCostUSD: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			claimed := getClaim(t, store)
			calls := 0
			executor := &engine.Engine{Store: store, Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
				calls++
				return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
					return &schema.Message{Role: schema.Assistant, Content: "done", ResponseMeta: &schema.ResponseMeta{Usage: &schema.TokenUsage{PromptTokens: 3}}}, nil
				}}, map[string]engine.Binding{"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}}}, nil
			}}
			if configured {
				pricedSpec(0, 0)(&claimed.Spec)
			}
			if err = executor.Execute(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			done, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if configured {
				if calls != 1 || done.Status != durable.Completed || done.CostUSD != 0 || done.TokensUsed != 3 {
					t.Fatalf("calls=%d run=%+v", calls, done)
				}
			} else if calls != 0 || done.Status != durable.Failed || !strings.Contains(done.Error, engine.ErrModelPricingUnavailable.Error()) || done.TokensUsed != 0 {
				t.Fatalf("calls=%d run=%+v", calls, done)
			}
		})
	}
}

func TestFrozenPricingSurvivesTakeoverAndPriceChange(t *testing.T) {
	store, dsn := testDB(t)
	ctx := context.Background()
	configure := func(input, cached, output float64) func(*durable.Spec) {
		return func(spec *durable.Spec) {
			pricedSpec(input, output)(spec)
			spec.PricingSnapshot.CachedInputPerMillion = pricePointer(cached)
		}
	}
	run := submit(t, store, "lookup", durable.ReadOnly, configure(2, 0.5, 10))
	claimed := getClaim(t, store)
	calls := 0
	response := func() *schema.Message {
		calls++
		return &schema.Message{Role: schema.Assistant, Content: "done", ResponseMeta: &schema.ResponseMeta{Usage: &schema.TokenUsage{
			PromptTokens: 1_000_000, CompletionTokens: 100_000, TotalTokens: 1_100_000,
			PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 500_000},
		}}}
	}
	providerErr := errors.New("interrupted after usage")
	first := engine.ModelBoundaryForTest(t, store, claimed, &usageModel{generate: func() (*schema.Message, error) {
		return response(), providerErr
	}})
	if _, err := invokeUsageModel(ctx, first, false); !errors.Is(err, providerErr) {
		t.Fatalf("first call: %v", err)
	}
	newRun := submit(t, store, "lookup", durable.ReadOnly, configure(20, 5, 100))
	newClaim := getClaim(t, store)
	if newClaim.ID != newRun.ID {
		t.Fatalf("claimed %s, want %s", newClaim.ID, newRun.ID)
	}
	newBoundary := engine.ModelBoundaryForTest(t, store, newClaim, &usageModel{generate: func() (*schema.Message, error) { return response(), nil }})
	if _, err := invokeUsageModel(ctx, newBoundary, false); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Exec("UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	replacement, err := durable.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	reclaimed, err := replacement.Claim(ctx, "replacement", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.ID != run.ID || reclaimed.Epoch <= claimed.Epoch || reclaimed.Spec.PricingSnapshot == nil ||
		reclaimed.Spec.PricingSnapshot.InputPerMillion != 2 || *reclaimed.Spec.PricingSnapshot.CachedInputPerMillion != 0.5 {
		t.Fatalf("snapshot not restored: %+v", reclaimed)
	}
	recovered := engine.ModelBoundaryForTest(t, replacement, reclaimed, &usageModel{generate: func() (*schema.Message, error) { return response(), nil }})
	if _, err = invokeUsageModel(ctx, recovered, false); err != nil {
		t.Fatal(err)
	}
	oldSaved, err := replacement.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	newSaved, err := replacement.Get(ctx, "owner", newRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || oldSaved.TokensUsed != 2_200_000 || math.Abs(oldSaved.CostUSD-4.5) > 1e-12 || math.Abs(newSaved.CostUSD-22.5) > 1e-12 {
		t.Fatalf("calls=%d old cost=%v tokens=%d new cost=%v", calls, oldSaved.CostUSD, oldSaved.TokensUsed, newSaved.CostUSD)
	}
}

func TestLegacyRunFailsBeforeBuildAndSummary(t *testing.T) {
	store, _ := testDB(t)
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed := getClaim(t, store)
	claimed.Spec.PricingFrozen = false
	executor := &engine.Engine{Store: store, CompactionMaxBytes: 1,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			t.Error("legacy run reached adapter build")
			return nil, nil, errors.New("unexpected build")
		},
		Summarizer: summaryFunc(func(context.Context, engine.SummaryInput) (string, error) {
			t.Error("legacy run reached summarizer")
			return "", errors.New("unexpected summary")
		}),
	}
	if err := executor.Execute(context.Background(), claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(context.Background(), "owner", run.ID)
	if err != nil || saved.Status != durable.Failed || !strings.Contains(saved.Error, "no frozen pricing") || saved.TokensUsed != 0 {
		t.Fatalf("run=%+v error=%v", saved, err)
	}
}
