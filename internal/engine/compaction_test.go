package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type summaryFunc func(context.Context, engine.SummaryInput) (string, error)

func (summarize summaryFunc) Summarize(ctx context.Context, input engine.SummaryInput) (string, error) {
	return summarize(ctx, input)
}

func TestSummaryAndModelUseSameFrozenPricing(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
		pricedSpec(2, 10)(spec)
		spec.PricingSnapshot.CachedInputPerMillion = pricePointer(0.5)
		spec.History = []durable.Message{
			{Role: "user", Content: "previous question"},
			{Role: "assistant", Content: "previous answer"},
		}
	})
	claimed := getClaim(t, store)
	usage := &schema.TokenUsage{
		PromptTokens: 1_000_000, CompletionTokens: 100_000, TotalTokens: 1_100_000,
		PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 500_000},
	}
	models, summaries := 0, 0
	executor := &engine.Engine{
		Store:              store,
		CompactionMaxBytes: 1,
		Summarizer: summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
			summaries++
			if !input.Spec.PricingFrozen || input.Spec.PricingSnapshot == nil ||
				input.Spec.PricingSnapshot.Model != input.Spec.Model.Name ||
				input.Spec.PricingSnapshot.InputPerMillion != 2 {
				t.Fatalf("summary did not receive frozen pricing: %+v", input.Spec)
			}
			return "previous work summary", input.RecordUsage(summaryCtx, usage, 1_100_000)
		}),
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				models++
				return &schema.Message{
					Role:         schema.Assistant,
					Content:      "done",
					ResponseMeta: &schema.ResponseMeta{Usage: usage},
				}, nil
			}}, map[string]engine.Binding{"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}}}, nil
		},
	}
	if err := executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.Status != durable.Completed || models != 1 || summaries != 1 ||
		saved.TokensUsed != 2_200_000 ||
		saved.CostUSD != 4.5 {
		t.Fatalf("run=%+v models=%d summaries=%d error=%v", saved, models, summaries, err)
	}
	var records []struct {
		Call    string
		CostUSD float64
	}
	if err := store.DB().
		Table("durable_usage").
		Where("run_id = ?", run.ID).
		Order("call").
		Find(&records).
		Error; err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Call != "model" || records[1].Call != "summary" ||
		records[0].CostUSD != 2.25 ||
		records[1].CostUSD != 2.25 {
		t.Fatalf("usage=%+v", records)
	}
}

func checkToolPairs(messages []*schema.Message) error {
	pending := map[string]bool{}
	for _, message := range messages {
		if message.Role == schema.Assistant {
			if len(pending) != 0 {
				return errors.New("assistant precedes pending tool results")
			}
			for _, call := range message.ToolCalls {
				pending[call.ID] = true
			}
		}
		if message.Role == schema.Tool {
			if !pending[message.ToolCallID] {
				return fmt.Errorf("unmatched result %s", message.ToolCallID)
			}
			delete(pending, message.ToolCallID)
		}
	}
	if len(pending) != 0 {
		return errors.New("incomplete tool batch")
	}
	return nil
}

func TestCompactionProjectionCompletes(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	modelCalls, toolCalls, summaries := 0, 0, 0
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		previous := ""
		if summaries > 1 {
			previous = "Confirmed facts: first batch completed; continue lookup."
		}
		if input.PreviousSummary != previous || len(input.Messages) != 3 ||
			input.Messages[0].ToolCalls[0].ID != fmt.Sprintf("turn_%d_tool_0", summaries) {
			return "", fmt.Errorf("unexpected summary prefix: %+v", input)
		}
		if err := input.RecordUsage(
			summaryCtx,
			&schema.TokenUsage{PromptTokens: 12, CompletionTokens: 5, TotalTokens: 17},
			17,
		); err != nil {
			return "", err
		}
		return "Confirmed facts: first batch completed; continue lookup.", nil
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{
			generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
				modelCalls++
				if err := checkToolPairs(messages); err != nil {
					return nil, err
				}
				if modelCalls <= 3 {
					reply := toolReply("lookup")
					reply.ToolCalls = append(reply.ToolCalls, reply.ToolCalls[0])
					return reply, nil
				}
				summaryCount, resultCount, queryCount := 0, 0, 0
				for _, message := range messages {
					if strings.Contains(message.Content, "Confirmed facts: first batch") {
						summaryCount++
					}
					if strings.Contains(message.Content, "do work") && message.Role == schema.User {
						queryCount++
					}
					if message.Role == schema.Tool {
						resultCount++
						if strings.HasPrefix(message.ToolCallID, "turn_1_") {
							return nil, errors.New(
								"compacted tool result leaked into model input",
							)
						}
					}
				}
				if summaryCount != 1 || resultCount != 2 || queryCount != 1 {
					return nil, fmt.Errorf(
						"model input summary=%d tools=%d query=%d",
						summaryCount,
						resultCount,
						queryCount,
					)
				}
				return schema.AssistantMessage("recovered with summary", nil), nil
			},
		}, map[string]engine.Binding{
			"lookup": {
				Policy: durable.ReadOnly,
				Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
					toolCalls++
					return fmt.Sprintf("result-%d", toolCalls), nil
				}},
			},
		}, nil
	}
	first := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = first.Execute(attemptCtx, claimed); err != nil {
		t.Fatalf("first execution: %v", err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.Status != durable.Completed || saved.CompactedSummary == "" || saved.FirstKeptSeq != 8 {
		t.Fatalf("saved projection = %+v, %v", saved, err)
	}
	var savedState engine.State
	if err = json.Unmarshal(saved.State, &savedState); err != nil {
		t.Fatal(err)
	}
	if savedState.SummaryCalls != 2 || savedState.InflightModel != nil {
		t.Fatalf("saved state: summaryCalls=%d inflight=%v", savedState.SummaryCalls, savedState.InflightModel)
	}
	if saved.Output != "recovered with summary" || toolCalls != 6 || summaries != 2 || modelCalls != 4 {
		t.Fatalf(
			"saved=%+v toolCalls=%d summaries=%d modelCalls=%d",
			saved,
			toolCalls,
			summaries,
			modelCalls,
		)
	}
	audit, err := store.Messages(ctx, "owner", run.SessionID, 0, 200)
	if err != nil || len(audit) != 11 || audit[1].ToolCalls[0].ID != "turn_1_tool_0" {
		t.Fatalf("audit lost compacted messages: count=%d error=%v", len(audit), err)
	}
	var summaryUsage struct {
		Count  int64
		Tokens int64
	}
	if err = store.DB().Table("durable_usage").
		Select("count(*) AS count, COALESCE(sum(tokens), 0) AS tokens").
		Where("run_id = ? AND call = ?", run.ID, "summary").Scan(&summaryUsage).Error; err != nil {
		t.Fatal(err)
	}
	if summaryUsage.Count != 2 || summaryUsage.Tokens != 34 {
		t.Fatalf("summary usage=%+v", summaryUsage)
	}
}

func TestInflightSummaryBlocksReplacement(t *testing.T) {
	store, dsn := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		// The worker is lost after the summary request was dispatched but
		// before its usage was persisted; consumption is unknown.
		cancel()
		return "", context.Canceled
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
			return toolReply("lookup"), nil
		}}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly,
			Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
				return "tool result", nil
			}},
		}}, nil
	}
	first := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = first.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("first attempt: %v", err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var savedState engine.State
	if err = json.Unmarshal(saved.State, &savedState); err != nil {
		t.Fatal(err)
	}
	if savedState.InflightModel == nil || savedState.InflightModel.Kind != "summary" {
		t.Fatalf("expected inflight summary marker, got %+v", savedState.InflightModel)
	}
	if err = store.DB().Exec(
		"UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID,
	).Error; err != nil {
		t.Fatal(err)
	}
	reopened, err := durable.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replacement, err := reopened.Claim(ctx, "replacement-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var replacementBuilds, replacementSummaries int
	second := &engine.Engine{
		Store: reopened,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			replacementBuilds++
			return build(context.Background(), nil, durable.Spec{})
		},
		Summarizer: summaryFunc(func(context.Context, engine.SummaryInput) (string, error) {
			replacementSummaries++
			return "replayed summary", nil
		}),
		CompactionMaxBytes: 1,
	}
	if err = second.Execute(ctx, replacement); err != nil {
		t.Fatalf("replacement must commit the block boundary: %v", err)
	}
	done, err := reopened.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Blocked {
		t.Fatalf("expected blocked run, got %+v", done)
	}
	if !strings.Contains(done.Error, "unknown consumption") || !strings.Contains(done.Error, "summary") {
		t.Fatalf("unexpected block detail: %q", done.Error)
	}
	if replacementBuilds != 0 || replacementSummaries != 0 {
		t.Fatalf(
			"replacement must not build or replay the summary: builds=%d summaries=%d",
			replacementBuilds,
			replacementSummaries,
		)
	}
}

func TestBilledSummaryPersistsAfterParentCancel(t *testing.T) {
	store, dsn := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	modelCalls, toolCalls, summaries := 0, 0, 0
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{
			generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
				modelCalls++
				if err := checkToolPairs(input); err != nil {
					return nil, err
				}
				if modelCalls <= 2 {
					return toolReply("lookup"), nil
				}
				for _, message := range input {
					if message.ToolCallID == "turn_1_tool_0" {
						return nil, errors.New(
							"compacted tool result leaked into replacement input",
						)
					}
				}
				if len(input) == 0 || !strings.Contains(input[0].Content, "saved summary") {
					return nil, fmt.Errorf("replacement input lost summary: %+v", input)
				}
				return schema.AssistantMessage("done after takeover", nil), nil
			},
		}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly,
			Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
				toolCalls++
				return "tool result", nil
			}},
		}}, nil
	}
	first := &engine.Engine{
		Store: store,
		Build: build,
		Summarizer: summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
			summaries++
			if err := input.RecordUsage(
				summaryCtx,
				&schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
				6,
			); err != nil {
				return "", err
			}
			cancel()
			return "saved summary", nil
		}),
		CompactionMaxBytes: 1,
	}
	if err = first.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("first attempt: %v", err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var savedState engine.State
	if err = json.Unmarshal(saved.State, &savedState); err != nil {
		t.Fatal(err)
	}
	if saved.CompactedSummary != "saved summary" || saved.FirstKeptSeq == 0 ||
		savedState.InflightModel != nil || savedState.SummaryCalls != 1 {
		t.Fatalf("summary was not durable after billing: run=%+v state=%+v", saved, savedState)
	}
	if err = store.DB().Exec(
		"UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID,
	).Error; err != nil {
		t.Fatal(err)
	}
	reopened, err := durable.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replacement, err := reopened.Claim(ctx, "replacement-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second := &engine.Engine{
		Store: reopened,
		Build: build,
		Summarizer: summaryFunc(func(context.Context, engine.SummaryInput) (string, error) {
			summaries++
			return "unexpected second summary", nil
		}),
		CompactionMaxBytes: 1,
	}
	if err = second.Execute(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	done, err := reopened.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var summaryUsage int64
	if err = reopened.DB().Table("durable_usage").
		Where("run_id = ? AND call = ?", run.ID, "summary").
		Count(&summaryUsage).Error; err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Completed || done.Output != "done after takeover" ||
		summaries != 1 || summaryUsage != 1 || toolCalls != 2 || modelCalls != 3 {
		t.Fatalf(
			"done=%+v summaries=%d usage=%d tools=%d models=%d",
			done,
			summaries,
			summaryUsage,
			toolCalls,
			modelCalls,
		)
	}
}

func TestCompactionFailureRetriesWithoutMovingBoundary(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	summaries, models := 0, 0
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		if err := input.RecordUsage(
			summaryCtx,
			&schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
			6,
		); err != nil {
			return "", err
		}
		if summaries == 1 {
			return "", errors.New("summary endpoint unavailable")
		}
		if input.PreviousSummary != "" || len(input.Messages) != 4 {
			return "", fmt.Errorf("failed prefix was lost: %+v", input)
		}
		return "Recovered summary", nil
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
			models++
			if err := checkToolPairs(input); err != nil {
				return nil, err
			}
			if models == 3 {
				saved, readErr := store.Get(ctx, "owner", run.ID)
				if readErr != nil || saved.FirstKeptSeq != 0 || saved.CompactedSummary != "" {
					return nil, errors.New("failed summarization advanced the boundary")
				}
				found := false
				for _, message := range input {
					if message.ToolCallID == "turn_1_tool_0" {
						found = true
					}
				}
				if found {
					return nil, errors.New("round fallback kept the old model input")
				}
			}
			if models <= 3 {
				return toolReply("lookup"), nil
			}
			return schema.AssistantMessage("done", nil), nil
		}}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly,
			Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
				return "tool result", nil
			}},
		}}, nil
	}
	executor := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.Status != durable.Completed || saved.CompactedSummary != "Recovered summary" ||
		summaries != 2 || saved.FirstKeptSeq != 6 {
		t.Fatalf("saved=%+v summaries=%d error=%v", saved, summaries, err)
	}
	events, err := store.Events(ctx, "owner", run.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, event := range events {
		if event.Kind == "CompactionFailed" {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("compaction failures = %d, want 1", failures)
	}
}

func TestCompactionRecordedUsageSurvivesSummaryError(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	summaries, models := 0, 0
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		// Usage is recorded, then the summary call fails. Consumption is
		// known, so the run must not be blocked or double-charged.
		if err := input.RecordUsage(
			summaryCtx,
			&schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
			6,
		); err != nil {
			return "", err
		}
		return "", errors.New("summary response stream reset")
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
			models++
			if models <= 2 {
				return toolReply("lookup"), nil
			}
			return schema.AssistantMessage("done without summary", nil), nil
		}}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly,
			Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
				return "tool result", nil
			}},
		}}, nil
	}
	executor := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != durable.Completed || saved.Output != "done without summary" || saved.CompactedSummary != "" {
		t.Fatalf("saved=%+v", saved)
	}
	var savedState engine.State
	if err = json.Unmarshal(saved.State, &savedState); err != nil {
		t.Fatal(err)
	}
	if savedState.InflightModel != nil || savedState.SummaryCalls != 1 {
		t.Fatalf("state: inflight=%+v summaryCalls=%d", savedState.InflightModel, savedState.SummaryCalls)
	}
	var summaryUsage struct {
		Count  int64
		Tokens int64
	}
	if err = store.DB().Table("durable_usage").
		Select("count(*) AS count, COALESCE(sum(tokens), 0) AS tokens").
		Where("run_id = ? AND call = ?", run.ID, "summary").Scan(&summaryUsage).Error; err != nil {
		t.Fatal(err)
	}
	if summaryUsage.Count != 1 || summaryUsage.Tokens != 6 {
		t.Fatalf("summary usage double counted or lost: %+v", summaryUsage)
	}
	events, err := store.Events(ctx, "owner", run.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	failures, blocked := 0, 0
	for _, event := range events {
		switch event.Kind {
		case "CompactionFailed":
			failures++
		case "RunBlocked":
			blocked++
		}
	}
	if failures != 1 || blocked != 0 {
		t.Fatalf("failures=%d blocked=%d", failures, blocked)
	}
}

func TestCompactionIncludesFrozenHistory(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimed.Spec.SystemPrompt = "literal system prompt {keep}"
	claimed.Spec.History = []durable.Message{
		{Role: "user", Content: "old question"},
		{Role: "assistant", Content: strings.Repeat("old answer", 50)},
	}
	data, err := json.Marshal(claimed.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DB().
		Exec("UPDATE durable_run SET spec = ? WHERE id = ?", string(data), run.ID).
		Error; err != nil {
		t.Fatal(err)
	}
	summaries := 0
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		if len(input.Messages) != 2 || input.Messages[0].Content != "old question" {
			return "", errors.New("history was not included in summary")
		}
		return "Previous conversation summary", input.RecordUsage(
			summaryCtx,
			&schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
			6,
		)
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
			if len(input) != 3 || input[0].Role != schema.System ||
				input[0].Content != "literal system prompt {keep}" ||
				!strings.Contains(input[1].Content, "Previous conversation summary") ||
				input[2].Content != "do work" {
				return nil, fmt.Errorf("bad history projection: %+v", input)
			}
			return schema.AssistantMessage("done", nil), nil
		}}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"},
		}}, nil
	}
	executor := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.Status != durable.Completed || summaries != 1 || saved.FirstKeptSeq != 2 {
		t.Fatalf("saved=%+v summaries=%d error=%v", saved, summaries, err)
	}
}

func TestCompactionUsageGovernance(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		precharged       int64
		limit            int64
		tokens           int64
		summaryErr       error
		recordFails      bool
		wantModels       int
		wantSummaries    int
		wantFailures     int
		wantUsage        int64
		wantSummaryCalls int64
		wantFailure      string
		wantBlocked      bool
		costLimit        float64
		wantCost         float64
	}{
		{name: "success", limit: 100, tokens: 17, wantModels: 1, wantSummaries: 1, wantUsage: 17, wantSummaryCalls: 1},
		{name: "budget already reached", limit: 10, precharged: 10, wantFailure: durable.ErrUsageBudgetExceeded.Error()},
		{name: "summary crosses budget", limit: 10, tokens: 17, wantSummaries: 1, wantUsage: 17, wantSummaryCalls: 1, wantFailure: durable.ErrUsageBudgetExceeded.Error()},
		{name: "summary cost crosses budget", limit: 100, costLimit: 0.00001, tokens: 17, wantSummaries: 1, wantUsage: 17, wantSummaryCalls: 1, wantCost: 0.000017, wantFailure: durable.ErrUsageBudgetExceeded.Error()},
		{name: "failed summary charged", limit: 100, tokens: 9, summaryErr: errors.New("incomplete summary"), wantModels: 1, wantSummaries: 1, wantFailures: 1, wantUsage: 9, wantSummaryCalls: 1},
		{name: "missing usage stops execution", limit: 100, summaryErr: engine.ErrModelUsageUnavailable, wantSummaries: 1, wantBlocked: true, wantFailure: engine.ErrModelUsageUnavailable.Error()},
		{name: "successful summary without usage blocks", limit: 100, wantSummaries: 1, wantBlocked: true, wantFailure: engine.ErrModelUsageUnavailable.Error()},
		{name: "ordinary summary error without usage blocks", limit: 100, summaryErr: errors.New("summary endpoint unavailable"), wantSummaries: 1, wantBlocked: true, wantFailure: "summary endpoint unavailable"},
		{name: "budget error stops execution", limit: 100, summaryErr: durable.ErrUsageBudgetExceeded, wantSummaries: 1, wantBlocked: true, wantFailure: durable.ErrUsageBudgetExceeded.Error()},
		{name: "record failure stops execution", limit: 100, tokens: 5, recordFails: true, wantSummaries: 1, wantBlocked: true, wantFailure: context.Canceled.Error()},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID:     "owner",
				SessionID:   run.SessionID,
				RequestKey:  "summary-usage",
				RequestHash: "summary-usage",
				Objective:   "complete task",
				Evaluator:   durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
				Budget: durable.BudgetSpec{
					MaxTokens:  testCase.limit,
					MaxCostUSD: testCase.costLimit,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			claimed := getClaim(t, store)
			claimed.Spec.History = []durable.Message{
				{Role: "user", Content: "previous question"},
				{Role: "assistant", Content: "previous answer"},
			}
			pricedSpec(1, 1)(&claimed.Spec)
			if testCase.precharged > 0 {
				if err = store.RecordUsage(
					ctx,
					claimed.Lease(),
					0,
					"model",
					testCase.precharged,
					json.RawMessage(`{"version":3}`),
				); err != nil {
					t.Fatal(err)
				}
			}
			models, summaries := 0, 0
			executor := &engine.Engine{
				Store:              store,
				CompactionMaxBytes: 1,
				Summarizer: summaryFunc(
					func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
						summaries++
						if input.RecordUsage == nil {
							t.Error("summary usage callback missing")
							return "", errors.New("missing callback")
						}
						if testCase.tokens > 0 {
							summaryUsage := &schema.TokenUsage{
								PromptTokens:     int(testCase.tokens) - 1,
								CompletionTokens: 1,
								TotalTokens:      int(testCase.tokens),
							}
							if testCase.recordFails {
								canceled, cancel := context.WithCancel(summaryCtx)
								cancel()
								_ = input.RecordUsage(
									canceled,
									summaryUsage,
									testCase.tokens,
								)
								return "ignored recording error", nil
							}
							if usageErr := input.RecordUsage(
								summaryCtx,
								summaryUsage,
								testCase.tokens,
							); usageErr != nil {
								return "", usageErr
							}
						}
						return "earlier conversation summary", testCase.summaryErr
					},
				),
				Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					return &fakeModel{
						generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
							models++
							return &schema.Message{
								Role:    schema.Assistant,
								Content: "done",
								ResponseMeta: &schema.ResponseMeta{
									Usage: &schema.TokenUsage{},
								},
							}, nil
						},
					}, map[string]engine.Binding{
						"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}},
					}, nil
				},
			}
			if err = executor.Execute(ctx, claimed); err != nil {
				t.Fatal(err)
			}
			saved, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus := durable.Completed
			if testCase.wantFailure != "" {
				wantStatus = durable.Failed
			}
			if testCase.wantBlocked {
				wantStatus = durable.Blocked
			}
			if saved.Status != wantStatus || models != testCase.wantModels ||
				summaries != testCase.wantSummaries {
				t.Fatalf(
					"status=%s models=%d summaries=%d run=%+v",
					saved.Status,
					models,
					summaries,
					saved,
				)
			}
			var state engine.State
			if err = json.Unmarshal(saved.State, &state); err != nil {
				t.Fatal(err)
			}
			if state.SummaryCalls != testCase.wantSummaryCalls {
				t.Fatalf("summary calls=%d want=%d", state.SummaryCalls, testCase.wantSummaryCalls)
			}
			if testCase.wantBlocked {
				if state.InflightModel == nil || state.InflightModel.Kind != "summary" ||
					state.InflightModel.Sequence != 0 ||
					state.Turn != 0 ||
					saved.FirstKeptSeq != 0 ||
					saved.CompactedSummary != "" {
					t.Fatalf("unknown summary advanced execution: state=%+v run=%+v", state, saved)
				}
				if _, claimErr := store.Claim(
					ctx,
					"replacement",
					time.Minute,
				); !errors.Is(
					claimErr,
					durable.ErrNoWork,
				) {
					t.Fatalf("blocked summary was claimable: %v", claimErr)
				}
			}
			var usage []struct {
				Turn   int64
				Tokens int64
			}
			if err = store.DB().
				Table("durable_usage").
				Where("run_id = ? AND call = ?", run.ID, "summary").
				Find(&usage).
				Error; err != nil {
				t.Fatal(err)
			}
			if testCase.wantUsage == 0 {
				if len(usage) != 0 {
					t.Fatalf("unexpected summary usage: %+v", usage)
				}
			} else if len(usage) != 1 || usage[0].Turn != 0 || usage[0].Tokens != testCase.wantUsage {
				t.Fatalf("summary usage=%+v", usage)
			}
			savedGoal, err := store.GetGoal(ctx, "owner", goal.ID)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.wantBlocked && savedGoal.Status != durable.GoalBlocked {
				t.Fatalf("unknown summary did not block goal: %+v", savedGoal)
			}
			wantTokens := testCase.precharged + testCase.wantUsage
			if saved.TokensUsed != wantTokens || savedGoal.TokensUsed != wantTokens {
				t.Fatalf(
					"run tokens=%d goal tokens=%d want=%d",
					saved.TokensUsed,
					savedGoal.TokensUsed,
					wantTokens,
				)
			}
			if testCase.wantCost > 0 &&
				(saved.CostUSD < testCase.wantCost-1e-12 || saved.CostUSD > testCase.wantCost+1e-12 || savedGoal.CostUSD < testCase.wantCost-1e-12 || savedGoal.CostUSD > testCase.wantCost+1e-12) {
				t.Fatalf(
					"run cost=%v goal cost=%v want=%v",
					saved.CostUSD,
					savedGoal.CostUSD,
					testCase.wantCost,
				)
			}
			events, err := store.Events(ctx, "owner", run.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			failures, foundFailure := 0, false
			for _, event := range events {
				if event.Kind == "CompactionFailed" {
					failures++
				}
				if testCase.wantFailure != "" {
					data, marshalErr := json.Marshal(event)
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					if strings.Contains(string(data), testCase.wantFailure) {
						foundFailure = true
					}
				}
			}
			if failures != testCase.wantFailures || (testCase.wantFailure != "" && !foundFailure) {
				t.Fatalf(
					"compaction failures=%d expected=%d failure recorded=%v",
					failures,
					testCase.wantFailures,
					foundFailure,
				)
			}
		})
	}
}

func TestCompactionMergesPreviousSummaryAndPreservesModelFields(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimed.Spec.MaxTurns = 8
	data, err := json.Marshal(claimed.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DB().
		Exec("UPDATE durable_run SET spec = ? WHERE id = ?", string(data), run.ID).
		Error; err != nil {
		t.Fatal(err)
	}
	models, summaries := 0, 0
	summarizer := summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		previous := ""
		if summaries > 1 {
			previous = fmt.Sprintf("summary-%d", summaries-1)
		}
		if input.PreviousSummary != previous || len(input.Messages) != 2 ||
			input.Messages[0].ToolCalls[0].ID != fmt.Sprintf("turn_%d_tool_0", summaries) {
			return "", fmt.Errorf("incorrect incremental summary: %+v", input)
		}
		return fmt.Sprintf(
				"summary-%d",
				summaries,
			), input.RecordUsage(
				summaryCtx,
				&schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6},
				6,
			)
	})
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
			models++
			for _, message := range input {
				if message.Role == schema.Assistant && len(message.ToolCalls) > 0 &&
					(message.ReasoningContent != "provider reasoning" || message.ToolCalls[0].Extra["signature"] != "signed-call") {
					return nil, errors.New("projection discarded model metadata")
				}
			}
			if models <= 5 {
				message := toolReply("lookup")
				message.ReasoningContent = "provider reasoning"
				message.ToolCalls[0].Extra = map[string]any{"signature": "signed-call"}
				return message, nil
			}
			return schema.AssistantMessage("done", nil), nil
		}}, map[string]engine.Binding{"lookup": {
			Policy: durable.ReadOnly,
			Tool: &fakeTool{name: "lookup", invoke: func(context.Context, string) (string, error) {
				return "result", nil
			}},
		}}, nil
	}
	executor := &engine.Engine{Store: store, Build: build, Summarizer: summarizer, CompactionMaxBytes: 1}
	if err = executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.Status != durable.Completed || summaries != 4 ||
		saved.CompactedSummary != "summary-4" || saved.FirstKeptSeq != 10 {
		t.Fatalf("saved=%+v summaries=%d error=%v", saved, summaries, err)
	}
}
