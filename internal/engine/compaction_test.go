package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type summaryFunc func(context.Context, engine.SummaryInput) (string, error)

func (summarize summaryFunc) Summarize(ctx context.Context, input engine.SummaryInput) (string, error) {
	return summarize(ctx, input)
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

func TestCompactionProjectionSurvivesTakeover(t *testing.T) {
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
	summarizer := summaryFunc(func(_ context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		if input.PreviousSummary != "" || len(input.Messages) != 3 ||
			input.Messages[0].ToolCalls[0].ID != "turn_1_tool_0" {
			return "", fmt.Errorf("unexpected summary prefix: %+v", input)
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
					if message.Content == "do work" && message.Role == schema.User {
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
				if summaryCount != 1 || resultCount != 4 || queryCount != 1 {
					return nil, fmt.Errorf(
						"model input summary=%d tools=%d query=%d",
						summaryCount,
						resultCount,
						queryCount,
					)
				}
				if modelCalls == 4 {
					cancel()
					return nil, context.Canceled
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
	if err = first.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("first execution: %v", err)
	}
	saved, err := store.Get(ctx, "owner", run.ID)
	if err != nil || saved.CompactedSummary == "" || saved.FirstKeptSeq != 5 {
		t.Fatalf("saved projection = %+v, %v", saved, err)
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
	replacement, err := reopened.Claim(ctx, "replacement", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second := &engine.Engine{Store: reopened, Build: build}
	if err = second.Execute(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	done, err := reopened.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "recovered with summary" ||
		toolCalls != 6 || summaries != 1 || modelCalls != 5 {
		t.Fatalf(
			"done=%+v toolCalls=%d summaries=%d modelCalls=%d error=%v",
			done,
			toolCalls,
			summaries,
			modelCalls,
			err,
		)
	}
	audit, err := reopened.Messages(ctx, "owner", run.SessionID, 0, 200)
	if err != nil || len(audit) != 11 || audit[1].ToolCalls[0].ID != "turn_1_tool_0" {
		t.Fatalf("audit lost compacted messages: count=%d error=%v", len(audit), err)
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
	summarizer := summaryFunc(func(_ context.Context, input engine.SummaryInput) (string, error) {
		summaries++
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
			if models == 4 {
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
				if !found {
					return nil, errors.New("failed summarization discarded old messages")
				}
			}
			if models <= 4 {
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
	summarizer := summaryFunc(func(_ context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		if len(input.Messages) != 2 || input.Messages[0].Content != "old question" {
			return "", errors.New("history was not included in summary")
		}
		return "Previous conversation summary", nil
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
	summarizer := summaryFunc(func(_ context.Context, input engine.SummaryInput) (string, error) {
		summaries++
		previous := ""
		if summaries > 1 {
			previous = fmt.Sprintf("summary-%d", summaries-1)
		}
		if input.PreviousSummary != previous || len(input.Messages) != 2 ||
			input.Messages[0].ToolCalls[0].ID != fmt.Sprintf("turn_%d_tool_0", summaries) {
			return "", fmt.Errorf("incorrect incremental summary: %+v", input)
		}
		return fmt.Sprintf("summary-%d", summaries), nil
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
	if err != nil || saved.Status != durable.Completed || summaries != 3 ||
		saved.CompactedSummary != "summary-3" || saved.FirstKeptSeq != 8 {
		t.Fatalf("saved=%+v summaries=%d error=%v", saved, summaries, err)
	}
}
