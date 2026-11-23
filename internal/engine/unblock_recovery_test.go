package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	openai "github.com/meguminnnnnnnnn/go-openai"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestUnblockResumesWithoutRepeatingCompletedTools(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		kind       string
		summaryErr error
		tools      int
		tokens     int64
	}{
		{name: "model nil response with error", kind: "model", tools: 1, tokens: 2},
		{name: "summary error without usage", kind: "summary", summaryErr: errors.New("summary transport reset"), tools: 2, tokens: 9},
		{name: "summary success without usage", kind: "summary", tools: 2, tokens: 9},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, dsn := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: run.SessionID,
				RequestKey: "unblock-recovery", RequestHash: "unblock-recovery",
				Objective: "finish after human approval",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
				Budget:    durable.BudgetSpec{MaxTokens: 100},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			claim, err := store.Claim(ctx, "first", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			models, tools, summaries := 0, 0, 0
			resuming := false
			providerErr := errors.New("model transport reset")
			build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
				return &fakeModel{
					generate: func(_ context.Context, input []*schema.Message) (*schema.Message, error) {
						models++
						if err := checkToolPairs(input); err != nil {
							return nil, err
						}
						if !resuming {
							if models <= testCase.tools {
								return toolReply("lookup"), nil
							}
							return nil, providerErr
						}
						return schema.AssistantMessage("resumed after approval", nil), nil
					},
				}, map[string]engine.Binding{
					"lookup": {
						Policy: durable.ReadOnly,
						Tool: &fakeTool{
							name: "lookup",
							invoke: func(context.Context, string) (string, error) {
								tools++
								return "saved tool result", nil
							},
						},
					},
				}, nil
			}
			first := &engine.Engine{Store: store, Build: build}
			if testCase.kind == "summary" {
				first.CompactionMaxBytes = 1
				first.Summarizer = summaryFunc(
					func(context.Context, engine.SummaryInput) (string, error) {
						summaries++
						return "unaccounted summary", testCase.summaryErr
					},
				)
			}
			if err = first.Execute(ctx, claim); err != nil {
				t.Fatal(err)
			}
			blocked, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			var blockedState engine.State
			if err = json.Unmarshal(blocked.State, &blockedState); err != nil {
				t.Fatal(err)
			}
			if blocked.Status != durable.Blocked || blockedState.Version != 3 ||
				blockedState.InflightModel == nil || blockedState.InflightModel.Kind != testCase.kind ||
				len(blockedState.Checkpoint) == 0 || blockedState.Turn != testCase.tools ||
				blocked.TokensUsed != int64(testCase.tools) || tools != testCase.tools {
				t.Fatalf(
					"unexpected blocked execution: run=%+v state=%+v models=%d tools=%d",
					blocked,
					blockedState,
					models,
					tools,
				)
			}
			if !strings.Contains(blocked.Error, "unknown consumption") ||
				(testCase.kind == "model" && !strings.Contains(blocked.Error, providerErr.Error())) ||
				(testCase.summaryErr != nil && !strings.Contains(blocked.Error, testCase.summaryErr.Error())) {
				t.Fatalf("blocked execution lost its cause: %q", blocked.Error)
			}
			if _, err = store.Claim(
				ctx,
				"automatic-replay",
				time.Minute,
			); !errors.Is(
				err,
				durable.ErrNoWork,
			) {
				t.Fatalf("unknown dispatch was automatically retried: %v", err)
			}
			if err = store.Unblock(
				ctx,
				"owner",
				run.ID,
				blocked.Epoch,
				"Reviewed unknown consumption; authorize a fresh dispatch",
			); err != nil {
				t.Fatal(err)
			}
			queued, err := store.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			var queuedState engine.State
			if err = json.Unmarshal(queued.State, &queuedState); err != nil {
				t.Fatal(err)
			}
			if queued.Status != durable.Queued || queued.Epoch <= blocked.Epoch ||
				queuedState.InflightModel != nil ||
				queuedState.Turn != blockedState.Turn ||
				queuedState.SummaryCalls != blockedState.SummaryCalls ||
				string(queuedState.Checkpoint) != string(blockedState.Checkpoint) ||
				queued.TokensUsed != blocked.TokensUsed {
				t.Fatalf(
					"unblock changed execution progress: queued=%+v state=%+v",
					queued,
					queuedState,
				)
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
			resuming = true
			second := &engine.Engine{Store: reopened, Build: build}
			if testCase.kind == "summary" {
				second.CompactionMaxBytes = 1
				second.Summarizer = summaryFunc(
					func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
						summaries++
						return "confirmed earlier work", input.RecordUsage(
							summaryCtx,
							&schema.TokenUsage{
								PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6,
							},
							6,
						)
					},
				)
			}
			if err = second.Execute(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			done, err := reopened.Get(ctx, "owner", run.ID)
			if err != nil {
				t.Fatal(err)
			}
			var doneState engine.State
			if err = json.Unmarshal(done.State, &doneState); err != nil {
				t.Fatal(err)
			}
			wantModels, wantSummaries := 3, 0
			if testCase.kind == "summary" {
				wantModels, wantSummaries = 3, 2
			}
			if done.Status != durable.Completed || done.Output != "resumed after approval" ||
				doneState.InflightModel != nil || tools != testCase.tools || models != wantModels ||
				summaries != wantSummaries || done.TokensUsed != testCase.tokens {
				t.Fatalf(
					"recovery run=%+v models=%d tools=%d summaries=%d",
					done,
					models,
					tools,
					summaries,
				)
			}
			savedGoal, err := reopened.GetGoal(ctx, "owner", goal.ID)
			if err != nil {
				t.Fatal(err)
			}
			if savedGoal.Status != durable.GoalActive || savedGoal.TokensUsed != testCase.tokens {
				t.Fatalf("goal did not resume with accounted usage: %+v", savedGoal)
			}
			var completedTools int64
			if err = reopened.DB().
				Table("durable_step").
				Where("run_id = ? AND status = ?", run.ID, "Completed").
				Count(&completedTools).
				Error; err != nil {
				t.Fatal(err)
			}
			if completedTools != int64(testCase.tools) {
				t.Fatalf("completed tool records=%d want=%d", completedTools, testCase.tools)
			}
			events, err := reopened.Events(ctx, "owner", run.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			unblocked, retries := 0, 0
			for _, event := range events {
				switch event.Kind {
				case "RunUnblocked":
					unblocked++
				case "RunRetry":
					retries++
				}
			}
			if unblocked != 1 || retries != 0 {
				t.Fatalf("unblocked=%d retries=%d", unblocked, retries)
			}
		})
	}
}

func TestMainModelUnknownConsumptionStillRequiresUnblock(t *testing.T) {
	for _, test := range []struct {
		name string
		msg  *schema.Message
		err  error
	}{
		{name: "HTTP 502", err: &openai.APIError{HTTPStatusCode: 502, Code: "context_length_exceeded", Message: "context window"}},
		{name: "connection reset", err: syscall.ECONNRESET},
		{name: "missing usage", msg: schema.AssistantMessage("unaccounted response", nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			run := submit(t, store, "lookup", durable.ReadOnly)
			dispatches := 0
			worker := &engine.Engine{
				Store: store,
				Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					base := &usageModel{
						disableCallbacks: true,
						generate: func() (*schema.Message, error) {
							dispatches++
							if dispatches == 1 {
								return test.msg, test.err
							}
							msg := schema.AssistantMessage("after unblock", nil)
							msg.ResponseMeta = &schema.ResponseMeta{
								Usage: &schema.TokenUsage{
									PromptTokens:     12,
									CompletionTokens: 3,
								},
							}
							return msg, nil
						},
					}
					return base, map[string]engine.Binding{
						"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}},
					}, nil
				},
			}
			if err := worker.Execute(ctx, claimForCompression(t, store)); err != nil {
				t.Fatal(err)
			}
			blocked, state, events := compressionResult(t, store, run)
			if blocked.Status != durable.Blocked || state.InflightModel == nil ||
				state.InflightModel.Kind != "model" ||
				dispatches != 1 ||
				events.counts["ModelOverflowRejected"] > 0 {
				t.Fatalf(
					"status=%s error=%q inflight=%+v dispatches=%d",
					blocked.Status,
					blocked.Error,
					state.InflightModel,
					dispatches,
				)
			}
			if err := store.Unblock(
				ctx,
				run.OwnerID,
				run.ID,
				blocked.Epoch,
				"Reviewed unknown consumption",
			); err != nil {
				t.Fatal(err)
			}
			if err := worker.Execute(ctx, claimForCompression(t, store)); err != nil {
				t.Fatal(err)
			}
			done, state, _ := compressionResult(t, store, run)
			if done.Status != durable.Completed || state.InflightModel != nil || dispatches != 2 ||
				done.TokensUsed != 15 {
				t.Fatalf(
					"status=%s error=%q inflight=%+v dispatches=%d",
					done.Status,
					done.Error,
					state.InflightModel,
					dispatches,
				)
			}
		})
	}
}
