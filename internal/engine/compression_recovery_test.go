package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	openai "github.com/meguminnnnnnnnn/go-openai"
	"gorm.io/gorm"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestHTTPContextOverflowRecovery(t *testing.T) {
	for _, test := range []struct {
		name    string
		persist bool
		status  durable.Status
	}{
		{name: "recovers", status: durable.Completed},
		{name: "exhausted at floor", persist: true, status: durable.Failed},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx := context.Background()
			var requestSizes []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct{ Messages json.RawMessage }
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				requestSizes = append(requestSizes, len(req.Messages))
				if len(requestSizes) == 1 || test.persist {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(
						w,
						`{"error":{"code":"context_length_exceeded","message":"Maximum context length exceeded"}}`,
					)
					return
				}
				startModelStream(w)
				writeModelChunk(w, `{"role":"assistant","content":"recovered"}`)
				finishModelStream(
					w,
					"stop",
					`{"prompt_tokens":20,"completion_tokens":4,"total_tokens":24}`,
				)
			}))
			defer server.Close()
			run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
				spec.Model.BaseURL = server.URL
				spec.History = []durable.Message{
					{Role: "user", Content: "previous question"},
					{Role: "assistant", Content: strings.Repeat("previous evidence marker ", 1000)},
				}
			})
			var publishAttempts int
			publisher := &capturedDeltas{}
			worker := &engine.Engine{
				Store: store, Build: adapters.Build,
				NewDeltaPublisher: func(context.Context) engine.DeltaPublisher {
					publishAttempts++
					return publisher
				},
			}
			if err := worker.Execute(ctx, claimForCompression(t, store)); err != nil {
				t.Fatal(err)
			}
			done, state, events := compressionResult(t, store, run)
			if done.Status != test.status || state.InflightModel != nil || len(requestSizes) != 2 ||
				requestSizes[1] >= requestSizes[0] {
				t.Fatalf(
					"status=%s error=%q inflight=%+v request sizes=%v",
					done.Status,
					done.Error,
					state.InflightModel,
					requestSizes,
				)
			}
			rejections := 1
			if test.persist {
				rejections++
				if !strings.Contains(done.Error, engine.ErrContextCompressionExhausted.Error()) ||
					done.TokensUsed != 0 {
					t.Fatalf("unaccounted failure: %q tokens=%d", done.Error, done.TokensUsed)
				}
			} else if done.Output != "recovered" || done.TokensUsed != 24 ||
				state.UsageAnchor == nil || state.UsageAnchor.PromptTokens != 20 || state.UsageAnchor.EstimatedInputTokens <= 0 {
				t.Fatalf(
					"unexpected recovery output=%q tokens=%d anchor=%+v",
					done.Output,
					done.TokensUsed,
					state.UsageAnchor,
				)
			}
			if events.counts["ModelOverflowRejected"] != rejections || events.counts["RunBlocked"] != 0 ||
				len(events.contexts) != 2 || events.contexts[1].ReactiveAttempt != 1 {
				t.Fatalf("recovery events=%v contexts=%+v", events.counts, events.contexts)
			}
			if test.persist {
				if publishAttempts != 0 || len(publisher.deltas) != 0 {
					t.Fatalf(
						"rejected requests opened a publisher: attempts=%d deltas=%+v",
						publishAttempts,
						publisher.deltas,
					)
				}
			} else if publishAttempts != 1 || len(publisher.deltas) != 1 ||
				publisher.deltas[0].Delta != "recovered" || publisher.deltas[0].Offset != 0 ||
				publisher.deltas[0].Turn != 0 || publisher.deltas[0].Kind != durable.EventModelContentDelta {
				t.Fatalf(
					"unexpected recovery deltas: attempts=%d deltas=%+v",
					publishAttempts,
					publisher.deltas,
				)
			}
		})
	}
}

func TestStreamingContextOverflowRecovery(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
		spec.History = []durable.Message{
			{Role: "user", Content: "previous question"},
			{Role: "assistant", Content: strings.Repeat("previous evidence marker ", 1000)},
		}
	})
	calls := 0
	base := &usageModel{disableCallbacks: true, stream: func() (*schema.StreamReader[*schema.Message], error) {
		calls++
		if calls == 1 {
			return nil, errors.New("HTTP 400 Bad Request: maximum context length exceeded")
		}
		msg := schema.AssistantMessage("recovered", nil)
		msg.ResponseMeta = &schema.ResponseMeta{
			Usage: &schema.TokenUsage{PromptTokens: 20, CompletionTokens: 2},
		}
		return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
	}}
	boundary := engine.ModelBoundaryForTest(t, store, claimForCompression(t, store), base)
	msg, err := invokeUsageModel(ctx, boundary, true)
	if err != nil || msg.Content != "recovered" || calls != 2 {
		t.Fatalf("msg=%+v calls=%d err=%v", msg, calls, err)
	}
	done, state, events := compressionResult(t, store, run)
	if done.TokensUsed != 22 || state.InflightModel != nil || events.counts["ModelOverflowRejected"] != 1 {
		t.Fatalf("usage=%d inflight=%+v events=%v", done.TokensUsed, state.InflightModel, events.counts)
	}
}

func TestRollingCompactionPreservesInputsAndPromotesSession(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	cfg := durable.AgentConfig{
		Model: durable.ModelSpec{Name: "test", BaseURL: "http://localhost/v1"}, MaxTurns: 8,
		Tools: []durable.ToolSpec{
			{
				Name:       "ask_for_clarification",
				Policy:     durable.ReadOnly,
				URL:        "http://localhost/tool",
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
			{
				Name:       "lookup",
				Policy:     durable.ReadOnly,
				URL:        "http://localhost/tool",
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}
	const query, answer, steer = "keep original task", "clarified scope: only the blue region", "prefer existing resources"
	session, run := submitChatFixture(t, store, "owner", "rolling", cfg, query, time.Hour, 4)
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID: "owner", SessionID: session.ID, RequestKey: "rolling-goal", RequestHash: "rolling-goal",
		Objective: query, Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	budget := contextbuilder.Budget{MaxInputBytes: 12000, MaxToolResultBytes: 7000}
	original := strings.Repeat("完整证据 data ", 1700)
	modelCalls, summaryCalls := 0, 0
	summary := ""
	rejectedChunkBytes, retryChunkBytes := 0, 0
	worker := &engine.Engine{
		Store: store,
		Summarizer: summaryFunc(func(summaryCtx context.Context, request engine.SummaryInput) (string, error) {
			if request.PreviousSummary != summary || len(request.Messages) == 0 {
				return "", fmt.Errorf("unlinked summary chunk %d", summaryCalls+1)
			}
			payload, err := json.Marshal(request.Messages)
			if err != nil {
				return "", err
			}
			if len(payload) > 5000 && rejectedChunkBytes == 0 {
				rejectedChunkBytes = len(payload)
				return "", &openai.APIError{HTTPStatusCode: 400, Code: "context_length_exceeded"}
			}
			if rejectedChunkBytes > 0 && retryChunkBytes == 0 {
				retryChunkBytes = len(payload)
			}
			if err := chargeSummary(summaryCtx, request); err != nil {
				return "", err
			}
			summaryCalls++
			summary = fmt.Sprintf("rolling summary %d", summaryCalls)
			return summary, nil
		}),
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			base := &fakeModel{
				generate: func(callCtx context.Context, messages []*schema.Message) (*schema.Message, error) {
					modelCalls++
					if err := checkToolPairs(messages); err != nil {
						return nil, err
					}
					switch modelCalls {
					case 1:
						return toolReply("ask_for_clarification"), nil
					case 2:
						if err := store.SteerGoal(
							callCtx,
							"owner",
							goal.ID,
							steer,
						); err != nil {
							return nil, err
						}
						return toolReply("lookup"), nil
					case 3, 4:
						return toolReply("lookup"), nil
					default:
						for _, text := range []string{query, answer, steer, summary} {
							if text == "" || !containsMessage(messages, text) {
								return nil, fmt.Errorf(
									"missing projected input %q",
									text,
								)
							}
						}
						return schema.AssistantMessage("finished", nil), nil
					}
				},
			}
			return base, map[string]engine.Binding{
				"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "lookup",
						invoke: func(context.Context, string) (string, error) {
							return original, nil
						},
					},
				},
				"ask_for_clarification": {
					Policy: durable.ReadOnly,
					Tool:   &fakeTool{name: "ask_for_clarification"},
				},
			}, nil
		},
	}
	claim := claimForCompression(t, store)
	claim.ContextSnapshot = compressionSnapshot(t, budget)
	if err := worker.Execute(ctx, claim); err != nil {
		t.Fatal(err)
	}
	waiting, err := store.Get(ctx, "owner", run.ID)
	if err != nil || waiting.Status != durable.WaitingClarification {
		t.Fatalf("expected question, status=%s err=%v", waiting.Status, err)
	}
	if err := store.SupplyResult(
		ctx,
		"owner",
		run.ID,
		waiting.WaitingStep,
		answer,
		durable.InteractionClarification,
	); err != nil {
		t.Fatal(err)
	}
	claim = claimForCompression(t, store)
	claim.ContextSnapshot = compressionSnapshot(t, budget)
	if err := worker.Execute(ctx, claim); err != nil {
		t.Fatal(err)
	}
	done, state, events := compressionResult(t, store, run)
	if done.Status != durable.Completed || summaryCalls < 3 || state.SummaryCalls != int64(summaryCalls) ||
		done.CompactedSummary != summary || !strings.Contains(string(state.Checkpoint), original) {
		t.Fatalf(
			"status=%s error=%q summary calls=%d, persisted=%d",
			done.Status,
			done.Error,
			summaryCalls,
			state.SummaryCalls,
		)
	}
	var raw string
	if err := store.DB().
		Table("durable_step").
		Select("result").
		Where("run_id = ? AND step_id = ?", run.ID, "turn_2_tool_0").
		Row().
		Scan(&raw); err != nil ||
		raw != original {
		t.Fatalf("completed step lost original result: bytes=%d error=%v", len(raw), err)
	}
	if events.counts["RunCompacted"] < 1 || events.counts["ModelOverflowRejected"] != 1 ||
		retryChunkBytes <= 0 || retryChunkBytes >= rejectedChunkBytes {
		t.Fatalf(
			"summary did not retry with a smaller chunk: events=%v bytes=%d->%d",
			events.counts,
			rejectedChunkBytes,
			retryChunkBytes,
		)
	}
	next, _, err := store.Accept(ctx, execution.SubmitCommand{
		Owner: "owner", RequestKey: "next", RequestHash: "next", TTL: time.Hour, MaxAttempts: 4,
		Kind: execution.SubmitChat, SessionID: session.ID, Query: "continue",
	}, func(ctx context.Context, tx *gorm.DB, facts execution.SubmissionFacts) (execution.PreparedSubmission, error) {
		return durable.PrepareChatSubmission(cfg, session.AgentID, 1, nil)(ctx, tx, facts)
	})
	if err != nil {
		t.Fatal(err)
	}
	frozen := next.ContextSnapshot
	snapshot, err := contextbuilder.DecodeSnapshot(contextbuilder.Envelope{
		Format:        frozen.Format,
		PolicyVersion: frozen.PolicyVersion,
		Digest:        frozen.Digest,
		Payload:       frozen.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	history := snapshot.Materials.History
	if snapshot.Materials.BaseSummary != summary || len(history) != 1 || history[0].Source.ID != run.ID ||
		contextbuilder.ValidateGroup(
			history[0].Messages,
		) != nil || history[0].Messages[len(history[0].Messages)-1].Content != "finished" {
		t.Fatalf(
			"next run did not freeze summary and paired tail: summary=%q groups=%d",
			snapshot.Materials.BaseSummary,
			len(history),
		)
	}
}

func TestSummaryBreakerSurvivesTakeoverAndDropsRounds(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) { spec.MaxTurns = 8 })
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	modelCalls, toolCalls, summaryCalls := 0, 0, 0
	worker := &engine.Engine{
		Store:              store,
		CompactionMaxBytes: 1,
		Summarizer: summaryFunc(func(summaryCtx context.Context, request engine.SummaryInput) (string, error) {
			summaryCalls++
			if err := chargeSummary(summaryCtx, request); err != nil {
				return "", err
			}
			return "", errors.New("summary lacks confirmed facts")
		}),
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					modelCalls++
					if modelCalls <= 5 {
						return toolReply("lookup"), nil
					}
					if !containsMessage(messages, "do work") ||
						containsMessage(messages, "evidence-1") {
						return nil, errors.New(
							"round fallback lost the query or included an expired round",
						)
					}
					return schema.AssistantMessage("finished with fallback", nil), nil
				},
			}, map[string]engine.Binding{
				"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "lookup",
						invoke: func(context.Context, string) (string, error) {
							toolCalls++
							if toolCalls == 5 {
								stop()
								return "", context.Canceled
							}
							return fmt.Sprintf("evidence-%d", toolCalls), nil
						},
					},
				},
			}, nil
		},
	}
	first := claimForCompression(t, store)
	if err := worker.Execute(workerCtx, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected interrupted worker, got %v", err)
	}
	_, state, _ := compressionResult(t, store, run)
	if state.SummaryConsecutiveFailures != 3 || state.InflightModel != nil {
		t.Fatalf(
			"breaker was not saved before takeover: failures=%d inflight=%+v",
			state.SummaryConsecutiveFailures,
			state.InflightModel,
		)
	}
	if err := store.DB().
		Exec("UPDATE durable_run SET lease_until = clock_timestamp() WHERE id = ?", run.ID).
		Error; err != nil {
		t.Fatal(err)
	}
	replacement := claimForCompression(t, store)
	if replacement.Epoch <= first.Epoch {
		t.Fatalf("takeover epoch=%d first=%d", replacement.Epoch, first.Epoch)
	}
	if err := worker.Execute(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	done, state, events := compressionResult(t, store, run)
	last := events.contexts[len(events.contexts)-1]
	if done.Status != durable.Completed || summaryCalls != 3 || state.SummaryConsecutiveFailures != 3 ||
		events.counts["CompactionFailed"] != 3 || !last.SummaryBreakerOpen || last.DroppedRounds == 0 {
		t.Fatalf(
			"status=%s error=%q calls=%d failures=%d events=%v last=%+v",
			done.Status,
			done.Error,
			summaryCalls,
			state.SummaryConsecutiveFailures,
			events.counts,
			last,
		)
	}
}

func TestReadStepResultPaginatesStoredUTF8(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) { spec.MaxTurns = 8 })
	original := "A界B" + strings.Repeat("界", 9000)
	calls := 0
	worker := &engine.Engine{
		Store: store,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					calls++
					request := func(args string) *schema.Message {
						res := toolReply("read_step_result")
						res.ToolCalls[0].Function.Arguments = args
						return res
					}
					switch calls {
					case 1:
						return toolReply("lookup"), nil
					case 2:
						return request(`{"step_id":"turn_1_tool_0","offset":1,"limit":4}`), nil
					}
					var page struct {
						StepID    string `json:"step_id"`
						Offset    int    `json:"offset"`
						Next      int    `json:"next_offset"`
						Total     int    `json:"total_bytes"`
						Truncated bool   `json:"truncated"`
						Content   string `json:"content"`
					}
					if err := json.Unmarshal(
						[]byte(messages[len(messages)-1].Content),
						&page,
					); err != nil {
						return nil, err
					}
					if page.StepID != "turn_1_tool_0" || page.Total != len(original) ||
						!utf8.ValidString(page.Content) ||
						page.Content != original[page.Offset:page.Next] ||
						page.Truncated != (page.Next < page.Total) {
						return nil, fmt.Errorf(
							"bad result page: offset=%d next=%d total=%d",
							page.Offset,
							page.Next,
							page.Total,
						)
					}
					switch calls {
					case 3:
						if page.Offset != 1 || page.Next != 5 || page.Content != "界B" {
							return nil, errors.New("first result page does not align")
						}
						return request(
							fmt.Sprintf(
								`{"step_id":"turn_1_tool_0","offset":%d}`,
								page.Next,
							),
						), nil
					case 4:
						if page.Offset != 5 || len(page.Content) != 8190 {
							return nil, errors.New(
								"default page limit must respect rune boundary",
							)
						}
						return request(`{"step_id":"turn_1_tool_0","offset":2,"limit":1}`), nil
					case 5:
						if page.Offset != 4 || page.Content != "B" {
							return nil, errors.New("offset inside a rune was not advanced")
						}
						return request(
							`{"step_id":"turn_1_tool_0","offset":5,"limit":999999}`,
						), nil
					case 6:
						if page.Offset != 5 || len(page.Content) != 16383 {
							return nil, errors.New(
								"maximum page limit must respect rune boundary",
							)
						}
						return request(`{"step_id":"turn_1_tool_0","offset":999999}`), nil
					default:
						if page.Next != len(original) || page.Content != "" {
							return nil, errors.New(
								"offset beyond the result should return an empty page",
							)
						}
						return schema.AssistantMessage("read complete", nil), nil
					}
				},
			}, map[string]engine.Binding{
				"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "lookup",
						invoke: func(context.Context, string) (string, error) {
							return original, nil
						},
					},
				},
			}, nil
		},
	}
	if err := worker.Execute(ctx, claimForCompression(t, store)); err != nil {
		t.Fatal(err)
	}
	done, _, _ := compressionResult(t, store, run)
	if done.Status != durable.Completed || calls != 7 {
		t.Fatalf("status=%s error=%q calls=%d", done.Status, done.Error, calls)
	}
}

func chargeSummary(ctx context.Context, request engine.SummaryInput) error {
	return request.RecordUsage(ctx, &schema.TokenUsage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6}, 6)
}

func containsMessage(messages []*schema.Message, content string) bool {
	for _, message := range messages {
		if strings.Contains(message.Content, content) {
			return true
		}
	}
	return false
}

func claimForCompression(t *testing.T, store *durable.Store) *durable.Run {
	t.Helper()
	claim, err := store.Claim(context.Background(), "worker-"+durable.NewID(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func compressionSnapshot(t *testing.T, budget contextbuilder.Budget) durable.ContextSnapshot {
	t.Helper()
	env, err := contextbuilder.EncodeSnapshot(contextbuilder.Snapshot{
		MemoryMode: contextbuilder.MemoryOff,
		Policy:     contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4}, Budget: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	return durable.ContextSnapshot{
		Format:        env.Format,
		PolicyVersion: env.PolicyVersion,
		Digest:        env.Digest,
		Payload:       env.Payload,
	}
}

type compressionEvents struct {
	counts   map[string]int
	contexts []durable.ContextInfo
}

func compressionResult(
	t *testing.T,
	store *durable.Store,
	run *durable.Run,
) (*durable.Run, engine.State, compressionEvents) {
	t.Helper()
	ctx := context.Background()
	done, err := store.Get(ctx, run.OwnerID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var state engine.State
	if err := json.Unmarshal(done.State, &state); err != nil {
		t.Fatal(err)
	}
	events, err := store.Events(ctx, run.OwnerID, run.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := compressionEvents{counts: map[string]int{}}
	for _, event := range events {
		result.counts[event.Kind]++
		if event.Kind == "ModelStarted" {
			var payload struct{ Context durable.ContextInfo }
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			result.contexts = append(result.contexts, payload.Context)
		}
	}
	return done, state, result
}
