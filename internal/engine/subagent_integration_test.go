package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const (
	delegateTool     = durable.DelegateSubAgentToolName
	specialistModel  = "specialist"
	specialistPrompt = "Specialist prompt {{literal}}"
)

type subAgentModelMessage struct {
	Role      string            `json:"role"`
	Content   string            `json:"content"`
	ToolCalls []schema.ToolCall `json:"tool_calls,omitempty"`
}

type subAgentModelRequest struct {
	Model    string                 `json:"model"`
	Messages []subAgentModelMessage `json:"messages"`
	Tools    []struct {
		Function struct {
			Name        string `json:"name"`
			Description string `json:"description"`
			Parameters  struct {
				Properties map[string]json.RawMessage `json:"properties"`
				Required   []string                   `json:"required"`
			} `json:"parameters"`
		} `json:"function"`
	} `json:"tools"`
}

func (request subAgentModelRequest) toolResults() []string {
	var results []string
	for _, message := range request.Messages {
		if message.Role == "tool" {
			results = append(results, message.Content)
		}
	}
	return results
}

func (request subAgentModelRequest) hasTool(name string) bool {
	for _, tool := range request.Tools {
		if tool.Function.Name == name {
			return true
		}
	}
	return false
}

type subAgentModelReply struct {
	content      string
	calls        []schema.ToolCall
	finishReason string
	noUsage      bool
}

func subAgentReplyCall(name, args string) schema.ToolCall {
	return schema.ToolCall{
		ID: "upstream-call", Type: "function",
		Function: schema.FunctionCall{Name: name, Arguments: args},
	}
}

func subAgentDelegate(args string) subAgentModelReply {
	return subAgentModelReply{calls: []schema.ToolCall{subAgentReplyCall(delegateTool, args)}}
}

func subAgentModelServer(
	t *testing.T,
	reply func(int, subAgentModelRequest) subAgentModelReply,
) (string, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request subAgentModelRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode model request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		result := reply(int(calls.Add(1)), request)
		if result.finishReason == "" {
			result.finishReason = "stop"
			if len(result.calls) > 0 {
				result.finishReason = "tool_calls"
			}
		}
		message := map[string]any{"role": "assistant", "content": result.content}
		if len(result.calls) > 0 {
			message["tool_calls"] = result.calls
		}
		response := map[string]any{
			"id":     "completion",
			"object": "chat.completion",
			"model":  request.Model,
			"choices": []map[string]any{
				{"index": 0, "finish_reason": result.finishReason, "message": message},
			},
		}
		if !result.noUsage {
			response["usage"] = json.RawMessage(modelStreamUsage)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, &calls
}

func subAgentModelSpec(name, url string) durable.ModelSpec {
	return durable.ModelSpec{Name: name, BaseURL: url, Stream: new(false)}
}

func subAgentSpecialist(t *testing.T, store *durable.Store, url string) durable.SubAgentSnapshot {
	t.Helper()
	config := durable.AgentConfig{
		Model: subAgentModelSpec(specialistModel, url), SystemPrompt: specialistPrompt, MaxTurns: 5,
	}
	id, _ := createPublishedAgentFixture(t, store, "owner", "review specialist", config)
	return durable.SubAgentSnapshot{
		Name: "review", Description: "Review the proposed change", AgentID: id, ConfigVersion: "1",
		Model: config.Model, SystemPrompt: config.SystemPrompt, MaxTurns: config.MaxTurns, PricingFrozen: true,
		PricingSnapshot: &execution.PricingSnapshot{
			Version: "specialist-rate", Model: config.Model.Name, BaseURL: config.Model.BaseURL,
			InputPerMillion: 5, OutputPerMillion: 7,
		},
	}
}

func claimSubAgentRun(t *testing.T, store *durable.Store, id string) *durable.Run {
	t.Helper()
	run, err := store.Claim(t.Context(), durable.NewID(), time.Minute)
	if err != nil || run.ID != id {
		t.Fatalf("claim %s: run=%+v err=%v", id, run, err)
	}
	return run
}

func loadSubAgentRun(t *testing.T, store *durable.Store, id string) *durable.Run {
	t.Helper()
	run, err := store.Get(t.Context(), "owner", id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func executeSubAgentRun(t *testing.T, worker *engine.Engine, claim *durable.Run) *durable.Run {
	t.Helper()
	if err := worker.Execute(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	return loadSubAgentRun(t, worker.Store, claim.ID)
}

func waitingChild(t *testing.T, store *durable.Store, parent *durable.Run) *durable.Run {
	t.Helper()
	childID, found := strings.CutPrefix(parent.WaitingExternalRef, "child:")
	var state engine.State
	err := json.Unmarshal(parent.State, &state)
	if parent.Status != durable.WaitingExternal || !found || childID == "" || parent.WorkerID != "" ||
		parent.LeaseUntil != nil || err != nil || len(state.Checkpoint) == 0 || state.Final != nil {
		t.Fatalf(
			"invalid suspended parent: status=%s step=%q ref=%q worker=%q checkpoint=%d final=%v err=%v",
			parent.Status,
			parent.WaitingStep,
			parent.WaitingExternalRef,
			parent.WorkerID,
			len(state.Checkpoint),
			state.Final,
			err,
		)
	}
	child := loadSubAgentRun(t, store, childID)
	if child.Status != durable.Queued || child.ParentRunID != parent.ID ||
		child.ParentStepID != parent.WaitingStep ||
		child.SessionID == parent.SessionID ||
		child.GoalID != parent.GoalID {
		t.Fatalf("invalid child: %+v", child)
	}
	return child
}

func countSubAgentChildren(t *testing.T, store *durable.Store, parent string, want int64) {
	t.Helper()
	var count int64
	err := store.DB().Table("durable_run").Where("parent_run_id = ?", parent).Count(&count).Error
	if err != nil || count != want {
		t.Fatalf("children of %s: got %d, want %d, err=%v", parent, count, want, err)
	}
}

func expectSubAgentResults(t *testing.T, request subAgentModelRequest, want ...string) {
	t.Helper()
	if results := request.toolResults(); !reflect.DeepEqual(results, want) {
		t.Errorf("tool results=%q, want %q", results, want)
	}
}

func TestSubAgentFrozenModesEndToEnd(t *testing.T) {
	for _, self := range []bool{false, true} {
		t.Run(map[bool]string{false: "configured", true: "self"}[self], func(t *testing.T) {
			store, _ := testDB(t)
			const parentPrompt = "Parent prompt {{literal}}\nDo the work."
			const childTask = "Review a small change"
			const instructions = "Be a careful reviewer."
			const clarifyTool = "ask_for_clarification"
			url, requests := subAgentModelServer(
				t,
				func(call int, request subAgentModelRequest) subAgentModelReply {
					switch {
					case call == 1:
						if request.Model != "parent" ||
							!request.hasTool(durable.ReadStepResultToolName) {
							t.Errorf("parent model or result reader missing: %+v", request)
						}
						for _, tool := range request.Tools {
							if tool.Function.Name != delegateTool {
								continue
							}
							params := tool.Function.Parameters
							if self {
								if !reflect.DeepEqual(
									params.Required,
									[]string{"task"},
								) ||
									params.Properties["instruction"] == nil ||
									params.Properties["subagent"] != nil {
									t.Errorf("self tool schema: %+v", params)
								}
								return subAgentDelegate(
									`{"task":"` + childTask + `","instruction":"` + instructions + `"}`,
								)
							}
							if !strings.Contains(
								tool.Function.Description,
								"review: Review the proposed change",
							) ||
								!reflect.DeepEqual(
									params.Required,
									[]string{"subagent", "task"},
								) ||
								!strings.Contains(
									string(params.Properties["subagent"]),
									`"enum":["review"]`,
								) ||
								params.Properties["instruction"] != nil {
								t.Errorf("configured tool schema: %+v", tool.Function)
							}
							return subAgentDelegate(
								`{"subagent":"review","task":"` + childTask + `","timeout_seconds":60}`,
							)
						}
						t.Error("parent did not receive the delegate tool")
					case call == 2:
						wantModel, wantPrompt, wantQuery := specialistModel, specialistPrompt, childTask
						if self {
							wantModel, wantPrompt = "parent", parentPrompt
							wantQuery = "[Subtask Instructions]\n" + instructions + "\n\n[Task]\n" + childTask
						}
						want := []subAgentModelMessage{
							{Role: "system", Content: wantPrompt},
							{Role: "user", Content: wantQuery},
						}
						if request.Model != wantModel ||
							!reflect.DeepEqual(request.Messages, want) ||
							request.hasTool(delegateTool) {
							t.Errorf(
								"child model=%q messages=%+v delegate=%t, want %q and %+v",
								request.Model,
								request.Messages,
								request.hasTool(delegateTool),
								wantModel,
								want,
							)
						}
						if self {
							if !request.hasTool(clarifyTool) {
								t.Error(
									"child did not inherit clarification definition",
								)
							}
							return subAgentModelReply{
								calls: []schema.ToolCall{
									subAgentReplyCall(
										clarifyTool,
										`{"question":"What context is missing?"}`,
									),
								},
							}
						}
						return subAgentModelReply{content: "child finding"}
					case self && call == 3:
						results := request.toolResults()
						if len(results) != 1 || !json.Valid([]byte(results[0])) ||
							!strings.Contains(results[0], "CLARIFICATION_UNAVAILABLE") {
							t.Errorf("clarification feedback=%q", results)
						}
						return subAgentModelReply{content: "child finding"}
					default:
						expectSubAgentResults(t, request, "child finding")
						return subAgentModelReply{content: "parent used child finding"}
					}
					return subAgentModelReply{content: "missing tool"}
				},
			)
			config := durable.AgentConfig{
				Model:        subAgentModelSpec("parent", url),
				SystemPrompt: parentPrompt,
				MaxTurns:     6,
				SelfDelegate: self,
			}
			var specialist durable.SubAgentSnapshot
			if self {
				config.Tools = []durable.ToolSpec{
					{
						Name:       clarifyTool,
						Policy:     durable.ReadOnly,
						URL:        url + "/unexpected-clarification",
						Parameters: json.RawMessage(`{"type":"object"}`),
					},
				}
			} else {
				specialist = subAgentSpecialist(t, store, url)
			}
			_, root := submitChatFixture(
				t,
				store,
				"owner",
				"delegating root",
				config,
				"parent request",
				time.Hour,
				4,
				pricedSpec(1, 2),
				func(spec *durable.Spec) {
					spec.History = []durable.Message{
						{Role: "user", Content: "private parent history"},
					}
					if !self {
						spec.SubAgents = []durable.SubAgentSnapshot{specialist}
					}
				},
			)
			var builtChild bool
			worker := &engine.Engine{
				Store: store,
				Build: func(ctx context.Context, store *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					rc, ok := engine.BuildRunContext(ctx)
					if !ok || (rc.RunID == root.ID && rc.ParentRunID != "") ||
						(rc.RunID != root.ID && rc.ParentRunID != root.ID) {
						t.Errorf("build context=%+v available=%t", rc, ok)
					}
					builtChild = builtChild || rc.ParentRunID == root.ID
					return adapters.Build(ctx, store, spec)
				},
			}
			start := time.Now()
			child := waitingChild(
				t,
				store,
				executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)),
			)
			ttl := 60 * time.Second
			if self {
				ttl = 10 * time.Minute
			}
			if len(child.Spec.History) != 0 || child.Deadline.Before(start.Add(ttl-time.Second)) ||
				child.Deadline.After(time.Now().Add(ttl+time.Second)) {
				t.Fatalf(
					"child history=%v or deadline=%v, ttl=%s",
					child.Spec.History,
					child.Deadline,
					ttl,
				)
			}
			if !self && (child.AgentID != specialist.AgentID ||
				!reflect.DeepEqual(child.Spec.PricingSnapshot, specialist.PricingSnapshot)) {
				t.Fatalf("specialist identity or pricing changed: %+v", child.Spec)
			}
			completedChild := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
			wantChildTokens, wantChildCost, wantRequests := int64(3), 17e-6, int32(3)
			if self {
				wantChildTokens, wantChildCost, wantRequests = 6, 8e-6, 4
				result, _ := integrationMCPStep(t, store, child.ID, "turn_1_tool_0")
				if !strings.Contains(result, "CLARIFICATION_UNAVAILABLE") {
					t.Errorf("persisted clarification feedback=%q", result)
				}
			}
			if !builtChild || completedChild.Status != durable.Completed ||
				completedChild.Output != "child finding" ||
				completedChild.TokensUsed != wantChildTokens ||
				math.Abs(completedChild.CostUSD-wantChildCost) > 1e-12 {
				t.Fatalf("child completed=%t status=%s output=%q error=%q tokens=%d cost=%v",
					builtChild, completedChild.Status, completedChild.Output, completedChild.Error,
					completedChild.TokensUsed, completedChild.CostUSD)
			}
			parent := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
			if parent.Status != durable.Completed || parent.Output != "parent used child finding" ||
				requests.Load() != wantRequests ||
				parent.TokensUsed != 6 ||
				math.Abs(parent.CostUSD-8e-6) > 1e-12 {
				t.Fatalf(
					"parent status=%s output=%q error=%q requests=%d tokens=%d cost=%v",
					parent.Status,
					parent.Output,
					parent.Error,
					requests.Load(),
					parent.TokensUsed,
					parent.CostUSD,
				)
			}
			countSubAgentChildren(t, store, root.ID, 1)
		})
	}
}

func TestSubAgentDualModeCorrectsInputs(t *testing.T) {
	store, _ := testDB(t)
	invalid := []string{
		`{"subagent":"unknown","task":"review"}`,
		`{"subagent":"review","task":"review","instruction":"extra"}`,
		`{"task":"   "}`,
		`{"task":`,
	}
	url, requests := subAgentModelServer(t, func(call int, request subAgentModelRequest) subAgentModelReply {
		if call == 1 {
			for _, tool := range request.Tools {
				if tool.Function.Name == delegateTool {
					params := tool.Function.Parameters
					if !reflect.DeepEqual(params.Required, []string{"task"}) ||
						params.Properties["subagent"] == nil || params.Properties["instruction"] == nil {
						t.Errorf("dual mode schema: %+v", params)
					}
				}
			}
		}
		for _, message := range request.Messages {
			for _, tc := range message.ToolCalls {
				if !json.Valid([]byte(tc.Function.Arguments)) {
					t.Errorf(
						"request %d carried invalid JSON tool arguments in history: %q",
						call,
						tc.Function.Arguments,
					)
				}
			}
		}
		if call >= 2 && call <= len(invalid)+1 {
			results := request.toolResults()
			if len(results) != call-1 || !strings.Contains(results[len(results)-1], "INVALID_TOOL_INPUT") {
				t.Errorf("request %d correction results=%q", call, results)
			}
		}
		switch {
		case call <= len(invalid):
			return subAgentDelegate(invalid[call-1])
		case call == len(invalid)+1:
			return subAgentDelegate(`{"subagent":"review","task":"review the corrected request"}`)
		case request.Model == specialistModel:
			return subAgentModelReply{content: "corrected result"}
		case call == len(invalid)+3:
			results := request.toolResults()
			if len(results) != len(invalid)+1 || results[len(results)-1] != "corrected result" {
				t.Errorf("parent did not receive corrected result: %q", results)
			}
			return subAgentDelegate(
				`{"task":"cross-check the review","instruction":"Verify independently"}`,
			)
		case call == len(invalid)+4:
			want := []subAgentModelMessage{
				{
					Role:    "user",
					Content: "[Subtask Instructions]\nVerify independently\n\n[Task]\ncross-check the review",
				},
			}
			if !reflect.DeepEqual(request.Messages, want) || request.hasTool(delegateTool) {
				t.Errorf(
					"dual mode self-delegation messages=%+v delegate=%t",
					request.Messages,
					request.hasTool(delegateTool),
				)
			}
			return subAgentModelReply{content: "cross-check result"}
		default:
			results := request.toolResults()
			if len(results) != len(invalid)+2 || results[len(results)-1] != "cross-check result" {
				t.Errorf("parent did not receive self-delegated result: %q", results)
			}
			return subAgentModelReply{content: "corrected successfully"}
		}
	})
	specialist := subAgentSpecialist(t, store, url)
	_, root := submitChatFixture(t, store, "owner", "dual", durable.AgentConfig{
		Model: subAgentModelSpec("parent", url), MaxTurns: 8, SelfDelegate: true,
	}, "review", time.Hour, 4, func(spec *durable.Spec) {
		spec.SubAgents = []durable.SubAgentSnapshot{specialist}
	})
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	child := waitingChild(t, store, executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)))
	countSubAgentChildren(t, store, root.ID, 1)
	executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
	child = waitingChild(t, store, executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)))
	countSubAgentChildren(t, store, root.ID, 2)
	executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
	done := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
	if done.Status != durable.Completed || done.Output != "corrected successfully" ||
		requests.Load() != int32(len(invalid)+5) {
		t.Fatalf(
			"status=%s output=%q error=%q requests=%d",
			done.Status,
			done.Output,
			done.Error,
			requests.Load(),
		)
	}
}

func expireSubAgentLease(t *testing.T, store *durable.Store, run *durable.Run) *durable.Run {
	t.Helper()
	result := store.DB().WithContext(t.Context()).Exec(
		"UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ? AND epoch = ?",
		run.ID, run.Epoch,
	)
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("expire lease: rows=%d err=%v", result.RowsAffected, result.Error)
	}
	reclaimed := claimSubAgentRun(t, store, run.ID)
	if reclaimed.Epoch <= run.Epoch {
		t.Fatalf("reclaim epoch=%d, previous=%d", reclaimed.Epoch, run.Epoch)
	}
	return reclaimed
}

func stopBeforeChildSpawnCommits(t *testing.T, worker *engine.Engine, run *durable.Run) {
	t.Helper()
	store := worker.Store
	database := store.DB().WithContext(t.Context())
	if err := database.Exec(`
		CREATE FUNCTION hold_child_spawn() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(8103493);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER hold_child_spawn BEFORE UPDATE ON durable_run
		FOR EACH ROW WHEN (NEW.status = 'WaitingExternal' AND OLD.status = 'Running')
		EXECUTE FUNCTION hold_child_spawn();
	`).Error; err != nil {
		t.Fatal(err)
	}
	gate := database.Begin()
	defer gate.Rollback()
	if err := gate.Exec("SELECT pg_advisory_xact_lock(8103493)").Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Execute(ctx, run) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := database.Raw(`SELECT EXISTS (
			SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 8103493 AND NOT granted
		)`).Scan(&waiting).Error
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("worker exited before issuing durable step: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted worker: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("interrupted worker did not stop")
	}
	countSubAgentChildren(t, store, run.ID, 0)
	parent := loadSubAgentRun(t, store, run.ID)
	var started bool
	if err := database.Raw(`SELECT EXISTS (
		SELECT 1 FROM durable_step WHERE run_id = ? AND step_id = 'turn_1_tool_0'
			AND status = 'Started' AND external_ref = ''
	)`, run.ID).Scan(&started).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != durable.Running || parent.TokensUsed != 3 || !started {
		t.Fatalf("interrupted parent status=%s tokens=%d", parent.Status, parent.TokensUsed)
	}
	if err := gate.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Exec("DROP TRIGGER hold_child_spawn ON durable_run").Error; err != nil {
		t.Fatal(err)
	}
}

func TestSubAgentSequentialBatchSurvivesReclaim(t *testing.T) {
	store, _ := testDB(t)
	url, requests := subAgentModelServer(t, func(call int, request subAgentModelRequest) subAgentModelReply {
		switch call {
		case 1:
			return subAgentModelReply{calls: []schema.ToolCall{
				subAgentReplyCall(delegateTool, `{"task":"first task"}`),
				subAgentReplyCall(delegateTool, `{"task":"second task"}`),
			}}
		case 2, 3:
			task, result := "first task", "first result"
			if call == 3 {
				task, result = "second task", "second result"
			}
			want := []subAgentModelMessage{{Role: "user", Content: task}}
			if !reflect.DeepEqual(request.Messages, want) {
				t.Errorf("child request %d: got %+v, want %+v", call, request.Messages, want)
			}
			return subAgentModelReply{content: result}
		default:
			expectSubAgentResults(t, request, "first result", "second result")
			return subAgentModelReply{content: "combined result"}
		}
	})
	_, root := submitChatFixture(t, store, "owner", "batch", durable.AgentConfig{
		Model: subAgentModelSpec("parent", url), MaxTurns: 5, SelfDelegate: true,
	}, "combine two tasks", time.Hour, 4)
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	firstClaim := claimSubAgentRun(t, store, root.ID)
	stopBeforeChildSpawnCommits(t, worker, firstClaim)
	paused := executeSubAgentRun(t, worker, expireSubAgentLease(t, store, firstClaim))
	first := waitingChild(t, store, paused)
	if paused.WaitingStep != "turn_1_tool_0" {
		t.Fatalf("first wait is on %q", paused.WaitingStep)
	}
	countSubAgentChildren(t, store, root.ID, 1)
	executeSubAgentRun(t, worker, claimSubAgentRun(t, store, first.ID))

	wokenClaim := claimSubAgentRun(t, store, root.ID)
	paused = executeSubAgentRun(t, worker, expireSubAgentLease(t, store, wokenClaim))
	second := waitingChild(t, store, paused)
	if paused.WaitingStep != "turn_1_tool_1" || second.ID == first.ID {
		t.Fatalf("second wait is on %q child %q", paused.WaitingStep, second.ID)
	}
	countSubAgentChildren(t, store, root.ID, 2)
	executeSubAgentRun(t, worker, claimSubAgentRun(t, store, second.ID))
	parent := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
	if parent.Status != durable.Completed || parent.Output != "combined result" ||
		parent.TokensUsed != 6 || requests.Load() != 4 {
		t.Fatalf("status=%s output=%q error=%q tokens=%d requests=%d",
			parent.Status, parent.Output, parent.Error, parent.TokensUsed, requests.Load())
	}
	countSubAgentChildren(t, store, root.ID, 2)
}

func TestSubAgentBatchWithReturnDirectly(t *testing.T) {
	for _, delegateFirst := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "direct then delegation", true: "delegation then direct"}[delegateFirst],
			func(t *testing.T) {
				store, _ := testDB(t)
				var directCalls atomic.Int32
				toolEndpoint := httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						directCalls.Add(1)
						fmt.Fprint(w, "direct result")
					}),
				)
				t.Cleanup(toolEndpoint.Close)
				url, requests := subAgentModelServer(
					t,
					func(call int, request subAgentModelRequest) subAgentModelReply {
						switch call {
						case 1:
							direct := subAgentReplyCall("deliver", `{}`)
							delegate := subAgentReplyCall(
								delegateTool,
								`{"task":"child task"}`,
							)
							calls := []schema.ToolCall{direct, delegate}
							if delegateFirst {
								calls = []schema.ToolCall{delegate, direct}
							}
							return subAgentModelReply{calls: calls}
						case 2:
							return subAgentModelReply{content: "child result"}
						default:
							expectSubAgentResults(
								t,
								request,
								"direct result",
								"child result",
							)
							return subAgentModelReply{content: "parent integrated child"}
						}
					},
				)
				_, root := submitChatFixture(t, store, "owner", "mixed batch", durable.AgentConfig{
					Model: subAgentModelSpec("parent", url), MaxTurns: 5, SelfDelegate: true,
					Tools: []durable.ToolSpec{
						{
							Name:           "deliver",
							URL:            toolEndpoint.URL,
							Policy:         durable.ReadOnly,
							ReturnDirectly: true,
							Parameters:     json.RawMessage(`{"type":"object"}`),
						},
					},
				}, "do a mixed batch", time.Hour, 4)
				worker := &engine.Engine{Store: store, Build: adapters.Build}
				child := waitingChild(
					t,
					store,
					executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)),
				)
				wantCallsBeforePause := int32(1)
				wantOutput, wantRequests := "parent integrated child", int32(3)
				if delegateFirst {
					wantCallsBeforePause, wantOutput, wantRequests = 0, "direct result", 2
				}
				if directCalls.Load() != wantCallsBeforePause {
					t.Fatalf(
						"direct calls at the pause=%d, want %d",
						directCalls.Load(),
						wantCallsBeforePause,
					)
				}
				executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
				done := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
				if done.Status != durable.Completed || done.Output != wantOutput ||
					requests.Load() != wantRequests ||
					directCalls.Load() != 1 {
					t.Fatalf(
						"status=%s output=%q error=%q requests=%d direct calls=%d",
						done.Status,
						done.Output,
						done.Error,
						requests.Load(),
						directCalls.Load(),
					)
				}
			},
		)
	}
}

func TestSubAgentTerminalStatusWakesParent(t *testing.T) {
	for _, status := range []durable.Status{durable.Failed, durable.Canceled} {
		t.Run(string(status), func(t *testing.T) {
			store, _ := testDB(t)
			url, requests := subAgentModelServer(
				t,
				func(call int, request subAgentModelRequest) subAgentModelReply {
					if call == 1 {
						return subAgentDelegate(`{"task":"child task"}`)
					}
					if call == 2 && status == durable.Failed {
						return subAgentModelReply{content: "incomplete", finishReason: "length"}
					}
					results := request.toolResults()
					if len(results) != 1 || !strings.Contains(results[0], " "+string(status)+": ") {
						t.Errorf("terminal child feedback=%q, status=%q", results, status)
					}
					return subAgentModelReply{content: "parent recovered"}
				},
			)
			_, root := submitChatFixture(t, store, "owner", "terminal child", durable.AgentConfig{
				Model: subAgentModelSpec("parent", url), MaxTurns: 5, SelfDelegate: true,
			}, "handle terminal child", time.Hour, 4)
			worker := &engine.Engine{Store: store, Build: adapters.Build}
			child := waitingChild(
				t,
				store,
				executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)),
			)
			wantRequests := int32(3)
			if status == durable.Canceled {
				wantRequests = 2
				if err := store.Cancel(t.Context(), "owner", child.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
			}
			if current := loadSubAgentRun(t, store, child.ID); current.Status != status {
				t.Fatalf("child status=%s error=%q, want %s", current.Status, current.Error, status)
			}
			if current := loadSubAgentRun(t, store, root.ID); current.Status != durable.Queued {
				t.Fatalf("parent did not wake: status=%s error=%q", current.Status, current.Error)
			}
			result, _ := integrationMCPStep(t, store, root.ID, child.ParentStepID)
			if !strings.HasPrefix(result, "subtask "+child.ID+" "+string(status)+": ") {
				t.Errorf("stored terminal child result=%q", result)
			}
			done := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
			if done.Status != durable.Completed || done.Output != "parent recovered" ||
				requests.Load() != wantRequests {
				t.Fatalf(
					"status=%s output=%q error=%q requests=%d",
					done.Status,
					done.Output,
					done.Error,
					requests.Load(),
				)
			}
		})
	}
}

func TestSubAgentGoalUnblockResumesEvaluation(t *testing.T) {
	store, _ := testDB(t)
	var toolCalls atomic.Int32
	toolEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		toolCalls.Add(1)
		fmt.Fprint(w, "evidence")
	}))
	t.Cleanup(toolEndpoint.Close)
	url, requests := subAgentModelServer(t, func(call int, request subAgentModelRequest) subAgentModelReply {
		switch call {
		case 1:
			return subAgentDelegate(`{"task":"gather evidence"}`)
		case 2:
			return subAgentModelReply{calls: []schema.ToolCall{subAgentReplyCall("lookup", `{}`)}}
		case 3:
			expectSubAgentResults(t, request, "evidence")
			return subAgentModelReply{content: "unknown consumption", noUsage: true}
		case 4:
			expectSubAgentResults(t, request, "evidence")
			return subAgentModelReply{content: "child evidence"}
		default:
			expectSubAgentResults(t, request, "child evidence")
			return subAgentModelReply{content: "verified parent result"}
		}
	})
	_, root := submitChatFixture(t, store, "owner", "goal delegation", durable.AgentConfig{
		Model: subAgentModelSpec("parent", url), MaxTurns: 5, SelfDelegate: true,
		Tools: []durable.ToolSpec{
			{
				Name:       "lookup",
				URL:        toolEndpoint.URL,
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}, "gather and verify evidence", time.Hour, 4, pricedSpec(1, 2))
	goal, _, err := store.CreateGoal(t.Context(), durable.Goal{
		OwnerID: "owner", SessionID: root.SessionID, RequestKey: "delegate", RequestHash: "delegate",
		Objective: "verify evidence",
		Budget:    durable.BudgetSpec{MaxWallClockSeconds: 60, MaxTokens: 20, MaxCostUSD: 0.01},
		Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "verify evidence"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindGoalToRun(t.Context(), "owner", goal.ID, root.ID); err != nil {
		t.Fatal(err)
	}
	executor := &engine.Engine{Store: store, Build: adapters.Build}
	child := waitingChild(t, store, executeSubAgentRun(t, executor, claimSubAgentRun(t, store, root.ID)))
	if deadline := goal.CreatedAt.Add(time.Minute); !child.Deadline.Equal(deadline) {
		t.Fatalf("child deadline=%s want Goal deadline %s", child.Deadline, deadline)
	}
	blocked := executeSubAgentRun(t, executor, claimSubAgentRun(t, store, child.ID))
	before, err := store.GetGoal(t.Context(), "owner", goal.ID)
	if err != nil || before.Status != durable.GoalBlocked || before.TokensUsed != 6 ||
		blocked.Status != durable.Blocked || blocked.TokensUsed != 3 {
		t.Fatalf("goal=%+v child=%s child tokens=%d child error=%q err=%v",
			before, blocked.Status, blocked.TokensUsed, blocked.Error, err)
	}
	if err := store.Unblock(
		t.Context(),
		"owner",
		child.ID,
		blocked.Epoch,
		"accept unknown model consumption",
	); err != nil {
		t.Fatal(err)
	}
	before, err = store.GetGoal(t.Context(), "owner", goal.ID)
	if err != nil || before.Status != durable.GoalBlocked {
		t.Fatalf("child unblock changed Goal status: goal=%+v error=%v", before, err)
	}
	if err := store.TransitionGoal(t.Context(), "owner", goal.ID, durable.GoalActive); err != nil {
		t.Fatal(err)
	}
	var evaluations atomic.Int32
	worker := &engine.Worker{
		Engine:      executor,
		ID:          "goal-worker",
		Concurrency: 1,
		Lease:       15 * time.Second,
		Poll:        10 * time.Millisecond,
		Evaluator: iterationEvaluator(
			func(_ context.Context, spec durable.EvaluatorSpec, run *durable.Run) (durable.EvaluationStatus, string, error) {
				evaluations.Add(1)
				if run.ID != root.ID || run.Output != "verified parent result" ||
					spec.Config != "verify evidence" {
					return durable.EvaluationFailed, "unexpected evaluation input", nil
				}
				return durable.EvaluationPassed, "evidence verified", nil
			},
		),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-workerDone; !errors.Is(err, context.Canceled) {
			t.Errorf("Goal Worker stopped: %v", err)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var completed *durable.Goal
	for {
		completed, err = store.GetGoal(ctx, "owner", goal.ID)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Status == durable.GoalComplete {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Goal did not complete: %+v", completed)
		case <-ticker.C:
		}
	}
	receipt, err := store.GetEvaluation(ctx, "owner", goal.ID, root.ID)
	if err != nil || receipt.Status != durable.EvaluationPassed || evaluations.Load() != 1 {
		t.Fatalf("evaluation receipt=%+v calls=%d err=%v", receipt, evaluations.Load(), err)
	}
	done := loadSubAgentRun(t, store, root.ID)
	finishedChild := loadSubAgentRun(t, store, child.ID)
	if done.Status != durable.Completed || finishedChild.Status != durable.Completed ||
		done.TokensUsed != 6 || finishedChild.TokensUsed != 6 || completed.TokensUsed != 12 ||
		math.Abs(completed.CostUSD-16e-6) > 1e-12 || requests.Load() != 5 || toolCalls.Load() != 1 {
		t.Fatalf(
			"parent=%s/%d child=%s/%d Goal=%+v models=%d tools=%d",
			done.Status,
			done.TokensUsed,
			finishedChild.Status,
			finishedChild.TokensUsed,
			completed,
			requests.Load(),
			toolCalls.Load(),
		)
	}
	countSubAgentChildren(t, store, root.ID, 1)
}

func TestSubAgentLegacyStaticToolKeepsHTTPBinding(t *testing.T) {
	store, _ := testDB(t)
	var httpCalls atomic.Int32
	staticEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		var input map[string]string
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input["legacy"] != "payload" {
			t.Errorf("legacy HTTP request=%v err=%v", input, err)
		}
		fmt.Fprint(w, "legacy HTTP result")
	}))
	t.Cleanup(staticEndpoint.Close)
	url, requests := subAgentModelServer(t, func(call int, request subAgentModelRequest) subAgentModelReply {
		if call == 1 {
			if !request.hasTool(delegateTool) {
				t.Error("legacy tool is missing")
			}
			return subAgentDelegate(`{"legacy":"payload"}`)
		}
		expectSubAgentResults(t, request, "legacy HTTP result")
		return subAgentModelReply{content: "used HTTP result"}
	})
	_, root := submitChatFixture(t, store, "owner", "legacy same name", durable.AgentConfig{
		Model: subAgentModelSpec("parent", url), MaxTurns: 5,
	}, "call legacy tool", time.Hour, 4, func(spec *durable.Spec) {
		spec.SelfDelegate = true
		spec.Tools = []durable.ToolSpec{
			{
				Name:   delegateTool,
				URL:    staticEndpoint.URL,
				Policy: durable.ReadOnly,
				Parameters: json.RawMessage(
					`{"type":"object","required":["legacy"],"properties":{"legacy":{"type":"string"}}}`,
				),
			},
		}
	})
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	done := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
	if done.Status != durable.Completed || done.Output != "used HTTP result" ||
		requests.Load() != 2 || httpCalls.Load() != 1 {
		t.Fatalf("status=%s output=%q error=%q model calls=%d HTTP calls=%d",
			done.Status, done.Output, done.Error, requests.Load(), httpCalls.Load())
	}
	countSubAgentChildren(t, store, root.ID, 0)
}

func TestSubAgentToolCountUsesFinalDynamicToolset(t *testing.T) {
	for _, test := range []struct {
		name        string
		staticCount int
		delegate    bool
		wantCount   int
		wantFailure bool
	}{
		{name: "128 assembled with delegate", staticCount: 126, delegate: true, wantCount: 128},
		{name: "129 assembled with delegate", staticCount: 127, delegate: true, wantFailure: true},
		{name: "128 assembled without delegate", staticCount: 127, wantCount: 128},
		{name: "129 assembled without delegate", staticCount: 128, wantFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			url, requests := subAgentModelServer(
				t,
				func(_ int, request subAgentModelRequest) subAgentModelReply {
					if len(request.Tools) != test.wantCount ||
						request.hasTool(delegateTool) != test.delegate ||
						!request.hasTool(durable.ReadStepResultToolName) {
						t.Errorf(
							"advertised tools=%d delegate=%t read=%t",
							len(request.Tools),
							request.hasTool(
								delegateTool,
							),
							request.hasTool(durable.ReadStepResultToolName),
						)
					}
					return subAgentModelReply{content: "count accepted"}
				},
			)
			tools := make([]durable.ToolSpec, test.staticCount)
			for index := range tools {
				tools[index] = durable.ToolSpec{
					Name: fmt.Sprintf("static_%d", index), URL: url, Policy: durable.ReadOnly,
					Parameters: json.RawMessage(`{"type":"object"}`),
				}
			}
			_, root := submitChatFixture(t, store, "owner", "many tools", durable.AgentConfig{
				Model: subAgentModelSpec(
					"parent",
					url,
				),
				MaxTurns:     5,
				SelfDelegate: test.delegate,
				Tools:        tools,
			}, "check tool count", time.Hour, 4)
			worker := &engine.Engine{Store: store, Build: adapters.Build}
			done := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
			if test.wantFailure {
				if done.Status != durable.Failed ||
					!strings.Contains(done.Error, "tool count 129 exceeds limit of 128") ||
					requests.Load() != 0 {
					t.Fatalf(
						"status=%s error=%q calls=%d",
						done.Status,
						done.Error,
						requests.Load(),
					)
				}
			} else if done.Status != durable.Completed || done.Output != "count accepted" || requests.Load() != 1 {
				t.Fatalf(
					"status=%s output=%q error=%q calls=%d",
					done.Status,
					done.Output,
					done.Error,
					requests.Load(),
				)
			}
		})
	}
}

func TestSubAgentChildDelegationReturnsCorrectableFeedback(t *testing.T) {
	store, _ := testDB(t)
	url, requests := subAgentModelServer(t, func(call int, request subAgentModelRequest) subAgentModelReply {
		for _, message := range request.Messages {
			for _, tc := range message.ToolCalls {
				if !json.Valid([]byte(tc.Function.Arguments)) {
					t.Errorf(
						"request %d carried invalid JSON tool arguments: %q",
						call,
						tc.Function.Arguments,
					)
				}
			}
		}
		switch call {
		case 1:
			return subAgentDelegate(`{"task":"investigate directly"}`)
		case 2:
			if request.hasTool(delegateTool) {
				t.Errorf("child should not advertise %s", delegateTool)
			}
			// Child mimics inherited parent prompt and attempts nested delegation with broken JSON.
			return subAgentDelegate(`{"task":`)
		case 3:
			results := request.toolResults()
			if len(results) != 1 || !strings.Contains(results[0], "SUBTASK_DELEGATION_UNAVAILABLE") {
				t.Errorf("child did not receive delegation-unavailable feedback: %q", results)
			}
			return subAgentModelReply{content: "child solved it directly"}
		default:
			results := request.toolResults()
			if len(results) != 1 || results[0] != "child solved it directly" {
				t.Errorf("parent did not receive child output: %q", results)
			}
			return subAgentModelReply{content: "parent finished with direct child answer"}
		}
	})
	_, root := submitChatFixture(t, store, "owner", "self-delegate-guard", durable.AgentConfig{
		Model:        subAgentModelSpec("parent", url),
		SystemPrompt: "Use delegate_subagent for subtasks.",
		MaxTurns:     5,
		SelfDelegate: true,
		Tools: []durable.ToolSpec{{
			Name:       "lookup",
			URL:        url,
			Policy:     durable.ReadOnly,
			Parameters: json.RawMessage(`{"type":"object"}`),
		}},
	}, "solve task", time.Hour, 4)
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	child := waitingChild(t, store, executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID)))
	doneChild := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, child.ID))
	if doneChild.Status != durable.Completed || doneChild.Output != "child solved it directly" {
		t.Fatalf("child status=%s output=%q error=%q", doneChild.Status, doneChild.Output, doneChild.Error)
	}
	doneParent := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, root.ID))
	if doneParent.Status != durable.Completed || doneParent.Output != "parent finished with direct child answer" ||
		requests.Load() != 4 {
		t.Fatalf(
			"parent status=%s output=%q error=%q calls=%d",
			doneParent.Status,
			doneParent.Output,
			doneParent.Error,
			requests.Load(),
		)
	}
	countSubAgentChildren(t, store, root.ID, 1)
}
