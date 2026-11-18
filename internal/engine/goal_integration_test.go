package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type goalEnvironmentTool struct {
	fakeTool
	*sandboxAdapter.Backend
	onDetach func()
}

func (tool *goalEnvironmentTool) Detach() {
	tool.Backend.Detach()
	tool.onDetach()
}

func TestWorkerEvaluatesGoalBeforeSandboxRelease(t *testing.T) {
	zero, one := 0, 1
	cases := []struct {
		name       string
		exitCode   *int
		opStatus   string
		stream     string
		opError    string
		evaluation durable.EvaluationStatus
		goalStatus durable.GoalStatus
		stalls     int
		output     string
		retain     bool
		injectSQL  string
		noReceipt  bool
	}{
		{
			name:       "passed",
			exitCode:   &zero,
			opStatus:   sdk.SandboxOpSucceeded,
			stream:     "{\"type\":\"stdout\",\"text\":\"PASS\\n\"}\n{\"type\":\"execution_complete\",\"exit_code\":0}",
			evaluation: durable.EvaluationPassed,
			goalStatus: durable.GoalComplete,
			output:     "PASS\n",
		},
		{
			name:       "failed",
			exitCode:   &one,
			opStatus:   sdk.SandboxOpFailed,
			stream:     "{\"type\":\"stderr\",\"text\":\"FAIL\\n\"}\n{\"type\":\"error\",\"error\":{\"exit_code\":1}}",
			opError:    "exit status 1",
			evaluation: durable.EvaluationFailed,
			goalStatus: durable.GoalActive,
			stalls:     1,
			output:     "FAIL\n",
		},
		{
			name: "missing verdict", opStatus: sdk.SandboxOpFailed,
			stream: "{\"type\":\"stdout\",\"text\":\"partial\\n\"}", opError: "missing terminal event",
			evaluation: durable.EvaluationError, goalStatus: durable.GoalActive,
			output: "partial\n\nsandbox-center exec: missing command exit verdict: missing terminal event",
			retain: true,
		},
		{
			name: "receipt persistence fails", exitCode: &zero, opStatus: sdk.SandboxOpSucceeded,
			goalStatus: durable.GoalActive, retain: true, noReceipt: true,
			injectSQL: "ALTER TABLE durable_evaluation ADD CONSTRAINT injected_failure CHECK (false)",
		},
		{
			name: "goal progress persistence fails", exitCode: &zero, opStatus: sdk.SandboxOpSucceeded,
			evaluation: durable.EvaluationPassed, goalStatus: durable.GoalActive, retain: true,
			injectSQL: "ALTER TABLE durable_goal ADD CONSTRAINT injected_failure CHECK (status = 'Active')",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			run := submit(t, store, "submit", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID:     "owner",
				SessionID:   run.SessionID,
				RequestKey:  "acceptance",
				RequestHash: "acceptance",
				Objective:   "pass the acceptance command",
				Evaluator: durable.EvaluatorSpec{
					Kind:   durable.EvaluatorCommand,
					Config: "  go test ./...\n",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			entered, proceed, released := make(
				chan struct{},
				1,
			), make(
				chan struct{},
			), make(
				chan struct{},
				1,
			)
			renewed, detached := make(chan struct{}, 1), make(chan struct{}, 1)
			var execCalls, releaseCalls atomic.Int32
			var initialExpiry atomic.Int64
			environmentKey := run.ID + ":env"
			server := httptest.NewServer(
				http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Type", "application/json")
					switch {
					case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes":
						var input sdk.AcquireSandboxRequest
						if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
							t.Errorf("acquire request: %v", err)
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
						env, err := store.GetEnvironment(
							request.Context(),
							run.ID,
							environmentKey,
						)
						if err != nil || env.Status != durable.EnvironmentPreparing ||
							input.RunID != run.ID {
							t.Errorf(
								"acquire before durable intent: env=%+v input=%+v err=%v",
								env,
								input,
								err,
							)
						}
						current, err := store.Get(request.Context(), "owner", run.ID)
						if err != nil || current.LeaseUntil == nil {
							t.Errorf("acquire without active run lease: %v", err)
							writer.WriteHeader(http.StatusConflict)
							return
						}
						initialExpiry.Store(current.LeaseUntil.UnixNano())
						json.NewEncoder(writer).Encode(sdk.SandboxView{
							Sandbox: &sdk.Sandbox{
								ID:         "sb-eval",
								Status:     sdk.SandboxStatusActive,
								LeaseID:    "lease-eval",
								Generation: 1,
							},
							Lease: &sdk.SandboxLease{
								ID:            "lease-eval",
								Revision:      1,
								Status:        "Active",
								HardExpiresAt: *current.LeaseUntil,
							},
						})
					case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes/sb-eval/renew":
						var input sdk.RenewSandboxRequest
						if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
							t.Errorf("renew request: %v", err)
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
						current, err := store.Get(request.Context(), "owner", run.ID)
						if err != nil {
							t.Errorf("renewal could not read run: err=%v", err)
							writer.WriteHeader(http.StatusInternalServerError)
							return
						}
						if current.Status == durable.Completed &&
							(current.WorkerID != "" || current.LeaseUntil != nil) {
							t.Errorf(
								"completed run retained its claim during renewal: run=%+v",
								current,
							)
						}
						json.NewEncoder(writer).Encode(sdk.SandboxLease{
							ID: "lease-eval", Revision: input.Revision, Status: "Active",
							HardExpiresAt: time.Now().Add(time.Second),
						})
						if current.Status == durable.Completed && current.WorkerID == "" &&
							current.LeaseUntil == nil &&
							time.Now().After(time.Unix(0, initialExpiry.Load())) {
							select {
							case renewed <- struct{}{}:
							default:
							}
						}
					case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes/sb-eval/operations":
						execCalls.Add(1)
						var input sdk.ExecSandboxRequest
						if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
							t.Errorf("exec request: %v", err)
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
						if input.RequestKey != "eval-sb-eval" ||
							input.Script != "go test ./..." {
							t.Errorf("unexpected evaluation request: %+v", input)
						}
						current, err := store.Get(request.Context(), "owner", run.ID)
						if err != nil || current.Status != durable.Completed {
							t.Errorf(
								"evaluation before run completion: run=%+v err=%v",
								current,
								err,
							)
						}
						entered <- struct{}{}
						select {
						case <-proceed:
						case <-request.Context().Done():
							return
						case <-ctx.Done():
							return
						}
						if err := json.NewEncoder(writer).Encode(sdk.SandboxOperation{
							ID: "op-eval", Status: test.opStatus, ExitCode: test.exitCode,
							Output: test.stream, Error: test.opError,
						}); err != nil {
							t.Errorf("write receipt: %v", err)
						}
					case request.Method == http.MethodDelete && request.URL.Path == "/v1/sandboxes/sb-eval":
						releaseCalls.Add(1)
						receipt, err := store.GetEvaluation(
							request.Context(),
							"owner",
							goal.ID,
							run.ID,
						)
						if err != nil || receipt.Status != test.evaluation {
							t.Errorf(
								"release before evaluation persisted: receipt=%+v err=%v",
								receipt,
								err,
							)
						}
						current, err := store.GetGoal(request.Context(), "owner", goal.ID)
						if err != nil || current.Status != test.goalStatus ||
							current.StallCount != test.stalls {
							t.Errorf(
								"release before goal progress: goal=%+v err=%v",
								current,
								err,
							)
						}
						fmt.Fprint(writer, `{"id":"sb-eval","status":"Destroyed"}`)
						released <- struct{}{}
					default:
						t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
						writer.WriteHeader(http.StatusNotFound)
					}
				}),
			)
			defer server.Close()
			client, err := sdk.NewClient(server.URL, "test-token", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			worker := engine.Worker{
				ID:          "goal-worker",
				Concurrency: 1,
				Lease:       time.Second,
				Poll:        10 * time.Millisecond,
				Evaluator: engine.SandboxEvaluator{
					Store:  store,
					Runner: engine.HTTPSandboxRunner{Client: client.Sandboxes},
				},
				Engine: &engine.Engine{
					Store: store,
					Build: func(buildCtx context.Context, store *durable.Store, _ durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
						claim, ok := engine.BuildRunContext(buildCtx)
						if !ok {
							return nil, nil, errors.New("missing run claim")
						}
						backend, err := sandboxAdapter.New(
							buildCtx,
							store,
							sandboxAdapter.Config{
								Endpoint:   server.URL,
								Token:      "test-token",
								Workspace:  "/workspace",
								OwnerID:    claim.OwnerID,
								RunID:      claim.RunID,
								WorkerID:   claim.WorkerID,
								Epoch:      claim.Epoch,
								AcquireKey: environmentKey,
							},
						)
						if err != nil {
							return nil, nil, err
						}
						return &fakeModel{
							generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
								return schema.AssistantMessage(
									"work completed",
									nil,
								), nil
							},
						}, map[string]engine.Binding{
							"submit": {Policy: durable.ReadOnly, Tool: &goalEnvironmentTool{
								fakeTool: fakeTool{name: "submit"}, Backend: backend,
								onDetach: func() { detached <- struct{}{} },
							}},
						}, nil
					},
				},
			}
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("worker exit: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				inspectCtx, stopInspect := context.WithTimeout(context.Background(), 2*time.Second)
				defer stopInspect()
				current, readErr := store.Get(inspectCtx, "owner", run.ID)
				t.Fatalf("worker did not reach evaluation: run=%+v err=%v", current, readErr)
			}
			if releaseCalls.Load() != 0 {
				t.Fatal("sandbox released while evaluator was running")
			}
			if _, err := store.GetEvaluation(
				ctx,
				"owner",
				goal.ID,
				run.ID,
			); !errors.Is(
				err,
				durable.ErrNotFound,
			) {
				t.Fatalf("receipt present before evaluator response: %v", err)
			}
			env, err := store.ActiveEnvironment(ctx, run.ID)
			if err != nil || env.SandboxID != "sb-eval" {
				t.Fatalf("environment unavailable during evaluation: env=%+v err=%v", env, err)
			}
			select {
			case <-renewed:
			case <-ctx.Done():
				t.Fatal("sandbox renewal did not survive the original run claim deadline")
			}
			if test.injectSQL != "" {
				if err := store.DB().WithContext(ctx).Exec(test.injectSQL).Error; err != nil {
					t.Fatal(err)
				}
			}
			close(proceed)
			finished := released
			if test.retain {
				finished = detached
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("sandbox lifecycle did not finish after evaluation")
			}
			receipt, err := store.GetEvaluation(ctx, "owner", goal.ID, run.ID)
			if test.noReceipt {
				if !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("failed persistence left a receipt: %+v err=%v", receipt, err)
				}
			} else {
				if err != nil || receipt.Status != test.evaluation || receipt.Output != test.output {
					t.Fatalf(
						"receipt=%+v err=%v, want status=%s output=%q",
						receipt,
						err,
						test.evaluation,
						test.output,
					)
				}
			}
			current, err := store.GetGoal(ctx, "owner", goal.ID)
			if err != nil || current.Status != test.goalStatus || current.StallCount != test.stalls {
				t.Fatalf("goal progress: goal=%+v err=%v", current, err)
			}
			if !test.retain {
				current, err = store.CheckGoalProgress(ctx, "owner", goal.ID)
				if err != nil || current.Status != test.goalStatus ||
					current.StallCount != test.stalls {
					t.Fatalf("repeated progress check: goal=%+v err=%v", current, err)
				}
			} else {
				env, err = store.ActiveEnvironment(ctx, run.ID)
				if err != nil || env.Status != durable.EnvironmentActive {
					t.Fatalf("unresolved evaluation lost its environment: env=%+v err=%v", env, err)
				}
			}
			wantReleases := int32(1)
			if test.retain {
				wantReleases = 0
			}
			if execCalls.Load() != 1 || releaseCalls.Load() != wantReleases {
				t.Fatalf(
					"execs=%d releases=%d, want 1 and %d",
					execCalls.Load(),
					releaseCalls.Load(),
					wantReleases,
				)
			}
			if receipt != nil && strings.Contains(receipt.Output, `"type"`) {
				t.Fatalf("raw protocol in evaluation output: %q", receipt.Output)
			}
		})
	}
}

type retainedEnvironmentTool struct {
	closingTool
	detached chan struct{}
}

func (tool *retainedEnvironmentTool) Detach() { close(tool.detached) }

func TestWorkerRetainsEnvironmentWhenGoalReadFails(t *testing.T) {
	for _, tableName := range []string{"durable_goal", "durable_evaluation"} {
		t.Run(tableName, func(t *testing.T) {
			store, _ := testDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := submit(t, store, "submit", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: run.SessionID, RequestKey: "acceptance",
				RequestHash: "acceptance", Objective: "pass acceptance",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			readFailure := errors.New("injected goal read failure")
			var failures atomic.Int32
			if err := store.DB().
				Callback().
				Query().
				Before("gorm:query").
				Register("test:fail_goal_read", func(database *gorm.DB) {
					if database.Statement.Table == tableName {
						var completed bool
						if err := store.DB().WithContext(database.Statement.Context).
							Raw("SELECT status = ? FROM durable_run WHERE id = ?", durable.Completed, run.ID).
							Scan(&completed).Error; err != nil {
							database.AddError(err)
							return
						}
						if !completed {
							return
						}
						failures.Add(1)
						database.AddError(readFailure)
					}
				}); err != nil {
				t.Fatal(err)
			}
			environment := &retainedEnvironmentTool{
				closingTool: closingTool{fakeTool: fakeTool{name: "submit"}},
				detached:    make(chan struct{}),
			}
			worker := engine.Worker{
				ID: "goal-worker", Concurrency: 1, Lease: time.Second, Poll: 10 * time.Millisecond,
				Engine: &engine.Engine{
					Store: store,
					Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
						return &fakeModel{
							generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
								return schema.AssistantMessage("done", nil), nil
							},
						}, map[string]engine.Binding{
							"submit": {Policy: durable.ReadOnly, Tool: environment},
						}, nil
					},
				},
			}
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			select {
			case <-environment.detached:
			case <-ctx.Done():
				t.Error("goal read failure did not detach the environment")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("worker did not stop")
			}
			if failures.Load() == 0 || environment.closed != 0 {
				t.Fatalf("read failures=%d releases=%d", failures.Load(), environment.closed)
			}
			current, err := store.Get(context.Background(), "owner", run.ID)
			if err != nil || current.Status != durable.Completed {
				t.Fatalf("run did not complete before read failure: run=%+v err=%v", current, err)
			}
		})
	}
}
