package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestGoalIterationAcquiresSavedWorkspace(t *testing.T) {
	store, dsn := testDB(t)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first := submit(t, store, "submit", durable.ReadOnly)
	checkpoint := fmt.Sprintf("sha256:%064x:%064x", 1, 2)
	acquired := make(chan sdk.AcquireSandboxRequest, 4)
	released := make(chan string, 4)
	var mutex sync.Mutex
	views := make(map[string]sdk.SandboxView)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		mutex.Lock()
		defer mutex.Unlock()
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes":
			var input sdk.AcquireSandboxRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if !input.RequiredSave {
				t.Error("goal sandbox did not request a persistent workspace")
			}
			sandboxID := "sandbox-" + input.RunID
			sandbox := &sdk.Sandbox{
				ID:                   sandboxID,
				Status:               sdk.SandboxStatusActive,
				RequiredSave:         input.RequiredSave,
				LeaseID:              "lease-" + input.RunID,
				Generation:           1,
				RestoreFromSandboxID: input.RestoreFromSandboxID,
			}
			if input.RestoreFromSandboxID != "" {
				sandbox.ResumeCheckpoint = checkpoint
			}
			view := sdk.SandboxView{
				Sandbox: sandbox,
				Lease: &sdk.SandboxLease{
					ID:            sandbox.LeaseID,
					Revision:      1,
					Status:        "Active",
					HardExpiresAt: time.Now().Add(time.Minute),
				},
			}
			views[sandboxID] = view
			acquired <- input
			_ = json.NewEncoder(writer).Encode(view)
		case request.Method == http.MethodGet:
			view, ok := views[strings.TrimPrefix(request.URL.Path, "/v1/sandboxes/")]
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(writer).Encode(view)
		case request.Method == http.MethodDelete:
			id := strings.TrimPrefix(request.URL.Path, "/v1/sandboxes/")
			view, ok := views[id]
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			view.Sandbox.Status = sdk.SandboxStatusSaveBlocked
			released <- id
			_ = json.NewEncoder(writer).Encode(view.Sandbox)
		default:
			t.Errorf("unexpected sandbox request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Setenv("WORKSPACE_TEST_TOKEN", "test")
	spec := first.Spec
	spec.Toolsets = []string{"filesystem"}
	spec.Filesystem = &durable.FilesystemBinding{
		Provider: "Sandbox", URL: server.URL, TokenEnv: "WORKSPACE_TEST_TOKEN", Workspace: "/workspace/project",
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Table("durable_run").Where("id = ?", first.ID).Update("spec", string(data)).Error; err != nil {
		t.Fatal(err)
	}
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID:    "owner",
		SessionID:  first.SessionID,
		RequestKey: "workspace",
		Objective:  "fix the project",
		Evaluator:  durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "check"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(ctx, "owner", goal.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	var evaluations atomic.Int32
	worker := engine.Worker{
		ID:          "workspace-worker",
		Concurrency: 1,
		Lease:       5 * time.Second,
		Poll:        10 * time.Millisecond,
		Evaluator: iterationEvaluator(
			func(context.Context, durable.EvaluatorSpec, *durable.Run) (durable.EvaluationStatus, string, error) {
				if evaluations.Add(1) == 1 {
					return durable.EvaluationFailed, "fix the file", nil
				}
				return durable.EvaluationPassed, "accepted", nil
			},
		),
		Engine: &engine.Engine{
			Store: store,
			Build: func(ctx context.Context, store *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
				_, bindings, err := adapters.Build(ctx, store, spec)
				return &fakeModel{
					generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
						return schema.AssistantMessage("done", nil), nil
					},
				}, bindings, err
			},
		},
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("worker did not stop")
		}
	}()
	select {
	case id := <-released:
		if id != "sandbox-"+first.ID {
			t.Fatalf("released wrong sandbox: %s", id)
		}
	case <-ctx.Done():
		t.Fatal("first run did not reach workspace saving")
	}
	if next, err := store.QueueGoalIteration(ctx); !errors.Is(err, durable.ErrNoWork) || next != nil {
		t.Fatalf("iteration started before checkpoint confirmation: %+v %v", next, err)
	}
	firstAcquire := <-acquired
	if firstAcquire.RestoreFromSandboxID != "" || firstAcquire.RunID != first.ID {
		t.Fatalf("unexpected first acquire: %+v", firstAcquire)
	}
	if err = db.Table("durable_run_environment").Where("run_id = ?", first.ID).
		Updates(map[string]any{"status": durable.EnvironmentReleased, "saved_checkpoint": checkpoint}).
		Error; err != nil {
		t.Fatal(err)
	}
	var secondAcquire sdk.AcquireSandboxRequest
	select {
	case secondAcquire = <-acquired:
	case <-ctx.Done():
		t.Fatal("saved workspace did not produce the next acquire")
	}
	if !secondAcquire.RequiredSave || secondAcquire.RestoreFromSandboxID != "sandbox-"+first.ID ||
		secondAcquire.RunID == first.ID {
		t.Fatalf("next run did not request the saved workspace: %+v", secondAcquire)
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("second run did not complete")
	}
	current, err := store.GetGoal(ctx, "owner", goal.ID)
	if err != nil || current.Status != durable.GoalComplete || current.ActiveRunID != secondAcquire.RunID ||
		evaluations.Load() != 2 {
		t.Fatalf("goal=%+v evaluations=%d err=%v", current, evaluations.Load(), err)
	}
	runs, err := store.SessionRuns(ctx, "owner", first.SessionID, "", 100)
	if err != nil || len(runs) != 2 {
		t.Fatalf("unexpected iterations: %+v %v", runs, err)
	}
}
