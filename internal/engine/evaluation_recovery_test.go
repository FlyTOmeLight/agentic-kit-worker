package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"

	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestEvaluationRecoveryChild(t *testing.T) {
	if os.Getenv("EVALUATION_RECOVERY_CHILD") != "1" {
		return
	}
	ctx := context.Background()
	store, err := durable.Open(ctx, os.Getenv("EVALUATION_RECOVERY_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	run, err := store.ClaimEvaluation(
		ctx,
		"worker-before-crash",
		400*time.Millisecond,
		os.Getenv("EVALUATION_RECOVERY_RUN"),
	)
	if err != nil {
		t.Fatal(err)
	}
	client, err := sdk.NewClient(os.Getenv("EVALUATION_RECOVERY_URL"), "test-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Sandboxes.Exec(ctx, &sdk.ExecSandboxRequest{
		ID: "sb-recovery", RequestKey: "eval-sb-recovery", Script: "true",
	})
	t.Fatalf("worker should be killed during execution for run %s: %v", run.ID, err)
}

func TestStoredEvaluationAdvancesWithoutSandbox(t *testing.T) {
	cases := []struct {
		status durable.EvaluationStatus
		goal   durable.GoalStatus
		stalls int
	}{
		{status: durable.EvaluationPassed, goal: durable.GoalComplete, stalls: 0},
		{status: durable.EvaluationFailed, goal: durable.GoalActive, stalls: 1},
	}
	for _, test := range cases {
		t.Run(string(test.status), func(t *testing.T) {
			store, _ := testDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			run := submit(t, store, "stored-acceptance", durable.ReadOnly)
			goal, _, err := store.CreateGoal(ctx, durable.Goal{
				OwnerID: "owner", SessionID: run.SessionID, RequestKey: "stored-goal",
				RequestHash: "stored-goal", Objective: "accept the stored result",
				Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			original, err := store.Claim(ctx, "execution-worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			env, err := store.PrepareEnvironment(ctx, original.Lease(), "stored-env", "coding", 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.ActivateEnvironment(
				ctx,
				original.Lease(),
				env.RequestKey,
				"sb-stored",
				"lease-stored",
				1,
			); err != nil {
				t.Fatal(err)
			}
			if err = store.CommitBoundary(ctx, original.Lease(), durable.Boundary{
				Kind: durable.BoundaryComplete, State: json.RawMessage(`{}`), Output: "done",
			}); err != nil {
				t.Fatal(err)
			}
			if _, err = store.RecordEvaluation(
				ctx,
				"owner",
				goal.ID,
				run.ID,
				test.status,
				"stored verdict",
			); err != nil {
				t.Fatal(err)
			}
			type observation struct {
				goal *durable.Goal
				err  error
			}
			observed := make(chan observation, 1)
			worker := engine.Worker{
				ID:          "cleanup-worker",
				Concurrency: 1,
				Lease:       time.Second,
				Poll:        10 * time.Millisecond,
				Engine:      &engine.Engine{Store: store},
				ResumeEvaluationEnvironment: func(ctx context.Context, _ *durable.Run) (engine.EvaluationEnvironment, error) {
					current, err := store.GetGoal(ctx, "owner", goal.ID)
					observed <- observation{goal: current, err: err}
					return nil, errors.New("original sandbox is unavailable")
				},
			}
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			var result observation
			select {
			case result = <-observed:
			case <-ctx.Done():
				t.Error("replacement did not reach environment cleanup")
			}
			cancel()
			if err = <-done; !errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("worker exit: %v", err)
			}
			if result.err != nil || result.goal == nil {
				t.Fatalf("load goal during cleanup: %+v", result)
			}
			if result.goal.Status != test.goal || result.goal.StallCount != test.stalls {
				t.Fatalf("stored receipt was not applied before cleanup: goal=%+v", result.goal)
			}
			receipt, err := store.GetEvaluation(context.Background(), "owner", goal.ID, run.ID)
			if err != nil || receipt.Status != test.status || receipt.Output != "stored verdict" {
				t.Fatalf("stored receipt changed: %+v %v", receipt, err)
			}
		})
	}
}

func TestEvaluationRecoversAfterWorkerKilled(t *testing.T) {
	store, dsn := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run := submit(t, store, "acceptance", durable.ReadOnly)
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID: "owner", SessionID: run.SessionID, RequestKey: "recover-goal",
		RequestHash: "recover-goal", Objective: "accept the result",
		Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	original, err := store.Claim(ctx, "execution-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	env, err := store.PrepareEnvironment(ctx, original.Lease(), "recover-env", "coding", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ActivateEnvironment(
		ctx,
		original.Lease(),
		env.RequestKey,
		"sb-recovery",
		"lease-recovery",
		1,
	); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitBoundary(ctx, original.Lease(), durable.Boundary{
		Kind: durable.BoundaryComplete, State: json.RawMessage(`{}`), Output: "done",
	}); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	released := make(chan struct{}, 1)
	var requests, dispatches atomic.Int32
	zero := 0
	operation := sdk.SandboxOperation{
		ID: "op-recovery", SandboxID: "sb-recovery", LeaseID: "lease-recovery",
		RequestKey: "eval-sb-recovery", Kind: "command", Status: sdk.SandboxOpSucceeded,
		ExitCode: &zero, Output: "passed",
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/sandboxes/sb-recovery":
			_ = json.NewEncoder(writer).Encode(sdk.SandboxView{
				Sandbox: &sdk.Sandbox{
					ID:         "sb-recovery",
					OwnerID:    "owner",
					Status:     sdk.SandboxStatusActive,
					LeaseID:    "lease-recovery",
					Generation: 1,
				},
				Lease: &sdk.SandboxLease{
					ID:                "lease-recovery",
					SandboxID:         "sb-recovery",
					SandboxGeneration: 1,
					HolderRunID:       run.ID,
					HolderRunEpoch:    original.Epoch,
					WorkerID:          "execution-worker",
					Revision:          1,
					Status:            "Active",
					HardExpiresAt:     time.Now().Add(3 * time.Second),
				},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes/sb-recovery/renew":
			var input sdk.RenewSandboxRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			_ = json.NewEncoder(writer).
				Encode(sdk.SandboxLease{ID: "lease-recovery", Revision: input.Revision, Status: "Active", HardExpiresAt: time.Now().Add(3 * time.Second)})
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sandboxes/sb-recovery/operations":
			var input sdk.ExecSandboxRequest
			if err := json.NewDecoder(request.Body).
				Decode(&input); err != nil || input.RequestKey != operation.RequestKey ||
				input.Script != "true" {
				t.Errorf("unexpected evaluation request: %+v, %v", input, err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if requests.Add(1) == 1 {
				dispatches.Add(1)
				close(entered)
				<-request.Context().Done()
				return
			}
			_ = json.NewEncoder(writer).Encode(operation)
		case request.Method == http.MethodGet && request.URL.Path == "/v1/sandboxes/sb-recovery/operations/op-recovery":
			_ = json.NewEncoder(writer).Encode(operation)
		case request.Method == http.MethodDelete && request.URL.Path == "/v1/sandboxes/sb-recovery":
			receipt, receiptErr := store.GetEvaluation(request.Context(), "owner", goal.ID, run.ID)
			current, goalErr := store.GetGoal(request.Context(), "owner", goal.ID)
			if receiptErr != nil || receipt.Status != durable.EvaluationPassed || goalErr != nil ||
				current.Status != durable.GoalComplete {
				t.Errorf(
					"release before persisted result: receipt=%+v goal=%+v errors=%v/%v",
					receipt,
					current,
					receiptErr,
					goalErr,
				)
			}
			_, _ = fmt.Fprint(writer, `{"id":"sb-recovery","status":"Destroyed"}`)
			released <- struct{}{}
		default:
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, binary, "-test.run=^TestEvaluationRecoveryChild$")
	child.Env = append(os.Environ(), "EVALUATION_RECOVERY_CHILD=1", "EVALUATION_RECOVERY_DSN="+dsn,
		"EVALUATION_RECOVERY_RUN="+run.ID, "EVALUATION_RECOVERY_URL="+server.URL)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("original evaluation did not reach sandbox")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if _, err = store.GetEvaluation(ctx, "owner", goal.ID, run.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("receipt unexpectedly present after crash: %v", err)
	}
	client, err := sdk.NewClient(server.URL, "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	worker := engine.Worker{
		ID:          "worker-after-crash",
		Concurrency: 1,
		Lease:       400 * time.Millisecond,
		Poll:        10 * time.Millisecond,
		Engine:      &engine.Engine{Store: store},
		Evaluator: engine.SandboxEvaluator{
			Store:  store,
			Runner: engine.HTTPSandboxRunner{Client: client.Sandboxes},
		},
		ResumeEvaluationEnvironment: func(ctx context.Context, run *durable.Run) (engine.EvaluationEnvironment, error) {
			return sandboxAdapter.ResumeEvaluation(ctx, store, client.Sandboxes, run)
		},
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("replacement did not finish evaluation and release")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker exit: %v", err)
	}
	if dispatches.Load() != 1 || requests.Load() != 2 {
		t.Fatalf(
			"requests=%d dispatches=%d, want replay without redispatch",
			requests.Load(),
			dispatches.Load(),
		)
	}
	if _, err = store.ActiveEnvironment(context.Background(), run.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("environment not released: %v", err)
	}
}
