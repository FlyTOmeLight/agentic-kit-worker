package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/adk/filesystem"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type evaluationClient struct {
	sdk.SandboxAPI
	get     func(context.Context, string) (*sdk.SandboxView, error)
	renew   func(context.Context, *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error)
	release func(context.Context, string) (*sdk.Sandbox, error)
}

func (client evaluationClient) Get(ctx context.Context, id string) (*sdk.SandboxView, error) {
	return client.get(ctx, id)
}

func (client evaluationClient) Renew(ctx context.Context, req *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
	return client.renew(ctx, req)
}

func (client evaluationClient) Release(ctx context.Context, id string) (*sdk.Sandbox, error) {
	return client.release(ctx, id)
}

func evaluationStore(t *testing.T) *durable.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	schemaName := "eval_attach_" + durable.NewID()
	if err = admin.Exec("CREATE SCHEMA " + schemaName).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP SCHEMA " + schemaName + " CASCADE").Error; err != nil {
			t.Error(err)
		}
	})
	address, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := address.Query()
	query.Set("search_path", schemaName)
	address.RawQuery = query.Encode()
	store, err := durable.Open(context.Background(), address.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err = store.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

func evaluationEnvironment(t *testing.T, store *durable.Store, active bool) (*durable.Run, *durable.RunEnvironment) {
	t.Helper()
	ctx := context.Background()
	session, run := submitChatFixture(t, store, "owner", "evaluation-attachment", durable.AgentConfig{
		Model: durable.ModelSpec{Name: "test", BaseURL: "http://localhost/v1"}, MaxTurns: 2,
	}, "work", time.Hour, 2)
	run, err := store.Claim(ctx, "original-executor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	env, err := store.PrepareEnvironment(ctx, run.Lease(), run.ID+":env:original", "coding", 0)
	if err != nil {
		t.Fatal(err)
	}
	if active {
		env, err = store.ActivateEnvironment(
			ctx,
			run.Lease(),
			env.RequestKey,
			"sandbox-"+run.ID,
			"lease-"+run.ID,
			3,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID: "owner", SessionID: session.ID, RequestKey: durable.NewID(), Objective: "accept work",
		Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitBoundary(ctx, run.Lease(), durable.Boundary{
		Kind: durable.BoundaryComplete, State: []byte(`{}`), Output: "done",
	}); err != nil {
		t.Fatal(err)
	}
	run, err = store.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return run, env
}

func evaluationView(run *durable.Run, env *durable.RunEnvironment) *sdk.SandboxView {
	return &sdk.SandboxView{
		Sandbox: &sdk.Sandbox{
			ID:         env.SandboxID,
			LeaseID:    env.LeaseID,
			Generation: env.Generation,
			Status:     sdk.SandboxStatusActive,
		},
		Lease: &sdk.SandboxLease{
			ID: env.LeaseID, SandboxID: env.SandboxID, SandboxGeneration: env.Generation,
			HolderRunID: run.ID, HolderRunEpoch: run.Epoch, WorkerID: "original-executor",
			Status: "Active", Revision: 7, HardExpiresAt: time.Now().Add(time.Minute),
		},
	}
}

func TestResumeEvaluationRequiresCompletedRun(t *testing.T) {
	for _, run := range []*durable.Run{nil, {}, {ID: "run", Status: durable.Running}} {
		if backend, err := ResumeEvaluation(
			context.Background(),
			&durable.Store{},
			evaluationClient{},
			run,
		); err == nil ||
			backend != nil {
			t.Fatalf("run=%+v backend=%+v err=%v", run, backend, err)
		}
	}
	completed := &durable.Run{ID: "run", Status: durable.Completed}
	if _, err := ResumeEvaluation(context.Background(), nil, evaluationClient{}, completed); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := ResumeEvaluation(context.Background(), &durable.Store{}, nil, completed); err == nil {
		t.Fatal("nil client accepted")
	}
}

func TestResumeEvaluationRejectsInvalidBindings(t *testing.T) {
	store := evaluationStore(t)
	run, env := evaluationEnvironment(t, store, true)
	cases := []struct {
		name   string
		mutate func(*sdk.SandboxView)
	}{
		{name: "missing sandbox", mutate: func(view *sdk.SandboxView) { view.Sandbox = nil }},
		{name: "missing lease", mutate: func(view *sdk.SandboxView) { view.Lease = nil }},
		{
			name:   "inactive sandbox",
			mutate: func(view *sdk.SandboxView) { view.Sandbox.Status = sdk.SandboxStatusLost },
		},
		{name: "sandbox id", mutate: func(view *sdk.SandboxView) { view.Sandbox.ID = "other" }},
		{name: "sandbox lease", mutate: func(view *sdk.SandboxView) { view.Sandbox.LeaseID = "other" }},
		{name: "sandbox generation", mutate: func(view *sdk.SandboxView) { view.Sandbox.Generation++ }},
		{name: "inactive lease", mutate: func(view *sdk.SandboxView) { view.Lease.Status = "Released" }},
		{name: "lease id", mutate: func(view *sdk.SandboxView) { view.Lease.ID = "other" }},
		{name: "lease sandbox", mutate: func(view *sdk.SandboxView) { view.Lease.SandboxID = "other" }},
		{name: "lease generation", mutate: func(view *sdk.SandboxView) { view.Lease.SandboxGeneration++ }},
		{name: "holder run", mutate: func(view *sdk.SandboxView) { view.Lease.HolderRunID = "other" }},
		{name: "holder epoch", mutate: func(view *sdk.SandboxView) { view.Lease.HolderRunEpoch++ }},
		{name: "missing original worker", mutate: func(view *sdk.SandboxView) { view.Lease.WorkerID = "" }},
		{
			name:   "different original worker",
			mutate: func(view *sdk.SandboxView) { view.Lease.WorkerID = "other-executor" },
		},
		{name: "invalid revision", mutate: func(view *sdk.SandboxView) { view.Lease.Revision = 0 }},
		{
			name:   "expired",
			mutate: func(view *sdk.SandboxView) { view.Lease.HardExpiresAt = time.Now().Add(-time.Second) },
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			view := evaluationView(run, env)
			test.mutate(view)
			client := evaluationClient{get: func(context.Context, string) (*sdk.SandboxView, error) {
				return view, nil
			}}
			backend, err := ResumeEvaluation(context.Background(), store, client, run)
			if err == nil {
				backend.Detach()
				t.Fatal("invalid binding accepted")
			}
			if backend != nil {
				t.Fatal("rejected binding returned a backend")
			}
		})
	}
	t.Run("get failure", func(t *testing.T) {
		failure := errors.New("center unavailable")
		client := evaluationClient{get: func(context.Context, string) (*sdk.SandboxView, error) {
			return nil, failure
		}}
		if _, err := ResumeEvaluation(context.Background(), store, client, run); !errors.Is(err, failure) {
			t.Fatalf("get error lost: %v", err)
		}
	})
	t.Run("nil view", func(t *testing.T) {
		client := evaluationClient{get: func(context.Context, string) (*sdk.SandboxView, error) {
			return nil, nil
		}}
		if _, err := ResumeEvaluation(context.Background(), store, client, run); err == nil {
			t.Fatal("nil view accepted")
		}
	})
	t.Run("environment epoch", func(t *testing.T) {
		staleRun := *run
		staleRun.Epoch++
		if _, err := ResumeEvaluation(context.Background(), store, evaluationClient{}, &staleRun); err == nil {
			t.Fatal("mismatched environment epoch accepted")
		}
	})
	t.Run("preparing environment", func(t *testing.T) {
		preparingRun, _ := evaluationEnvironment(t, store, false)
		if _, err := ResumeEvaluation(
			context.Background(),
			store,
			evaluationClient{},
			preparingRun,
		); err == nil {
			t.Fatal("preparing environment accepted")
		}
	})
	if err := store.MarkEnvironmentReleased(context.Background(), env.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := ResumeEvaluation(
		context.Background(),
		store,
		evaluationClient{},
		run,
	); !errors.Is(
		err,
		durable.ErrNotFound,
	) {
		t.Fatalf("released environment error = %v", err)
	}
}

func TestResumeEvaluationRenewsDetachesAndCloses(t *testing.T) {
	store := evaluationStore(t)
	run, env := evaluationEnvironment(t, store, true)
	run.EvaluationClaimWorkerID = "new-evaluation-claimant"
	view := evaluationView(run, env)
	view.Lease.HardExpiresAt = time.Now().Add(900 * time.Millisecond)
	acceptedDeadline := time.Now().Add(time.Minute)
	renewed := make(chan sdk.RenewSandboxRequest, 1)
	renewErrors := make(chan error, 1)
	releases := 0
	client := evaluationClient{
		get: func(_ context.Context, id string) (*sdk.SandboxView, error) {
			if id != env.SandboxID {
				t.Errorf("get sandbox = %q", id)
			}
			return view, nil
		},
		renew: func(ctx context.Context, req *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
			if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(view.Lease.HardExpiresAt) {
				renewErrors <- fmt.Errorf("renew deadline = %v, want %v", deadline, view.Lease.HardExpiresAt)
			}
			renewed <- *req
			lease := *view.Lease
			lease.Revision = req.Revision
			lease.HardExpiresAt = acceptedDeadline
			return &lease, nil
		},
		release: func(_ context.Context, id string) (*sdk.Sandbox, error) {
			releases++
			if id != env.SandboxID {
				t.Errorf("release sandbox = %q", id)
			}
			current, err := store.ActiveEnvironment(context.Background(), run.ID)
			if err != nil || current.ID != env.ID {
				t.Errorf("environment marked before release: env=%+v err=%v", current, err)
			}
			return &sdk.Sandbox{ID: id, Status: sdk.SandboxStatusDestroyed}, nil
		},
	}
	backend, err := ResumeEvaluation(context.Background(), store, client, run)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Detach)
	if backend.cfg.AcquireKey != env.RequestKey || backend.cfg.WorkerID != "original-executor" {
		t.Fatalf("original identity changed: %+v", backend.cfg)
	}
	if _, err := backend.Execute(
		WithOperationID(context.Background(), "tool"),
		&filesystem.ExecuteRequest{Command: "true"},
	); err == nil {
		t.Fatal("evaluation attachment dispatched a tool")
	}
	select {
	case req := <-renewed:
		if req.ID != env.SandboxID || req.Revision != 8 || req.RequestKey != env.RequestKey+":renew:8" {
			t.Fatalf("renew request = %+v", req)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("evaluation attachment did not renew")
	}
	backend.Detach()
	select {
	case err := <-renewErrors:
		t.Error(err)
	default:
	}
	if backend.revision != 8 || !backend.expiresAt.Equal(acceptedDeadline) || backend.Released() || releases != 0 {
		t.Fatalf(
			"detach lost renewal or released sandbox: revision=%d expires=%v releases=%d",
			backend.revision,
			backend.expiresAt,
			releases,
		)
	}
	current, err := store.ActiveEnvironment(context.Background(), run.ID)
	if err != nil || *current != *env {
		t.Fatalf("attachment changed environment: env=%+v err=%v", current, err)
	}
	if err = backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = backend.Close(context.Background()); err != nil || releases != 1 || !backend.Released() {
		t.Fatalf("close: err=%v releases=%d", err, releases)
	}
	current, err = store.GetEnvironment(context.Background(), run.ID, env.RequestKey)
	if err != nil || current.Status != durable.EnvironmentReleased || current.ReleasedAt == nil {
		t.Fatalf("environment not marked released: env=%+v err=%v", current, err)
	}
}

func TestResumeEvaluationFailedReleasePreservesEnvironment(t *testing.T) {
	store := evaluationStore(t)
	run, env := evaluationEnvironment(t, store, true)
	failure := errors.New("release unavailable")
	client := evaluationClient{
		get:     func(context.Context, string) (*sdk.SandboxView, error) { return evaluationView(run, env), nil },
		release: func(context.Context, string) (*sdk.Sandbox, error) { return nil, failure },
	}
	backend, err := ResumeEvaluation(context.Background(), store, client, run)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Detach)
	if err = backend.Close(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("release error lost: %v", err)
	}
	current, err := store.ActiveEnvironment(context.Background(), run.ID)
	if err != nil || current.ID != env.ID || current.Status != durable.EnvironmentActive {
		t.Fatalf("failed release changed environment: env=%+v err=%v", current, err)
	}
}

func TestResumeEvaluationClosesAfterTerminalGoalRejectsRenewal(t *testing.T) {
	store := evaluationStore(t)
	for _, status := range []durable.EvaluationStatus{durable.EvaluationPassed, durable.EvaluationFailed} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			run, env := evaluationEnvironment(t, store, true)
			if _, err := store.RecordEvaluation(
				ctx,
				run.OwnerID,
				run.GoalID,
				run.ID,
				status,
				"recorded",
			); err != nil {
				t.Fatal(err)
			}
			terminal := durable.GoalComplete
			if status == durable.EvaluationFailed {
				terminal = durable.GoalFailed
			}
			if err := store.TransitionGoal(ctx, run.OwnerID, run.GoalID, terminal); err != nil {
				t.Fatal(err)
			}
			view := evaluationView(run, env)
			view.Lease.HardExpiresAt = time.Now().Add(900 * time.Millisecond)
			renewals, releases := 0, 0
			client := evaluationClient{
				get: func(context.Context, string) (*sdk.SandboxView, error) { return view, nil },
				renew: func(context.Context, *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
					renewals++
					return nil, errors.New("evaluation authority ended with goal")
				},
				release: func(_ context.Context, id string) (*sdk.Sandbox, error) {
					releases++
					return &sdk.Sandbox{ID: id, Status: sdk.SandboxStatusDestroyed}, nil
				},
			}
			backend, err := ResumeEvaluation(ctx, store, client, run)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Detach)
			select {
			case <-backend.renewDone:
			case <-time.After(3 * time.Second):
				t.Fatal("rejected renewal did not stop")
			}
			if renewals != 1 {
				t.Fatalf("renewals = %d, want one rejected attempt", renewals)
			}
			if err = backend.Close(ctx); err != nil || releases != 1 {
				t.Fatalf("cleanup after rejected renewal: err=%v releases=%d", err, releases)
			}
			if _, err = store.ActiveEnvironment(ctx, run.ID); !errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("environment not released: %v", err)
			}
			receipt, err := store.GetEvaluation(ctx, run.OwnerID, run.GoalID, run.ID)
			if err != nil || receipt.Status != status || receipt.Output != "recorded" {
				t.Fatalf("cleanup changed evaluation: receipt=%+v err=%v", receipt, err)
			}
		})
	}
}
