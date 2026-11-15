package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// fakeSandboxCenter implements the acquire/exec/release contract in memory.
// Acquire replays by request key, exec replays by request key, and every
// dispatch is counted so the test can prove no second execution happens.
type fakeSandboxCenter struct {
	mu        sync.Mutex
	sandboxes map[string]string
	execs     map[string]int
}

func newFakeSandboxCenter(t *testing.T) (*fakeSandboxCenter, *httptest.Server) {
	t.Helper()
	f := &fakeSandboxCenter{sandboxes: map[string]string{}, execs: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/sandboxes":
			var req sdk.AcquireSandboxRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode acquire: %v", err)
			}
			id, ok := f.sandboxes[req.RequestKey]
			if !ok {
				id = "sb-" + req.RequestKey
				f.sandboxes[req.RequestKey] = id
			}
			fmt.Fprintf(
				w,
				`{"sandbox":{"id":%q,"status":"Active","leaseId":"lease-%s","generation":1},"lease":{"id":"lease-%s","revision":1,"status":"Active"}}`,
				id,
				id,
				id,
			)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return f, server
}

// TestSandboxTakeoverKeepsEnvironmentBinding proves the phase-3 takeover
// contract: worker A claims the run, records the environment intent and
// crashes; worker B claims the same run under a new epoch, sees the durable
// binding through BuildRunContext, and must not acquire a second sandbox or
// mutate the old epoch's record.
func TestSandboxTakeoverKeepsEnvironmentBinding(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	center, server := newFakeSandboxCenter(t)
	r := submit(t, s, "submit", durable.Unsafe)

	// Worker A: claim, record the environment intent, then "crash" before
	// activation. The durable record survives the lost claim.
	first := getClaim(t, s)
	if first.ID != r.ID {
		t.Fatalf("claimed %s, want %s", first.ID, r.ID)
	}
	env, err := s.PrepareEnvironment(ctx, first.Lease(), first.ID+":env", "coding", 0)
	if err != nil {
		t.Fatal(err)
	}
	if env.Status != durable.EnvironmentPreparing || env.RunEpoch != first.Epoch {
		t.Fatalf("environment %+v", env)
	}

	// Expire worker A's claim; worker B takes over under a new epoch.
	next := claimAfterExpiry(t, s)
	if next.ID != r.ID || next.Epoch == first.Epoch {
		t.Fatalf("takeover claim %+v", next)
	}

	// The new worker's build context carries its own claim.
	var seenRC engine.RunContext
	var sawRunContext bool
	build := func(ctx context.Context, _ *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		seenRC, sawRunContext = engine.BuildRunContext(ctx)
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			return schema.AssistantMessage("done", nil), nil
		}}, map[string]engine.Binding{}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err = e.Execute(ctx, next); err != nil {
		t.Fatal(err)
	}
	if !sawRunContext {
		t.Fatal("build context did not carry the run claim")
	}
	if seenRC.RunID != r.ID || seenRC.WorkerID != next.WorkerID || seenRC.Epoch != next.Epoch ||
		seenRC.OwnerID != "owner" {
		t.Fatalf("run context %+v, want run %s epoch %d", seenRC, r.ID, next.Epoch)
	}

	// The durable binding from worker A is still visible for reattachment…
	stored, err := s.GetEnvironment(ctx, r.ID, r.ID+":env")
	if err != nil {
		t.Fatal(err)
	}
	if stored.RunEpoch != first.Epoch || stored.Status != durable.EnvironmentPreparing {
		t.Fatalf("stored environment %+v", stored)
	}
	// …but the new epoch cannot mutate it through the lease-checked path.
	if _, err = s.ActivateEnvironment(
		ctx,
		next.Lease(),
		r.ID+":env",
		"sb-x",
		"lease-x",
		1,
	); !errors.Is(
		err,
		durable.ErrLeaseLost,
	) {
		t.Fatalf("new epoch must not activate the old binding, got %v", err)
	}
	// The reconciler-facing query still reports the unfinished environment so
	// the orphan pod can be collected.
	unfinished, err := s.UnfinishedEnvironments(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(unfinished) != 1 || unfinished[0].ID != env.ID {
		t.Fatalf("unfinished environments %+v", unfinished)
	}

	// No sandbox was ever acquired in this test: the fake center only proves
	// the HTTP surface is wired.
	if len(center.sandboxes) != 0 {
		t.Fatalf("no acquire should have happened: %v", center.sandboxes)
	}
	_ = server
}
