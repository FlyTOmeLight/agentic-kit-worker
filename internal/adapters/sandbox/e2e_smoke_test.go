//go:build sandboxe2e

package sandbox_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/adk/filesystem"

	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
)

// TestSandboxBackendAgainstRealCenter is the phase-3 smoke: a real PostgreSQL
// store plus the phase-2 sandbox center on a real cluster. Run it with:
//
//	TEST_DATABASE_URL=... SANDBOX_CENTER_URL=http://127.0.0.1:18084 \
//	  go test -tags sandboxe2e -run TestSandboxBackendAgainstRealCenter -v ./internal/adapters/sandbox/
func TestSandboxBackendAgainstRealCenter(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	base := os.Getenv("SANDBOX_CENTER_URL")
	if dsn == "" || base == "" {
		t.Skip("TEST_DATABASE_URL and SANDBOX_CENTER_URL are required")
	}
	store, err := durable.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	owner := "owner"
	_, run := submitChatFixture(t, store, owner, "sandbox-smoke", durable.AgentConfig{
		Model:    durable.ModelSpec{Name: "test", BaseURL: "http://localhost:11434/v1"},
		MaxTurns: 3,
	}, "smoke", 10*time.Minute, 2)
	runLease := 30 * time.Second
	claimed, err := store.Claim(ctx, "smoke-worker", runLease)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != run.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, run.ID)
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(runLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				heartbeatDone <- nil
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(heartbeatCtx, runLease/3)
				renewErr := store.Renew(renewCtx, claimed.Lease(), runLease)
				stop()
				if renewErr != nil {
					if heartbeatCtx.Err() != nil {
						heartbeatDone <- nil
						return
					}
					heartbeatDone <- renewErr
					cancel()
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		stopHeartbeat()
		if err := <-heartbeatDone; err != nil {
			t.Errorf("run heartbeat: %v", err)
		}
	})

	backend, err := sandboxAdapter.New(ctx, store, sandboxAdapter.Config{
		Endpoint:   base,
		Token:      "smoke-token",
		Workspace:  "/workspace",
		ProfileID:  "coding",
		TenantID:   "tenant-smoke",
		OwnerID:    owner,
		AcquireKey: run.ID + ":env",
		RunID:      run.ID,
		WorkerID:   claimed.WorkerID,
		Epoch:      claimed.Epoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := backend.Close(closeCtx); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})

	// The durable environment binding is visible before any command runs.
	env, err := store.ActiveEnvironment(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if env.SandboxID == "" || env.Status != durable.EnvironmentActive {
		t.Fatalf("environment %+v", env)
	}

	// A real command executes through the guard channel as uid 65534.
	execCtx := sandboxAdapter.WithOperationID(ctx, run.ID+":call-1")
	resp, err := backend.Execute(execCtx, &filesystem.ExecuteRequest{Command: "id -u; pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ExitCode == nil || *resp.ExitCode != 0 {
		t.Fatalf("execute failed: %+v", resp)
	}
	if !strings.Contains(resp.Output, "65534") || !strings.Contains(resp.Output, "/workspace") {
		t.Fatalf("unexpected output %q", resp.Output)
	}

	// Replay with the same operation id returns the receipt without rerunning.
	replay, err := backend.Execute(execCtx, &filesystem.ExecuteRequest{Command: "id -u; pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Output != resp.Output {
		t.Fatalf("replay output %q, want %q", replay.Output, resp.Output)
	}

	// File write then read round-trips through the workspace.
	writeCtx := sandboxAdapter.WithOperationID(ctx, run.ID+":call-2")
	if err = backend.Write(
		writeCtx,
		&filesystem.WriteRequest{FilePath: "smoke.txt", Content: "phase3"},
	); err != nil {
		t.Fatal(err)
	}
	readCtx := sandboxAdapter.WithOperationID(ctx, run.ID+":call-3")
	content, err := backend.Read(readCtx, &filesystem.ReadRequest{FilePath: "smoke.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content.Content, "phase3") {
		t.Fatalf("unexpected file content %q", content.Content)
	}

	for attempt := 0; attempt < 4; attempt++ {
		longCtx := sandboxAdapter.WithOperationID(ctx, fmt.Sprintf("%s:call-long-%d", run.ID, attempt))
		longResult, err := backend.Execute(longCtx, &filesystem.ExecuteRequest{
			Command: "sleep 9; printf 'renewed\\n'",
		})
		if err != nil {
			t.Fatal(err)
		}
		if longResult.ExitCode == nil || *longResult.ExitCode != 0 ||
			!strings.Contains(longResult.Output, "renewed") {
			t.Fatalf("command did not survive lease renewal: %+v", longResult)
		}
	}
	client, err := sdk.NewClient(base, "smoke-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	view, err := client.Sandboxes.Get(ctx, env.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Lease == nil || view.Lease.Revision <= 1 {
		t.Fatalf("sandbox lease was not renewed: %+v", view.Lease)
	}

	// Close releases the sandbox and marks the environment released.
	if err = backend.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ActiveEnvironment(ctx, run.ID); err == nil {
		t.Fatal("environment must be released after close")
	}
}
