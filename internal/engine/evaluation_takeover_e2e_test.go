package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/FlyTOmeLight/agentic-kit-api/testutil/sandboxe2e"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// TestEvaluationTakeoverChild is the original worker in the takeover e2e: it
// completes the run with a real sandbox, claims the evaluation and starts a
// long acceptance command. The parent kills it mid-evaluation.
func TestEvaluationTakeoverChild(t *testing.T) {
	if os.Getenv("EVALUATION_TAKEOVER_CHILD") != "1" {
		t.Skip("child process helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	store, err := durable.Open(ctx, os.Getenv("EVALUATION_TAKEOVER_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runID := os.Getenv("EVALUATION_TAKEOVER_RUN")
	claimed, err := store.Claim(ctx, "worker-before-kill", 2*time.Minute)
	if err != nil || claimed.ID != runID {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	backend, err := sandboxAdapter.New(ctx, store, sandboxAdapter.Config{
		Endpoint:   os.Getenv("EVALUATION_TAKEOVER_URL"),
		Token:      "local-e2e",
		Workspace:  "/workspace",
		ProfileID:  "coding",
		OwnerID:    claimed.OwnerID,
		AcquireKey: claimed.ID + ":env:1",
		RunID:      claimed.ID,
		WorkerID:   claimed.WorkerID,
		Epoch:      claimed.Epoch,
		// A goal-bound run always requests a persistent workspace.
		RequiredSave: true,
	})
	if err != nil {
		t.Fatalf("acquire real sandbox: %v", err)
	}
	defer backend.Detach()
	if err = store.CommitBoundary(ctx, claimed.Lease(), durable.Boundary{
		Kind: durable.BoundaryComplete, State: json.RawMessage(`{}`), Output: "done",
	}); err != nil {
		t.Fatal(err)
	}
	goal, err := store.ClaimEvaluation(ctx, "worker-before-kill", 90*time.Second, runID)
	if err != nil {
		t.Fatal(err)
	}
	client, err := sdk.NewClient(os.Getenv("EVALUATION_TAKEOVER_URL"), "local-e2e", nil)
	if err != nil {
		t.Fatal(err)
	}
	evaluator := engine.SandboxEvaluator{Store: store, Runner: engine.HTTPSandboxRunner{Client: client.Sandboxes}}
	status, output, err := evaluator.Evaluate(
		engine.WithEvaluationClaim(ctx, goal),
		durable.EvaluatorSpec{
			Kind:   durable.EvaluatorCommand,
			Config: os.Getenv("EVALUATION_TAKEOVER_SCRIPT"),
		},
		goal,
	)
	if err != nil || status != durable.EvaluationPassed || !strings.Contains(output, "takeover-ok") {
		t.Fatalf("original evaluation: status=%s output=%q err=%v", status, output, err)
	}
	if err := os.WriteFile(os.Getenv("EVALUATION_TAKEOVER_READY"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	t.Fatal("parent did not kill the original worker before the barrier deadline")
}

// TestEvaluationTakeoverRealCluster runs the full loop against a real
// Kubernetes cluster: the original worker is SIGKILLed mid-evaluation and a
// replacement claims the fenced lease, replays the durable operation without
// re-dispatching, records the verdict and saves the workspace.
//
//	TEST_DATABASE_URL=... SANDBOX_E2E_K8S_CONTEXT=orbstack \
//	  go test -count=1 -v -timeout 10m -run '^TestEvaluationTakeoverRealCluster$' internal/engine/evaluation_takeover_e2e_test.go
func TestEvaluationTakeoverRealCluster(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" || os.Getenv("SANDBOX_E2E_K8S_CONTEXT") == "" {
		t.Skip("TEST_DATABASE_URL and SANDBOX_E2E_K8S_CONTEXT are required")
	}
	store, schemaDSN := takeoverTestDB(t)
	cluster := sandboxe2e.StartOnSchema(t, schemaDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	run := submitTakeoverRun(t, store)
	script := "printf 'run\\n' >> /workspace/takeover-attempts && echo takeover-ok"
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID: "owner", SessionID: run.SessionID, RequestKey: "takeover-goal",
		RequestHash: "takeover-goal", Objective: "accept on the real cluster",
		Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: script},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "evaluation-ready")
	child := exec.CommandContext(ctx, binary, "-test.run=^TestEvaluationTakeoverChild$", "-test.v")
	child.Env = append(os.Environ(),
		"EVALUATION_TAKEOVER_CHILD=1",
		"EVALUATION_TAKEOVER_DSN="+schemaDSN,
		"EVALUATION_TAKEOVER_RUN="+run.ID,
		"EVALUATION_TAKEOVER_URL="+cluster.URL,
		"EVALUATION_TAKEOVER_SCRIPT="+script,
		"EVALUATION_TAKEOVER_READY="+readyPath,
	)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	childWaited := false
	defer func() {
		if !childWaited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()

	deadline := time.Now().Add(5 * time.Minute)
	var sandboxID string
	for sandboxID == "" {
		if time.Now().After(deadline) {
			t.Fatal("original worker did not activate a sandbox in time")
		}
		env, envErr := store.ActiveEnvironment(ctx, run.ID)
		if envErr == nil && env.SandboxID != "" {
			sandboxID = env.SandboxID
			continue
		}
		if envErr != nil && !errors.Is(envErr, durable.ErrNotFound) {
			t.Fatal(envErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
	cluster.CleanupSandbox(t, sandboxID)
	podName := "sandbox-" + sandboxID
	if err = cluster.WaitPodPhase(ctx, podName, "Running"); err != nil {
		t.Fatal(err)
	}

	succeeded := false
	for !succeeded {
		if time.Now().After(deadline) {
			t.Fatal("acceptance command did not reach a terminal state")
		}
		var status string
		if err := cluster.Store.DB().WithContext(ctx).
			Raw("SELECT status FROM sandbox_operation WHERE sandbox_id = ? AND request_key = ?",
				sandboxID, "eval-"+sandboxID).
			Scan(&status).Error; err != nil {
			t.Fatal(err)
		}
		if status == "Succeeded" {
			succeeded = true
			continue
		}
		if status == "Failed" || status == "Unknown" {
			t.Fatalf("acceptance command reached %s before takeover", status)
		}
		time.Sleep(300 * time.Millisecond)
	}

	for {
		if _, readyErr := os.Stat(readyPath); readyErr == nil {
			break
		} else if !errors.Is(readyErr, os.ErrNotExist) {
			t.Fatal(readyErr)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("original worker did not reach the pre-commit barrier")
		}
	}
	if _, err := store.GetEvaluation(ctx, "owner", goal.ID, run.ID); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("evaluation must be uncommitted before SIGKILL: %v", err)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := child.Wait()
	childWaited = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("original worker did not exit from SIGKILL: %v", waitErr)
	}
	waitStatus, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("original worker exit status: %v", exitErr)
	}

	// The original lease must be fenced before any takeover.
	fenced := false
	for !fenced {
		if time.Now().After(deadline) {
			t.Fatal("evaluation lease was not fenced after the kill")
		}
		current, getErr := store.Get(ctx, "owner", run.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.EvaluationLeaseUntil == nil || !current.EvaluationLeaseUntil.After(time.Now()) {
			fenced = true
			continue
		}
		time.Sleep(200 * time.Millisecond)
	}

	client, err := sdk.NewClient(cluster.URL, "local-e2e", nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := engine.Worker{
		ID:          "worker-after-kill",
		Concurrency: 1,
		Lease:       time.Second,
		Poll:        50 * time.Millisecond,
		Engine:      &engine.Engine{Store: store},
		Evaluator: engine.SandboxEvaluator{Store: store, Runner: takeoverCommandRunner(
			func(ctx context.Context, sandboxID, command string) ([]byte, error) {
				output, err := (engine.HTTPSandboxRunner{Client: client.Sandboxes}).RunCommand(
					ctx,
					sandboxID,
					command,
				)
				if err != nil {
					return output, err
				}
				count, err := cluster.Kubectl(ctx, "exec", "sandbox-"+sandboxID, "-c", "guard", "--",
					"setpriv", "--reuid=65534", "--regid=65534", "--clear-groups",
					"sh", "-c", "wc -l < /workspace/takeover-attempts")
				if err != nil || strings.TrimSpace(count) != "1" {
					return output, fmt.Errorf(
						"acceptance side effects: count=%q err=%v",
						count,
						err,
					)
				}
				return output, nil
			},
		)},
		ResumeEvaluationEnvironment: func(ctx context.Context, run *durable.Run) (engine.EvaluationEnvironment, error) {
			return sandboxAdapter.ResumeEvaluation(ctx, store, client.Sandboxes, run)
		},
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	done := make(chan error, 1)
	go func() { done <- worker.Run(workerCtx) }()

	var finished *durable.Goal
	for finished == nil {
		if time.Now().After(deadline) {
			t.Fatal("replacement worker did not finish the evaluation")
		}
		current, goalErr := store.GetGoal(ctx, "owner", goal.ID)
		if goalErr != nil {
			t.Fatal(goalErr)
		}
		if current.Status == durable.GoalComplete {
			finished = current
			continue
		}
		receipt, receiptErr := store.GetEvaluation(ctx, "owner", goal.ID, run.ID)
		if receiptErr != nil && !errors.Is(receiptErr, durable.ErrNotFound) {
			t.Fatal(receiptErr)
		}
		if receiptErr == nil && receipt.Status == durable.EvaluationError {
			t.Fatalf("replacement evaluation failed: %s", receipt.Output)
		}
		time.Sleep(500 * time.Millisecond)
	}
	stopWorker()
	if runErr := <-done; !errors.Is(runErr, context.Canceled) {
		t.Fatalf("worker exit: %v", runErr)
	}

	receipt, err := store.GetEvaluation(ctx, "owner", goal.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != durable.EvaluationPassed || !strings.Contains(receipt.Output, "takeover-ok") {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}

	var opCount int64
	if err = cluster.Store.DB().WithContext(ctx).
		Raw("SELECT count(*) FROM sandbox_operation WHERE sandbox_id = ?", sandboxID).
		Scan(&opCount).Error; err != nil {
		t.Fatal(err)
	}
	if opCount != 1 {
		t.Fatalf("operations=%d, want exactly one replayed operation", opCount)
	}

	// RequiredSave kept the pod for the saver; the released environment must
	// carry the checkpoint the center wrote back. The reconciler drives this
	// asynchronously, so wait for it instead of sampling once.
	var checkpoint string
	for checkpoint == "" {
		if time.Now().After(deadline) {
			envs, _ := store.UnfinishedEnvironments(ctx, run.ID)
			var sbStatus string
			_ = cluster.Store.DB().WithContext(ctx).
				Raw("SELECT status || '/' || saved_checkpoint FROM sandbox WHERE id = ?", sandboxID).
				Scan(&sbStatus).Error
			t.Fatalf("workspace was never saved: sandbox=%s unfinished=%+v", sbStatus, envs)
		}
		if err = cluster.Store.DB().WithContext(ctx).
			Raw("SELECT saved_checkpoint FROM durable_run_environment WHERE run_id = ? AND sandbox_id = ?",
				run.ID, sandboxID).
			Scan(&checkpoint).Error; err != nil {
			t.Fatal(err)
		}
		if checkpoint == "" {
			time.Sleep(500 * time.Millisecond)
		}
	}
	saved, err := store.ActiveEnvironment(ctx, run.ID)
	if !errors.Is(err, durable.ErrNotFound) || saved != nil {
		t.Fatalf("environment should be fully released: %+v %v", saved, err)
	}
	envs, err := store.UnfinishedEnvironments(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 0 {
		t.Fatalf("unfinished environments: %+v", envs)
	}
	var persisted struct {
		EnvironmentStatus string
		SandboxStatus     string
		SavedCheckpoint   string
	}
	if err := cluster.Store.DB().WithContext(ctx).Raw(
		`SELECT env.status AS environment_status, sb.status AS sandbox_status, sb.saved_checkpoint
		 FROM durable_run_environment env JOIN sandbox sb ON sb.id = env.sandbox_id
		 WHERE env.run_id = ? AND env.sandbox_id = ?`, run.ID, sandboxID).Scan(&persisted).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.EnvironmentStatus != "Released" || persisted.SandboxStatus != "Destroyed" ||
		persisted.SavedCheckpoint != checkpoint {
		t.Fatalf("save and release disagree: environment checkpoint=%q persisted=%+v", checkpoint, persisted)
	}
	for {
		resources, err := cluster.Kubectl(ctx, "get", "pod/sandbox-"+sandboxID,
			"pod/saver-"+sandboxID, "pod/digest-"+sandboxID, "pvc/workspace-"+sandboxID,
			"--ignore-not-found", "-o", "name")
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(resources) == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("center left Kubernetes resources before fixture cleanup: %s", resources)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Logf("goal=%s receipt=%s checkpoint=%s", finished.ID, receipt.Status, checkpoint)
}

type takeoverCommandRunner func(context.Context, string, string) ([]byte, error)

func (runner takeoverCommandRunner) RunCommand(ctx context.Context, sandboxID, command string) ([]byte, error) {
	return runner(ctx, sandboxID, command)
}

// submitTakeoverRun builds the agent/session/run rows directly. The public
// CreateAgent/PublishAgent/Submit helpers are mid-refactor on the API branch
// and not available; the takeover behavior only needs a queued run.
func submitTakeoverRun(t *testing.T, store *durable.Store) *durable.Run {
	t.Helper()
	ctx := context.Background()
	db := store.DB()
	agentID, sessionID, runID := durable.NewID(), durable.NewID(), durable.NewID()
	if err := db.WithContext(ctx).Exec(
		`INSERT INTO agent (id, tenant_id, project_id, owner_id, name, draft, revision, published_revision, published_version, created_at, updated_at)
		 VALUES (?, 'tenant', '', 'owner', 'takeover', '{}', 1, 1, 1, clock_timestamp(), clock_timestamp())`,
		agentID,
	).
		Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Exec(
		`INSERT INTO agent_version (agent_id, number, artifact_version_id, artifact_version_uri, created_at)
		 VALUES (?, 1, 'av-1', 'artifact://av-1', clock_timestamp())`,
		agentID,
	).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Exec(
		`INSERT INTO agent_session (id, tenant_id, project_id, owner_id, agent_id, title, created_at, updated_at)
		 VALUES (?, 'tenant', '', 'owner', ?, 'takeover', clock_timestamp(), clock_timestamp())`,
		sessionID, agentID,
	).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Exec(
		`INSERT INTO durable_run
		 (id, tenant_id, project_id, owner_id, session_id, request_key, request_hash, spec, state, status,
		  deadline, max_attempts)
		 VALUES (?, 'tenant', '', 'owner', ?, ?, 'hash', '{}', '{}', 'Queued',
		         clock_timestamp() + interval '1 hour', 1)`,
		runID, sessionID, "takeover-"+runID,
	).Error; err != nil {
		t.Fatal(err)
	}
	run, err := store.Get(ctx, "owner", runID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// takeoverTestDB is the isolated-schema fixture from engine_test.go, kept
// local so this test still compiles while the shared helpers are being
// refactored on the API branch.
func takeoverTestDB(t *testing.T) (*durable.Store, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	name := "takeover_" + durable.NewID()
	if err = admin.WithContext(ctx).Exec("CREATE SCHEMA " + name).Error; err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	store, err := durable.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		if err := admin.WithContext(ctx).Exec("DROP SCHEMA " + name + " CASCADE").Error; err != nil {
			t.Error(err)
		}
		pool, _ := admin.DB()
		_ = pool.Close()
	})
	return store, u.String()
}
