// Package sandbox adapts the agentic-kit-api sandbox center to Eino
// filesystem tools. Unlike the AIO adapter it owns the whole environment
// lifecycle: the durable intent is recorded before any network call, every
// command is dispatched under an idempotent operation key, and a lost
// response is resolved by replaying the same key rather than a blind retry.
package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/adk/filesystem"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
)

const (
	defaultProfile   = "coding"
	leaseTTLSeconds  = 300
	maxRenewInterval = 10 * time.Second
	defaultMaxOutput = 24000
)

// OutcomeUnknown means the operation may or may not have executed; the
// durable receipt is the only source of truth. The engine must surface this
// instead of retrying with a new key.
type OutcomeUnknown struct{ OperationID string }

func (e *OutcomeUnknown) Error() string {
	return "sandbox operation outcome unknown; query operation " + e.OperationID
}

// InputError means the request was rejected before any mutation.
type InputError struct{ Message string }

func (e *InputError) Error() string { return e.Message }

func (e *InputError) Correctable() bool { return true }

func invalid(message string) error { return &InputError{Message: message} }

type operationKey struct{}

// WithOperationID binds the durable runID:callID operation identity to the
// context so command execution stays idempotent across worker attempts.
func WithOperationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, operationKey{}, id)
}

func operationID(ctx context.Context) string {
	v, _ := ctx.Value(operationKey{}).(string)
	return v
}

// Config carries the trusted filesystem binding resolved for one run.
type Config struct {
	Endpoint             string
	Token                string
	Workspace            string
	ProfileID            string
	TenantID             string
	OwnerID              string
	AcquireKey           string
	RunID                string
	WorkerID             string
	Epoch                int64
	RequiredSave         bool
	RestoreFromSandboxID string
}

// Backend owns one sandbox environment for the lifetime of a run attempt.
type Backend struct {
	cfg    Config
	store  *durable.Store
	client sdk.SandboxAPI

	mu             sync.Mutex
	envID          string
	sandboxID      string
	leaseID        string
	revision       int64
	released       bool
	detached       bool
	expiresAt      time.Time
	stopRenew      context.CancelFunc
	renewDone      chan struct{}
	evaluationOnly bool
}

// New prepares the environment under the current claim. The durable intent is
// written first; a crash after the sandbox-center acquire is reconciled
// through the persisted request key on the next attempt.
func New(ctx context.Context, store *durable.Store, cfg Config) (*Backend, error) {
	if store == nil || cfg.Endpoint == "" || cfg.Token == "" || !path.IsAbs(cfg.Workspace) ||
		cfg.AcquireKey == "" || cfg.RunID == "" || cfg.WorkerID == "" {
		return nil, errors.New(
			"sandbox backend requires store, endpoint, token, workspace, acquire key and run claim",
		)
	}
	if cfg.ProfileID == "" {
		cfg.ProfileID = defaultProfile
	}
	if cfg.RestoreFromSandboxID != "" && !cfg.RequiredSave {
		return nil, errors.New("sandbox restore requires a persistent workspace")
	}
	if cfg.RequiredSave {
		workspace := path.Clean(cfg.Workspace)
		if workspace != "/workspace" && !strings.HasPrefix(workspace, "/workspace/") {
			return nil, errors.New("persistent sandbox workspace must be within /workspace")
		}
	}
	lease := durable.Lease{RunID: cfg.RunID, WorkerID: cfg.WorkerID, Epoch: cfg.Epoch}
	env, err := store.PrepareEnvironment(ctx, lease, cfg.AcquireKey, cfg.ProfileID, 0)
	if err != nil {
		return nil, fmt.Errorf("record environment intent: %w", err)
	}
	client, err := sdk.NewClient(cfg.Endpoint, cfg.Token, nil)
	if err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg, store: store, client: client.Sandboxes}
	if err = b.acquire(ctx, env); err != nil {
		return nil, err
	}
	b.startRenewal()
	return b, nil
}

func ResumeEvaluation(
	ctx context.Context,
	store *durable.Store,
	client sdk.SandboxAPI,
	run *durable.Run,
) (*Backend, error) {
	if store == nil || client == nil || run == nil || run.ID == "" || run.Status != durable.Completed {
		return nil, errors.New("sandbox evaluation requires store, client and a completed run")
	}
	env, err := store.ActiveEnvironment(ctx, run.ID)
	if err != nil {
		return nil, fmt.Errorf("load evaluation environment: %w", err)
	}
	if env.Status != durable.EnvironmentActive || env.RunEpoch != run.Epoch ||
		env.RequestKey == "" || env.SandboxID == "" || env.LeaseID == "" {
		return nil, errors.New("evaluation environment is not active under the completed run epoch")
	}
	view, err := client.Get(ctx, env.SandboxID)
	if err != nil {
		return nil, fmt.Errorf("get evaluation sandbox: %w", err)
	}
	if view == nil || view.Sandbox == nil || view.Lease == nil {
		return nil, errors.New("evaluation sandbox has no active binding")
	}
	sandbox, lease := view.Sandbox, view.Lease
	if sandbox.Status != sdk.SandboxStatusActive || sandbox.ID != env.SandboxID ||
		sandbox.LeaseID != env.LeaseID || sandbox.Generation != env.Generation ||
		lease.Status != "Active" || lease.ID != env.LeaseID || lease.SandboxID != env.SandboxID ||
		lease.SandboxGeneration != env.Generation || lease.HolderRunID != run.ID ||
		lease.HolderRunEpoch != run.Epoch || lease.WorkerID == "" ||
		lease.WorkerID != run.EvaluationWorkerID || lease.Revision < 1 ||
		!lease.HardExpiresAt.After(time.Now()) {
		return nil, errors.New("evaluation sandbox binding is mismatched or expired")
	}
	backend := &Backend{
		cfg: Config{
			AcquireKey:   env.RequestKey,
			RunID:        run.ID,
			Epoch:        run.Epoch,
			WorkerID:     lease.WorkerID,
			RequiredSave: sandbox.RequiredSave,
		},
		store:          store,
		client:         client,
		envID:          env.ID,
		sandboxID:      sandbox.ID,
		leaseID:        lease.ID,
		revision:       lease.Revision,
		expiresAt:      lease.HardExpiresAt,
		evaluationOnly: true,
	}
	backend.startRenewal()
	return backend, nil
}

func (b *Backend) startRenewal() {
	renewCtx, stopRenew := context.WithCancel(context.Background())
	b.stopRenew = stopRenew
	b.renewDone = make(chan struct{})
	go b.renewLoop(renewCtx)
}

// Close stops lease renewal and releases the sandbox. The environment record
// is only marked released after the center confirms destruction.
func (b *Backend) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.released {
		b.mu.Unlock()
		return nil
	}
	b.released = true
	sandboxID := b.sandboxID
	envID := b.envID
	b.mu.Unlock()

	b.Detach()
	if sandboxID == "" {
		return nil
	}
	sandbox, err := b.client.Release(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("release sandbox: %w", err)
	}
	if sandbox == nil || sandbox.ID != sandboxID {
		return errors.New("release sandbox returned no matching sandbox")
	}
	if sandbox.Status != sdk.SandboxStatusDestroyed || sandbox.RequiredSave || b.cfg.RequiredSave {
		return nil
	}
	if envID != "" {
		if err := b.store.MarkEnvironmentReleased(ctx, envID); err != nil {
			env, readErr := b.store.GetEnvironment(ctx, b.cfg.RunID, b.cfg.AcquireKey)
			if readErr == nil && env.ID == envID && env.Status == durable.EnvironmentReleased {
				return nil
			}
			return fmt.Errorf("mark environment released: %w", err)
		}
	}
	return nil
}

func (b *Backend) Detach() {
	b.mu.Lock()
	b.detached = true
	b.mu.Unlock()
	b.stopRenew()
	<-b.renewDone
}

// Released reports whether Close has already run, so the engine can release
// the environment when a run finishes without knowing the concrete provider.
func (b *Backend) Released() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.released
}

func (b *Backend) acquire(ctx context.Context, env *durable.RunEnvironment) error {
	// The environment request key doubles as the sandbox request key, so a
	// replayed acquire returns the same sandbox instead of leaking a new one.
	view, err := b.client.Acquire(ctx, &sdk.AcquireSandboxRequest{
		TenantID:             b.cfg.TenantID,
		OwnerID:              b.cfg.OwnerID,
		RequestKey:           env.RequestKey,
		ProfileID:            b.cfg.ProfileID,
		WorkerID:             b.cfg.WorkerID,
		RunID:                b.cfg.RunID,
		RunEpoch:             b.cfg.Epoch,
		TTLSeconds:           leaseTTLSeconds,
		RequiredSave:         b.cfg.RequiredSave,
		RestoreFromSandboxID: b.cfg.RestoreFromSandboxID,
	})
	if err != nil {
		return fmt.Errorf("acquire sandbox: %w", err)
	}
	if view == nil || view.Sandbox == nil || view.Sandbox.Status != sdk.SandboxStatusActive || view.Lease == nil ||
		!view.Lease.HardExpiresAt.After(time.Now()) {
		return errors.New("sandbox not active after acquire")
	}
	if view.Sandbox.RequiredSave != b.cfg.RequiredSave ||
		view.Sandbox.RestoreFromSandboxID != b.cfg.RestoreFromSandboxID ||
		(b.cfg.RestoreFromSandboxID != "" && view.Sandbox.ResumeCheckpoint == "") {
		return errors.New("sandbox workspace persistence binding does not match the request")
	}
	lease := durable.Lease{RunID: b.cfg.RunID, WorkerID: b.cfg.WorkerID, Epoch: b.cfg.Epoch}
	activated, err := b.store.ActivateEnvironment(
		ctx, lease, env.RequestKey, view.Sandbox.ID, view.Lease.ID, view.Sandbox.Generation,
	)
	if err != nil {
		return fmt.Errorf("persist environment binding: %w", err)
	}
	b.mu.Lock()
	b.sandboxID = view.Sandbox.ID
	b.leaseID = view.Lease.ID
	b.revision = view.Lease.Revision
	b.expiresAt = view.Lease.HardExpiresAt
	b.envID = activated.ID
	b.mu.Unlock()
	return nil
}

func (b *Backend) renewLoop(ctx context.Context) {
	defer close(b.renewDone)
	for {
		b.mu.Lock()
		expiresAt := b.expiresAt
		b.mu.Unlock()
		remaining := time.Until(expiresAt)
		if remaining <= 0 {
			return
		}
		timer := time.NewTimer(min(remaining/3, maxRenewInterval))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		b.mu.Lock()
		if b.released || b.detached || b.sandboxID == "" {
			b.mu.Unlock()
			return
		}
		sandboxID := b.sandboxID
		revision := b.revision + 1
		acquireKey := b.cfg.AcquireKey
		b.mu.Unlock()

		renewCtx, cancel := context.WithDeadline(ctx, expiresAt)
		lease, err := b.client.Renew(renewCtx, &sdk.RenewSandboxRequest{
			ID:         sandboxID,
			Revision:   revision,
			RequestKey: fmt.Sprintf("%s:renew:%d", acquireKey, revision),
			TTLSeconds: leaseTTLSeconds,
		})
		cancel()
		if err != nil || lease == nil || !lease.HardExpiresAt.After(time.Now()) {
			// The center clamps the lease at its own hard deadline; a failed
			// renew stops renewal and the sandbox closes itself at expiry.
			return
		}
		b.mu.Lock()
		b.revision = lease.Revision
		b.expiresAt = lease.HardExpiresAt
		b.mu.Unlock()
	}
}

func (b *Backend) resolve(raw string) (string, error) {
	if raw == "" {
		raw = "."
	}
	p := path.Clean(raw)
	if !path.IsAbs(p) {
		p = path.Join(b.cfg.Workspace, p)
	}
	if p != b.cfg.Workspace && !strings.HasPrefix(p, b.cfg.Workspace+"/") {
		return "", invalid("path escapes the workspace: " + raw)
	}
	return p, nil
}

// executeScript runs one shell script through the center with an idempotent
// operation key. Replaying the same key returns the stored receipt.
func (b *Backend) executeScript(ctx context.Context, kind, script string) (*sdk.SandboxOperation, error) {
	b.mu.Lock()
	if b.evaluationOnly {
		b.mu.Unlock()
		return nil, errors.New("evaluation attachment cannot dispatch tools")
	}
	if b.released || b.detached {
		b.mu.Unlock()
		return nil, errors.New("sandbox environment is no longer attached")
	}
	sandboxID := b.sandboxID
	b.mu.Unlock()

	key := operationID(ctx)
	if key == "" {
		return nil, errors.New("sandbox operation requires a durable operation id")
	}
	op, err := b.client.Exec(ctx, &sdk.ExecSandboxRequest{
		ID:         sandboxID,
		RequestKey: kind + ":" + key,
		Script:     script,
	})
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, errors.New("sandbox-center returned no operation receipt")
	}
	switch op.Status {
	case sdk.SandboxOpSucceeded, sdk.SandboxOpFailed:
		if op.ExitCode == nil || *op.ExitCode < 0 {
			return nil, fmt.Errorf("sandbox command has no exit verdict: %s", op.Error)
		}
		if op.Status == sdk.SandboxOpFailed && *op.ExitCode == 0 {
			return nil, fmt.Errorf("sandbox command failed without a non-zero exit code: %s", op.Error)
		}
		return op, nil
	default:
		return nil, &OutcomeUnknown{OperationID: op.ID}
	}
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// parseExecOutput reduces execd's event stream (init/ping/stdout/
// execution_complete frames) to plain output and an exit code.
func parseExecOutput(raw string) (string, int) {
	var stdout strings.Builder
	exitCode := 0
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var frame struct {
			Type       string `json:"type"`
			Text       string `json:"text"`
			ExitCode   *int   `json:"exitCode"`
			ExitCodeV2 *int   `json:"exit_code"`
			EName      string `json:"ename"`
			EValue     string `json:"evalue"`
			Error      *struct {
				EName      string `json:"ename"`
				EValue     string `json:"evalue"`
				ExitCode   *int   `json:"exitCode"`
				ExitCodeV2 *int   `json:"exit_code"`
			} `json:"error"`
			Status string `json:"status"`
		}
		if json.Unmarshal([]byte(line), &frame) != nil {
			continue
		}
		switch frame.Type {
		case "stdout":
			stdout.WriteString(frame.Text)
		case "execution_complete":
			if frame.ExitCode != nil {
				exitCode = *frame.ExitCode
			} else if frame.ExitCodeV2 != nil {
				exitCode = *frame.ExitCodeV2
			}
		case "error":
			ename, evalue := frame.EName, frame.EValue
			errCode := frame.ExitCode
			if errCode == nil {
				errCode = frame.ExitCodeV2
			}
			if frame.Error != nil {
				ename, evalue = frame.Error.EName, frame.Error.EValue
				if frame.Error.ExitCode != nil {
					errCode = frame.Error.ExitCode
				} else if frame.Error.ExitCodeV2 != nil {
					errCode = frame.Error.ExitCodeV2
				}
			}
			if errCode != nil {
				exitCode = *errCode
			} else if ename == "CommandExecError" {
				if parsed, err := strconv.Atoi(strings.TrimSpace(evalue)); err == nil {
					exitCode = parsed
				}
			}
		}
	}
	return stdout.String(), exitCode
}

func (b *Backend) Execute(ctx context.Context, r *filesystem.ExecuteRequest) (*filesystem.ExecuteResponse, error) {
	if r.RunInBackendGround {
		return nil, invalid("unmanaged background execution is disabled")
	}
	if strings.TrimSpace(r.Command) == "" {
		return nil, invalid("command is required")
	}
	op, err := b.executeScript(ctx, "command", r.Command)
	if err != nil {
		return nil, err
	}
	stdout, exitCode := parseExecOutput(op.Output)
	if op.ExitCode != nil {
		exitCode = *op.ExitCode
	}
	truncated := false
	if len(stdout) > defaultMaxOutput {
		stdout = stdout[:defaultMaxOutput]
		truncated = true
	}
	return &filesystem.ExecuteResponse{Output: stdout, ExitCode: &exitCode, Truncated: truncated}, nil
}

func (b *Backend) Read(ctx context.Context, r *filesystem.ReadRequest) (*filesystem.FileContent, error) {
	p, err := b.resolve(r.FilePath)
	if err != nil {
		return nil, err
	}
	script := "cat " + shellQuote(p)
	if r.Limit > 0 {
		start := r.Offset
		if start < 1 {
			start = 1
		}
		script = fmt.Sprintf(
			"sed -n %s %s",
			shellQuote(fmt.Sprintf("%d,%dp", start, start+r.Limit-1)),
			shellQuote(p),
		)
	}
	op, err := b.executeScript(ctx, "read", script)
	if err != nil {
		return nil, err
	}
	stdout, exitCode := parseExecOutput(op.Output)
	if op.ExitCode != nil {
		exitCode = *op.ExitCode
	}
	if op.Status == sdk.SandboxOpFailed || exitCode != 0 {
		return nil, fmt.Errorf("read %s failed with exit code %d: %s", p, exitCode, stdout)
	}
	return &filesystem.FileContent{Content: stdout}, nil
}

func (b *Backend) Write(ctx context.Context, r *filesystem.WriteRequest) error {
	p, err := b.resolve(r.FilePath)
	if err != nil {
		return err
	}
	if r.FilePath == "" {
		return invalid("file path is required")
	}
	script := "mkdir -p " + shellQuote(path.Dir(p)) + " && cat > " + shellQuote(p) +
		" <<'AGENTIC_SANDBOX_EOF'\n" + r.Content + "\nAGENTIC_SANDBOX_EOF"
	op, err := b.executeScript(ctx, "write", script)
	if err != nil {
		return err
	}
	stdout, exitCode := parseExecOutput(op.Output)
	if op.ExitCode != nil {
		exitCode = *op.ExitCode
	}
	if op.Status == sdk.SandboxOpFailed || exitCode != 0 {
		return fmt.Errorf("write %s failed with exit code %d: %s", p, exitCode, stdout)
	}
	return nil
}

// Edit is intentionally not implemented in the first profile: partial edits
// over a shell channel cannot be made idempotent without a file API, and the
// engine must not pretend they are.
func (b *Backend) Edit(ctx context.Context, r *filesystem.EditRequest) error {
	return invalid("sandbox edit is not supported; write the full file content")
}

func (b *Backend) LsInfo(ctx context.Context, r *filesystem.LsInfoRequest) ([]filesystem.FileInfo, error) {
	p, err := b.resolve(r.Path)
	if err != nil {
		return nil, err
	}
	script := "cd " + shellQuote(p) + " && for f in * .*; do [ \"$f\" = \".\" ] || [ \"$f\" = \"..\" ] || " +
		"{ [ -d \"$f\" ] && d=d || d=f; printf '%s\\t%s\\n' \"$d\" \"$f\"; }; done"
	op, err := b.executeScript(ctx, "ls", script)
	if err != nil {
		return nil, err
	}
	stdout, exitCode := parseExecOutput(op.Output)
	if op.ExitCode != nil {
		exitCode = *op.ExitCode
	}
	if op.Status == sdk.SandboxOpFailed || exitCode != 0 {
		return nil, fmt.Errorf("ls %s failed with exit code %d: %s", p, exitCode, stdout)
	}
	var out []filesystem.FileInfo
	for _, line := range strings.Split(stdout, "\n") {
		kind, name, ok := strings.Cut(line, "\t")
		if !ok || name == "" {
			continue
		}
		out = append(out, filesystem.FileInfo{Path: path.Join(p, name), IsDir: kind == "d"})
	}
	return out, nil
}

func (b *Backend) GlobInfo(ctx context.Context, r *filesystem.GlobInfoRequest) ([]filesystem.FileInfo, error) {
	return nil, invalid("sandbox glob is not supported")
}

func (b *Backend) GrepRaw(ctx context.Context, r *filesystem.GrepRequest) ([]filesystem.GrepMatch, error) {
	return nil, invalid("sandbox grep is not supported")
}

// Ready reports whether the acquired sandbox is still active.
func (b *Backend) Ready(ctx context.Context) error {
	b.mu.Lock()
	sandboxID := b.sandboxID
	released := b.released || b.detached
	b.mu.Unlock()
	if released || sandboxID == "" {
		return errors.New("sandbox environment is not active")
	}
	view, err := b.client.Get(ctx, sandboxID)
	if err != nil {
		return err
	}
	if view.Sandbox == nil || view.Sandbox.Status != sdk.SandboxStatusActive {
		return fmt.Errorf("sandbox %s is not active", sandboxID)
	}
	return nil
}
