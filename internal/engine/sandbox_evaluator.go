package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	serrors "github.com/FlyTOmeLight/server-go/errors"
)

// SandboxRunner executes one command inside the sandbox bound to the run and
// returns the raw output. Implementations must enforce sandbox isolation;
// the worker host never interprets goal-supplied commands.
type SandboxRunner interface {
	RunCommand(ctx context.Context, sandboxID, command string) ([]byte, error)
}

// EnvironmentStore resolves the active sandbox binding for a run.
type EnvironmentStore interface {
	ActiveEnvironment(ctx context.Context, runID string) (*durable.RunEnvironment, error)
}

// SandboxEvaluator accepts a goal by running its deterministic command in the
// run's sandbox. Exit code 0 maps to Passed, any other exit to Failed, and
// dispatch/infrastructure failures to Error.
type SandboxEvaluator struct {
	Runner SandboxRunner
	Store  EnvironmentStore
}

type CommandExitError struct {
	Code int
}

func (e *CommandExitError) Error() string {
	return fmt.Sprintf("sandbox command exited with code %d", e.Code)
}

func (e SandboxEvaluator) Evaluate(
	ctx context.Context,
	spec durable.EvaluatorSpec,
	run *durable.Run,
) (durable.EvaluationStatus, string, error) {
	switch spec.Kind {
	case durable.EvaluatorNone:
		return durable.EvaluationPassed, "", nil
	case durable.EvaluatorCommand:
		// proceed
	default:
		return durable.EvaluationError, "", fmt.Errorf("unsupported evaluator kind %q", spec.Kind)
	}
	if e.Runner == nil {
		return durable.EvaluationError, "", ErrEvaluatorUnavailable
	}
	if e.Store == nil {
		return durable.EvaluationError, "", errors.New("environment store is required")
	}
	command := strings.TrimSpace(spec.Config)
	if command == "" {
		return durable.EvaluationError, "", errors.New("evaluator command is empty")
	}
	env, err := e.Store.ActiveEnvironment(ctx, run.ID)
	if err != nil {
		return durable.EvaluationError, "", fmt.Errorf("resolve run environment: %w", err)
	}
	if env.SandboxID == "" {
		return durable.EvaluationError, "", errors.New("run has no active sandbox")
	}
	out, err := e.Runner.RunCommand(ctx, env.SandboxID, command)
	if err == nil {
		return durable.EvaluationPassed, string(out), nil
	}
	var exitErr *CommandExitError
	if errors.As(err, &exitErr) {
		return durable.EvaluationFailed, string(out), nil
	}
	return durable.EvaluationError, string(out), err
}

// HTTPSandboxRunner calls the sandbox-center HTTP API through the public SDK.
// It reuses the durable receipt path and guard channel without importing
// internal packages.
type HTTPSandboxRunner struct {
	Client  sdk.SandboxAPI
	Timeout time.Duration
}

func (r HTTPSandboxRunner) RunCommand(
	ctx context.Context,
	sandboxID string,
	command string,
) ([]byte, error) {
	if r.Client == nil {
		return nil, errors.New("sandbox-center client is not configured")
	}
	if r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}
	req := &sdk.ExecSandboxRequest{
		ID:         sandboxID,
		RequestKey: "eval-" + sandboxID,
		Script:     command,
	}
	if claim, ok := evaluationClaim(ctx); ok {
		req.EvaluationWorkerID = claim.EvaluationClaimWorkerID
		req.EvaluationEpoch = claim.EvaluationEpoch
	}
	op, err := r.Client.Exec(ctx, req)
	if err != nil && uncertainSandboxExec(err) && ctx.Err() == nil {
		op, err = r.reconcileCommand(ctx, req, op, err)
	} else if err == nil && op != nil &&
		op.Status != sdk.SandboxOpSucceeded && op.Status != sdk.SandboxOpFailed {
		op, err = r.reconcileCommand(ctx, req, op, nil)
	}
	if err != nil {
		if op != nil {
			return extractEvalOutput(op.Output), err
		}
		return nil, err
	}
	if op == nil {
		return nil, errors.New("sandbox-center returned no operation receipt")
	}
	out := extractEvalOutput(op.Output)
	if op.Status != sdk.SandboxOpSucceeded && op.Status != sdk.SandboxOpFailed {
		return out, fmt.Errorf("sandbox-center exec: operation %s: %s", op.Status, op.Error)
	}
	if op.ExitCode == nil || *op.ExitCode < 0 {
		return out, fmt.Errorf("sandbox-center exec: missing command exit verdict: %s", op.Error)
	}
	if *op.ExitCode != 0 {
		return out, &CommandExitError{Code: *op.ExitCode}
	}
	if op.Status != sdk.SandboxOpSucceeded {
		return out, fmt.Errorf("sandbox-center exec: operation failed: %s", op.Error)
	}
	return out, nil
}

func (r HTTPSandboxRunner) reconcileCommand(
	ctx context.Context,
	req *sdk.ExecSandboxRequest,
	op *sdk.SandboxOperation,
	execErr error,
) (*sdk.SandboxOperation, error) {
	if op == nil || op.ID == "" {
		var err error
		op, err = r.Client.Exec(ctx, req)
		if err != nil {
			execErr = errors.Join(execErr, err)
			if !uncertainSandboxExec(err) || op == nil || op.ID == "" {
				return op, execErr
			}
		} else if op != nil && (op.Status == sdk.SandboxOpSucceeded || op.Status == sdk.SandboxOpFailed) {
			return op, nil
		}
	}
	if op == nil || op.ID == "" {
		return op, errors.Join(
			execErr,
			errors.New("sandbox-center reconciliation: missing original operation ID"),
		)
	}
	receipt, err := r.Client.GetOperation(ctx, &sdk.GetSandboxOperationRequest{
		ID: req.ID, OperationID: op.ID,
	})
	if err != nil {
		return op, errors.Join(execErr, fmt.Errorf("query sandbox operation %s: %w", op.ID, err))
	}
	if receipt == nil || receipt.ID != op.ID || receipt.SandboxID != req.ID ||
		receipt.RequestKey != req.RequestKey {
		return op, errors.Join(
			execErr,
			errors.New("sandbox-center reconciliation: mismatched operation receipt"),
		)
	}
	return receipt, nil
}

func uncertainSandboxExec(err error) bool {
	var apiErr *serrors.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == "OutcomeUnknown" || apiErr.Code == "ReceiptNotDurable"
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded)
}

// extractEvalOutput reduces an execd event stream to its stdout/stderr text,
// preserving plain-text outputs unchanged when no event frames are present.
func extractEvalOutput(raw string) []byte {
	var (
		buf      strings.Builder
		sawFrame bool
	)
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "data:") {
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
		}
		if !strings.HasPrefix(trimmed, "{") {
			continue
		}
		var frame struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal([]byte(trimmed), &frame) != nil || frame.Type == "" {
			continue
		}
		sawFrame = true
		if frame.Type == "stdout" || frame.Type == "stderr" {
			buf.WriteString(frame.Text)
		}
	}
	if sawFrame {
		return []byte(buf.String())
	}
	return []byte(raw)
}
