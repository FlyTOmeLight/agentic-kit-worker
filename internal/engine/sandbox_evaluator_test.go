package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	serrors "github.com/FlyTOmeLight/server-go/errors"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type stubRunner struct {
	out          []byte
	err          error
	gotSandboxID string
	gotCommand   string
}

func (s *stubRunner) RunCommand(_ context.Context, sandboxID, command string) ([]byte, error) {
	s.gotSandboxID, s.gotCommand = sandboxID, command
	return s.out, s.err
}

type stubEnvStore struct {
	env *durable.RunEnvironment
	err error
}

func (s stubEnvStore) ActiveEnvironment(_ context.Context, _ string) (*durable.RunEnvironment, error) {
	return s.env, s.err
}

func TestSandboxEvaluatorNonePasses(t *testing.T) {
	ev := engine.SandboxEvaluator{}
	status, _, err := ev.Evaluate(context.Background(), durable.EvaluatorSpec{Kind: durable.EvaluatorNone}, nil)
	if err != nil || status != durable.EvaluationPassed {
		t.Fatalf("expected pass, got %s %v", status, err)
	}
}

func TestSandboxEvaluatorCommandPassed(t *testing.T) {
	runner := &stubRunner{out: []byte("ok")}
	store := stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}}
	ev := engine.SandboxEvaluator{Runner: runner, Store: store}
	run := &durable.Run{ID: "run-1"}
	status, out, err := ev.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "go test ./..."},
		run,
	)
	if err != nil || status != durable.EvaluationPassed || out != "ok" {
		t.Fatalf("expected pass with output, got %s %q %v", status, out, err)
	}
	if runner.gotSandboxID != "sb-1" || runner.gotCommand != "go test ./..." {
		t.Fatalf("unexpected runner args: %q %q", runner.gotSandboxID, runner.gotCommand)
	}
}

func TestSandboxEvaluatorCommandFailed(t *testing.T) {
	runner := &stubRunner{out: []byte("FAIL"), err: &engine.CommandExitError{Code: 1}}
	store := stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}}
	ev := engine.SandboxEvaluator{Runner: runner, Store: store}
	run := &durable.Run{ID: "run-1"}
	status, out, err := ev.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "false"},
		run,
	)
	if err != nil || status != durable.EvaluationFailed || out != "FAIL" {
		t.Fatalf("expected failed with output, got %s %q %v", status, out, err)
	}
}

func TestSandboxEvaluatorInfraError(t *testing.T) {
	runner := &stubRunner{err: errors.New("kubectl not found")}
	store := stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}}
	ev := engine.SandboxEvaluator{Runner: runner, Store: store}
	run := &durable.Run{ID: "run-1"}
	status, _, err := ev.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
		run,
	)
	if err == nil || status != durable.EvaluationError {
		t.Fatalf("expected error, got %s %v", status, err)
	}
}

func TestSandboxEvaluatorNoSandbox(t *testing.T) {
	runner := &stubRunner{}
	store := stubEnvStore{env: &durable.RunEnvironment{SandboxID: ""}}
	ev := engine.SandboxEvaluator{Runner: runner, Store: store}
	run := &durable.Run{ID: "run-1"}
	status, _, err := ev.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
		run,
	)
	if err == nil || status != durable.EvaluationError {
		t.Fatalf("expected error for missing sandbox, got %s %v", status, err)
	}
}

func TestSandboxEvaluatorMissingRunner(t *testing.T) {
	ev := engine.SandboxEvaluator{Store: stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}}}
	run := &durable.Run{ID: "run-1"}
	status, _, err := ev.Evaluate(
		context.Background(),
		durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "true"},
		run,
	)
	if !errors.Is(err, engine.ErrEvaluatorUnavailable) || status != durable.EvaluationError {
		t.Fatalf("expected unavailable, got %s %v", status, err)
	}
}

type stubSandboxClient struct {
	sdk.SandboxAPI
	op   *sdk.SandboxOperation
	err  error
	exec func(context.Context, *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error)
	get  func(context.Context, *sdk.GetSandboxOperationRequest) (*sdk.SandboxOperation, error)
}

func (s stubSandboxClient) Exec(ctx context.Context, req *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error) {
	if s.exec != nil {
		return s.exec(ctx, req)
	}
	return s.op, s.err
}

func (s stubSandboxClient) GetOperation(
	ctx context.Context,
	req *sdk.GetSandboxOperationRequest,
) (*sdk.SandboxOperation, error) {
	return s.get(ctx, req)
}

func TestHTTPSandboxRunnerVerdictMapping(t *testing.T) {
	intPtr := func(v int) *int { return &v }
	store := stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}}
	run := &durable.Run{ID: "run-1"}
	spec := durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "go test ./..."}

	t.Run("exit 0 passes and extracts stdout", func(t *testing.T) {
		ev := engine.SandboxEvaluator{
			Store: store,
			Runner: engine.HTTPSandboxRunner{
				Client: stubSandboxClient{
					op: &sdk.SandboxOperation{
						ID:       "op-1",
						Status:   sdk.SandboxOpSucceeded,
						ExitCode: intPtr(0),
						Output:   "{\"type\":\"init\",\"text\":\"1\"}\n{\"type\":\"stdout\",\"text\":\"PASS\\n\"}\n{\"type\":\"execution_complete\",\"exit_code\":0}\n",
					},
				},
			},
		}
		status, out, err := ev.Evaluate(context.Background(), spec, run)
		if err != nil || status != durable.EvaluationPassed || out != "PASS\n" {
			t.Fatalf("got status=%s out=%q err=%v", status, out, err)
		}
	})

	t.Run("non-zero exit maps to EvaluationFailed", func(t *testing.T) {
		ev := engine.SandboxEvaluator{
			Store: store,
			Runner: engine.HTTPSandboxRunner{
				Client: stubSandboxClient{
					op: &sdk.SandboxOperation{
						ID:       "op-2",
						Status:   sdk.SandboxOpFailed,
						ExitCode: intPtr(2),
						Output:   "{\"type\":\"init\",\"text\":\"2\"}\n{\"type\":\"stderr\",\"text\":\"FAIL: TestFoo\\n\"}\n{\"type\":\"error\",\"error\":{\"ename\":\"CommandExecError\",\"exit_code\":2}}\n",
						Error:    "exit status 2",
					},
				},
			},
		}
		status, out, err := ev.Evaluate(context.Background(), spec, run)
		if err != nil || status != durable.EvaluationFailed || out != "FAIL: TestFoo\n" {
			t.Fatalf("got status=%s out=%q err=%v", status, out, err)
		}
	})

	t.Run("missing exit code maps to EvaluationError", func(t *testing.T) {
		ev := engine.SandboxEvaluator{
			Store: store,
			Runner: engine.HTTPSandboxRunner{
				Client: stubSandboxClient{
					op: &sdk.SandboxOperation{
						ID:       "op-3",
						Status:   sdk.SandboxOpFailed,
						ExitCode: nil,
						Output:   "{\"type\":\"init\",\"text\":\"3\"}\n",
						Error:    "incomplete command stream: missing terminal event",
					},
				},
			},
		}
		status, _, err := ev.Evaluate(context.Background(), spec, run)
		if err == nil || status != durable.EvaluationError {
			t.Fatalf("got status=%s err=%v, want EvaluationError", status, err)
		}
	})
}

func TestHTTPSandboxRunnerReconciliation(t *testing.T) {
	receipt := func(status string, exitCode *int) *sdk.SandboxOperation {
		return &sdk.SandboxOperation{
			ID: "op-original", SandboxID: "sb-1", RequestKey: "eval-sb-1",
			Status: status, ExitCode: exitCode, Output: "verified\n",
		}
	}
	zero, one := 0, 1
	unknown := receipt(sdk.SandboxOpUnknown, nil)
	unknownErr := &serrors.Error{Status: 502, Code: "OutcomeUnknown", Message: "query original operation"}
	queryErr := errors.New("receipt unavailable")
	forbidden := &serrors.Error{Status: 403, Code: "Forbidden", Message: "forbidden"}
	mismatched := receipt(sdk.SandboxOpSucceeded, &zero)
	mismatched.RequestKey = "another-command"

	tests := []struct {
		name       string
		initial    *sdk.SandboxOperation
		execErr    error
		replayed   *sdk.SandboxOperation
		replayErr  error
		queried    *sdk.SandboxOperation
		queryErr   error
		wantExec   int
		wantQuery  int
		wantStatus durable.EvaluationStatus
		wantOutput string
		wantErr    error
	}{
		{
			name: "unknown queries original success", initial: unknown, execErr: unknownErr,
			queried:  receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name: "unknown queries original failure", initial: unknown, execErr: unknownErr,
			queried:  receipt(sdk.SandboxOpFailed, &one),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationFailed, wantOutput: "verified\n",
		},
		{
			name: "network error with ID only queries", initial: unknown, execErr: io.EOF,
			queried:  receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name: "lost response replays same request", execErr: io.ErrUnexpectedEOF,
			replayed: receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 2, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name:       "request timeout with live parent replays same request",
			execErr:    context.DeadlineExceeded,
			replayed:   receipt(sdk.SandboxOpFailed, &one),
			wantExec:   2,
			wantStatus: durable.EvaluationFailed,
			wantOutput: "verified\n",
		},
		{
			name: "unknown without ID replays same request", execErr: unknownErr,
			replayed: receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 2, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name: "replay recovers ID then queries", execErr: io.EOF,
			replayed: unknown, replayErr: unknownErr, queried: receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 2, wantQuery: 1, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name:       "unknown without recoverable ID remains error",
			execErr:    unknownErr,
			replayErr:  unknownErr,
			wantExec:   2,
			wantStatus: durable.EvaluationError,
			wantErr:    unknownErr,
		},
		{
			name: "nonterminal response queries original", initial: unknown,
			queried:  receipt(sdk.SandboxOpSucceeded, &zero),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationPassed, wantOutput: "verified\n",
		},
		{
			name: "pending remains error", initial: receipt(sdk.SandboxOpPending, nil),
			queried:  receipt(sdk.SandboxOpPending, nil),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationError, wantOutput: "verified\n",
		},
		{
			name: "unknown with zero exit remains error", initial: unknown, execErr: unknownErr,
			queried:  receipt(sdk.SandboxOpUnknown, &zero),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationError, wantOutput: "verified\n",
		},
		{
			name:       "query failure preserves original error",
			initial:    unknown,
			execErr:    io.EOF,
			queryErr:   queryErr,
			wantExec:   1,
			wantQuery:  1,
			wantStatus: durable.EvaluationError,
			wantOutput: "verified\n",
			wantErr:    io.EOF,
		},
		{
			name: "missing query receipt remains error", initial: unknown, execErr: unknownErr,
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationError, wantOutput: "verified\n",
		},
		{
			name:       "unrelated receipt remains error",
			initial:    unknown,
			execErr:    unknownErr,
			queried:    mismatched,
			wantExec:   1,
			wantQuery:  1,
			wantStatus: durable.EvaluationError,
			wantOutput: "verified\n",
		},
		{
			name: "terminal without exit remains error", initial: unknown, execErr: unknownErr,
			queried:  receipt(sdk.SandboxOpSucceeded, nil),
			wantExec: 1, wantQuery: 1, wantStatus: durable.EvaluationError, wantOutput: "verified\n",
		},
		{
			name: "permission rejection is not retried", execErr: forbidden,
			wantExec: 1, wantStatus: durable.EvaluationError, wantErr: forbidden,
		},
		{
			name: "permission rejection on replay stops", execErr: io.EOF, replayErr: forbidden,
			wantExec: 2, wantStatus: durable.EvaluationError, wantErr: forbidden,
		},
		{
			name: "second network failure stops", execErr: io.EOF, replayErr: io.ErrUnexpectedEOF,
			wantExec: 2, wantStatus: durable.EvaluationError, wantErr: io.ErrUnexpectedEOF,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			execCalls, queryCalls := 0, 0
			client := stubSandboxClient{
				exec: func(_ context.Context, req *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error) {
					execCalls++
					if req.ID != "sb-1" || req.RequestKey != "eval-sb-1" ||
						req.Script != "go test ./..." {
						t.Fatalf("changed evaluation request: %+v", req)
					}
					switch execCalls {
					case 1:
						return test.initial, test.execErr
					case 2:
						return test.replayed, test.replayErr
					default:
						t.Fatalf("unexpected third dispatch request")
						return nil, nil
					}
				},
				get: func(_ context.Context, req *sdk.GetSandboxOperationRequest) (*sdk.SandboxOperation, error) {
					queryCalls++
					if req.ID != "sb-1" || req.OperationID != "op-original" {
						t.Fatalf("query changed original operation: %+v", req)
					}
					return test.queried, test.queryErr
				},
			}
			evaluator := engine.SandboxEvaluator{
				Store:  stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}},
				Runner: engine.HTTPSandboxRunner{Client: client},
			}
			status, output, err := evaluator.Evaluate(context.Background(),
				durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "go test ./..."},
				&durable.Run{ID: "run-1"},
			)
			if status != test.wantStatus || output != test.wantOutput ||
				(err != nil) != (test.wantStatus == durable.EvaluationError) {
				t.Fatalf("got status=%s output=%q err=%v", status, output, err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("lost error identity: %v, want %v", err, test.wantErr)
			}
			if execCalls != test.wantExec || queryCalls != test.wantQuery {
				t.Fatalf(
					"got exec=%d query=%d, want exec=%d query=%d",
					execCalls,
					queryCalls,
					test.wantExec,
					test.wantQuery,
				)
			}
		})
	}
}

func TestHTTPSandboxRunnerCancellationStopsReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	runner := engine.HTTPSandboxRunner{Client: stubSandboxClient{
		exec: func(ctx context.Context, _ *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error) {
			calls++
			cancel()
			return nil, ctx.Err()
		},
		get: func(context.Context, *sdk.GetSandboxOperationRequest) (*sdk.SandboxOperation, error) {
			t.Fatal("queried after cancellation")
			return nil, nil
		},
	}}
	if _, err := runner.RunCommand(ctx, "sb-1", "true"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
	if calls != 1 {
		t.Fatalf("got %d exec requests after cancellation", calls)
	}
}

type evaluatorTransport func(*http.Request) (*http.Response, error)

func (transport evaluatorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return transport(req)
}

func TestHTTPSandboxRunnerSDKReconcilesLostReceipt(t *testing.T) {
	for _, failure := range []string{"network", "OutcomeUnknown"} {
		t.Run(failure, func(t *testing.T) {
			calls := 0
			client, err := sdk.NewClient("http://sandbox-center", "test-token", &http.Client{
				Transport: evaluatorTransport(func(req *http.Request) (*http.Response, error) {
					calls++
					if req.Method != http.MethodPost ||
						req.URL.Path != "/v1/sandboxes/sb-1/operations" {
						t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					}
					var body sdk.ExecSandboxRequest
					if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if body.RequestKey != "eval-sb-1" || body.Script != "true" {
						t.Fatalf("changed request: %+v", body)
					}
					status, response := http.StatusOK, `{"id":"op-original","sandboxId":"sb-1","requestKey":"eval-sb-1","status":"Succeeded","exitCode":0,"output":"PASS"}`
					if calls == 1 {
						if failure == "network" {
							return nil, io.ErrUnexpectedEOF
						}
						status, response = http.StatusBadGateway, `{"code":"OutcomeUnknown","message":"query original operation"}`
					}
					return &http.Response{
						StatusCode: status,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(response)),
						Request:    req,
					}, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			output, err := (engine.HTTPSandboxRunner{Client: client.Sandboxes}).RunCommand(
				context.Background(),
				"sb-1",
				"true",
			)
			if err != nil || string(output) != "PASS" || calls != 2 {
				t.Fatalf("got output=%q err=%v calls=%d", output, err, calls)
			}
		})
	}
}
