package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/adk/filesystem"
)

type receiptClient struct {
	sdk.SandboxAPI
	receipt *sdk.SandboxOperation
}

func (client receiptClient) Exec(context.Context, *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error) {
	return client.receipt, nil
}

func TestExecuteRequiresDurableExitVerdict(t *testing.T) {
	zero, one, negative := 0, 1, -1
	cases := []struct {
		name     string
		receipt  *sdk.SandboxOperation
		wantErr  string
		wantExit int
	}{
		{name: "success", receipt: &sdk.SandboxOperation{Status: sdk.SandboxOpSucceeded, ExitCode: &zero}},
		{
			name:     "command failure",
			receipt:  &sdk.SandboxOperation{Status: sdk.SandboxOpFailed, ExitCode: &one},
			wantExit: 1,
		},
		{
			name:    "start failure",
			receipt: &sdk.SandboxOperation{Status: sdk.SandboxOpFailed, Error: "cannot start process"},
			wantErr: "cannot start process",
		},
		{
			name:    "truncated stream",
			receipt: &sdk.SandboxOperation{Status: sdk.SandboxOpFailed, Error: "missing terminal event"},
			wantErr: "missing terminal event",
		},
		{
			name:    "success without verdict",
			receipt: &sdk.SandboxOperation{Status: sdk.SandboxOpSucceeded},
			wantErr: "no exit verdict",
		},
		{
			name: "failed with zero exit",
			receipt: &sdk.SandboxOperation{
				Status:   sdk.SandboxOpFailed,
				ExitCode: &zero,
				Error:    "dispatch failed",
			},
			wantErr: "dispatch failed",
		},
		{
			name: "signal termination",
			receipt: &sdk.SandboxOperation{
				Status:   sdk.SandboxOpFailed,
				ExitCode: &negative,
				Error:    "signal: terminated",
			},
			wantErr: "signal: terminated",
		},
		{name: "missing receipt", wantErr: "no operation receipt"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			backend := &Backend{sandboxID: "sandbox", client: receiptClient{receipt: test.receipt}}
			ctx := WithOperationID(context.Background(), "run:step")
			result, err := backend.Execute(ctx, &filesystem.ExecuteRequest{Command: "true"})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || result != nil {
					t.Fatalf(
						"result=%+v err=%v, want error containing %q",
						result,
						err,
						test.wantErr,
					)
				}
				return
			}
			if err != nil || result == nil || result.ExitCode == nil || *result.ExitCode != test.wantExit {
				t.Fatalf("result=%+v err=%v, want exit %d", result, err, test.wantExit)
			}
		})
	}
}

// fakeCenter implements the sandbox-center HTTP contract in memory so the
// backend's idempotency and output parsing are tested without a cluster.
type fakeCenter struct {
	mu       sync.Mutex
	sandbox  map[string]string // requestKey -> sandbox id
	ops      map[string]*opRecord
	released map[string]bool
	renewals []string
}

type opRecord struct {
	key    string
	script string
	status string
	output string
}

func newFakeCenter(t *testing.T) (*fakeCenter, *httptest.Server) {
	t.Helper()
	f := &fakeCenter{
		sandbox:  map[string]string{},
		ops:      map[string]*opRecord{},
		released: map[string]bool{},
	}
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
			if id, ok := f.sandbox[req.RequestKey]; ok {
				w.Write(
					[]byte(
						`{"sandbox":{"id":"` + id + `","status":"Active","leaseId":"lease-` + id + `"},"lease":{"id":"lease-` + id + `","revision":1,"status":"Active"}}`,
					),
				)
				return
			}
			id := "sb-" + req.RequestKey
			f.sandbox[req.RequestKey] = id
			w.Write(
				[]byte(
					`{"sandbox":{"id":"` + id + `","status":"Active","leaseId":"lease-` + id + `"},"lease":{"id":"lease-` + id + `","revision":1,"status":"Active"}}`,
				),
			)
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/operations"):
			var req sdk.ExecSandboxRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode exec: %v", err)
			}
			sbID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/"), "/operations")
			key := sbID + "/" + req.RequestKey
			if op, ok := f.ops[key]; ok {
				json.NewEncoder(w).
					Encode(map[string]any{"id": "op-" + op.key, "status": op.status, "output": op.output})
				return
			}
			output := execdStream("hello\n", 0)
			f.ops[key] = &opRecord{
				key:    req.RequestKey,
				script: req.Script,
				status: sdk.SandboxOpSucceeded,
				output: output,
			}
			json.NewEncoder(w).
				Encode(map[string]any{"id": "op-" + req.RequestKey, "status": sdk.SandboxOpSucceeded, "output": output})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/renew"):
			var req sdk.RenewSandboxRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode renew: %v", err)
			}
			f.renewals = append(f.renewals, req.RequestKey)
			json.NewEncoder(w).
				Encode(map[string]any{"id": "lease", "revision": req.Revision, "status": "Active"})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/sandboxes/"):
			sbID := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
			status := "Active"
			if f.released[sbID] {
				status = "Destroyed"
			}
			w.Write([]byte(`{"sandbox":{"id":"` + sbID + `","status":"` + status + `"}}`))
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/v1/sandboxes/"):
			sbID := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
			f.released[sbID] = true
			w.Write([]byte(`{"id":"` + sbID + `","status":"Destroyed"}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return f, server
}

func execdStream(stdout string, exitCode int) string {
	frame := func(v map[string]any) string {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return strings.Join([]string{
		frame(map[string]any{"type": "init"}),
		frame(map[string]any{"type": "stdout", "text": stdout}),
		frame(map[string]any{"type": "execution_complete", "exitCode": exitCode}),
	}, "\n")
}

func TestParseExecOutput(t *testing.T) {
	stdout, exitCode := parseExecOutput(execdStream("hello\nworld\n", 3))
	if stdout != "hello\nworld\n" || exitCode != 3 {
		t.Fatalf("stdout=%q exitCode=%d", stdout, exitCode)
	}
	// Garbage lines are ignored rather than failing the parse.
	stdout, exitCode = parseExecOutput("noise\n" + execdStream("ok", 0))
	if stdout != "ok" || exitCode != 0 {
		t.Fatalf("stdout=%q exitCode=%d", stdout, exitCode)
	}
	// Real execd reports non-zero exit codes via CommandExecError frames with snake_case exit_code.
	realFail := "{\"type\":\"init\",\"text\":\"1\"}\n" +
		"{\"type\":\"stdout\",\"text\":\"fail out\\n\"}\n" +
		"{\"type\":\"error\",\"error\":{\"ename\":\"CommandExecError\",\"evalue\":\"5\",\"traceback\":[\"exit status 5\"],\"exit_code\":5}}\n"
	stdout, exitCode = parseExecOutput(realFail)
	if stdout != "fail out\n" || exitCode != 5 {
		t.Fatalf("stdout=%q exitCode=%d", stdout, exitCode)
	}
}

func TestPathResolutionConfinesWorkspace(t *testing.T) {
	b := &Backend{cfg: Config{Workspace: "/workspace"}}
	for raw, want := range map[string]string{
		"":                "/workspace",
		"repo/main.go":    "/workspace/repo/main.go",
		"/workspace/a":    "/workspace/a",
		"./x":             "/workspace/x",
		"repo/../main.go": "/workspace/main.go",
	} {
		got, err := b.resolve(raw)
		if err != nil || got != want {
			t.Errorf("resolve(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"../etc/passwd", "/etc/passwd", "/workspace2/x", "repo/../../etc"} {
		if _, err := b.resolve(raw); err == nil {
			t.Errorf("resolve(%q) must reject workspace escape", raw)
		}
		var inputErr *InputError
		if err := func() error { _, err := b.resolve(raw); return err }(); !errors.As(err, &inputErr) {
			t.Errorf("resolve(%q) error must be an input error, got %v", raw, err)
		}
	}
}
