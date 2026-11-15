package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestSandboxReconcileOverHTTP(t *testing.T) {
	tests := []struct {
		name       string
		receipt    string
		wantStatus durable.EvaluationStatus
		wantOutput string
	}{
		{
			name:       "terminal success passes",
			receipt:    `{"id":"op-original","sandboxId":"sb-1","requestKey":"eval-sb-1","status":"Succeeded","exitCode":0,"output":"PASS"}`,
			wantStatus: durable.EvaluationPassed,
			wantOutput: "PASS",
		},
		{
			name:       "terminal failure fails",
			receipt:    `{"id":"op-original","sandboxId":"sb-1","requestKey":"eval-sb-1","status":"Failed","exitCode":1,"output":"FAIL"}`,
			wantStatus: durable.EvaluationFailed,
			wantOutput: "FAIL",
		},
		{
			name:       "persistent unknown stays error",
			receipt:    `{"id":"op-original","sandboxId":"sb-1","requestKey":"eval-sb-1","status":"Unknown","output":"partial"}`,
			wantStatus: durable.EvaluationError,
			wantOutput: "partial",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var posts, gets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/operations":
					posts.Add(1)
					var req sdk.ExecSandboxRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Errorf("decode exec: %v", err)
					}
					if req.RequestKey != "eval-sb-1" || req.Script != "go test ./..." {
						t.Errorf("unexpected exec request: %+v", req)
					}
					w.Header().Set("X-Sandbox-Operation-ID", "op-original")
					w.WriteHeader(http.StatusBadGateway)
					_, _ = w.Write(
						[]byte(
							`{"code":"OutcomeUnknown","message":"query the original operation"}`,
						),
					)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/sb-1/operations/op-original":
					gets.Add(1)
					_, _ = w.Write([]byte(test.receipt))
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			client, err := sdk.NewClient(server.URL+"/api/", "test-token", server.Client())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			evaluator := engine.SandboxEvaluator{
				Store:  stubEnvStore{env: &durable.RunEnvironment{SandboxID: "sb-1"}},
				Runner: engine.HTTPSandboxRunner{Client: client.Sandboxes},
			}
			status, output, evalErr := evaluator.Evaluate(
				context.Background(),
				durable.EvaluatorSpec{Kind: durable.EvaluatorCommand, Config: "go test ./..."},
				&durable.Run{ID: "run-1"},
			)
			if status != test.wantStatus || output != test.wantOutput {
				t.Fatalf("got status=%s output=%q err=%v", status, output, evalErr)
			}
			if test.wantStatus == durable.EvaluationError && evalErr == nil {
				t.Fatal("expected reconciliation error")
			}
			if test.wantStatus != durable.EvaluationError && evalErr != nil {
				t.Fatalf("unexpected evaluation error: %v", evalErr)
			}
			if posts.Load() != 1 || gets.Load() != 1 {
				t.Fatalf("got posts=%d gets=%d, want exactly one of each", posts.Load(), gets.Load())
			}
		})
	}
}

func TestSandboxReconcileHTTPKeepsOriginalErrorWhenQueryFails(t *testing.T) {
	var posts, gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			w.Header().Set("X-Sandbox-Operation-ID", "op-original")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"code":"OutcomeUnknown","message":"query the original operation"}`))
		case http.MethodGet:
			gets.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"Unavailable","message":"receipt store offline"}`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	client, err := sdk.NewClient(server.URL+"/api/", "test-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	_, err = (engine.HTTPSandboxRunner{Client: client.Sandboxes}).RunCommand(context.Background(), "sb-1", "true")
	if err == nil {
		t.Fatal("expected reconciliation error")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("query failure must stay a reconciliation error: %v", err)
	}
	if posts.Load() != 1 || gets.Load() != 1 {
		t.Fatalf("got posts=%d gets=%d, want exactly one of each", posts.Load(), gets.Load())
	}
}
