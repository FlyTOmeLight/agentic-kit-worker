package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const integrationMCPLookup = "mcp__catalog__lookup"

type integrationMCPRequest struct {
	method string
	header http.Header
}

func integrationMCPServer(
	t *testing.T,
	server *mcp.Server,
	intercept func(http.ResponseWriter, *http.Request, string) bool,
) (string, func() []integrationMCPRequest) {
	t.Helper()
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{JSONResponse: true})
	var mu sync.Mutex
	var requests []integrationMCPRequest
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		method := req.Method
		if req.Method == http.MethodPost {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Error(err)
				return
			}
			req.Body = io.NopCloser(bytes.NewReader(body))
			var message struct{ Method string }
			if err = json.Unmarshal(body, &message); err != nil {
				t.Error(err)
				return
			}
			method = message.Method
		}
		mu.Lock()
		requests = append(requests, integrationMCPRequest{method: method, header: req.Header.Clone()})
		mu.Unlock()
		if intercept == nil || !intercept(w, req, method) {
			handler.ServeHTTP(w, req)
		}
	}))
	t.Cleanup(endpoint.Close)
	return endpoint.URL, func() []integrationMCPRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]integrationMCPRequest(nil), requests...)
	}
}

func addIntegrationMCPTool(server *mcp.Server, name string, handler mcp.ToolHandler) {
	server.AddTool(&mcp.Tool{Name: name, InputSchema: map[string]any{"type": "object"}}, handler)
}

func integrationMCPText(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

func submitMCPRun(t *testing.T, store *durable.Store, remote durable.MCPServerSpec) *durable.Run {
	t.Helper()
	_, run := submitChatFixture(t, store, "owner", "mcp", durable.AgentConfig{
		Model:      durable.ModelSpec{Name: "test", BaseURL: "http://localhost:11434/v1"},
		MCPServers: []durable.MCPServerSpec{remote},
		MaxTurns:   4,
	}, "use the catalog", time.Hour, 4, func(spec *durable.Spec) {
		spec.Scope = &durable.Scope{TenantID: "tenant", TaskID: "task"}
	})
	return run
}

func claimMCPRun(t *testing.T, store *durable.Store) *durable.Run {
	t.Helper()
	run, err := store.Claim(t.Context(), "mcp-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func executeMCPRun(t *testing.T, store *durable.Store, claim *durable.Run, llm *fakeModel) *durable.Run {
	t.Helper()
	worker := &engine.Engine{
		Store: store,
		Build: func(ctx context.Context, store *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			_, bindings, err := adapters.Build(ctx, store, spec)
			return llm, bindings, err
		},
	}
	if err := worker.Execute(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	run, err := store.Get(t.Context(), claim.OwnerID, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func integrationMCPStep(t *testing.T, store *durable.Store, runID, stepID string) (string, durable.Policy) {
	t.Helper()
	var step struct {
		Result string
		Policy durable.Policy
		Status string
	}
	err := store.DB().WithContext(t.Context()).Table("durable_step").
		Select("result", "policy", "status").
		Where("run_id = ? AND step_id = ?", runID, stepID).Take(&step).Error
	if err != nil || step.Status != "Completed" {
		t.Fatalf("step %s: status=%q err=%v", stepID, step.Status, err)
	}
	return step.Result, step.Policy
}

func countMCPRequests(requests []integrationMCPRequest, method string) int {
	count := 0
	for _, request := range requests {
		if request.method == method {
			count++
		}
	}
	return count
}

func TestMCPDurableToolFeedback(t *testing.T) {
	for _, test := range []struct {
		name          string
		result        func() *mcp.CallToolResult
		rpcError      bool
		forwardScope  bool
		want          string
		wantTruncated bool
	}{
		{name: "tool error", result: func() *mcp.CallToolResult {
			result := integrationMCPText("catalog rejected request")
			result.IsError = true
			return result
		}, want: `{"error":{"code":"MCP_TOOL_ERROR","message":"catalog rejected request"}}`},
		{name: "invalid params", rpcError: true, want: "invalid catalog parameters", forwardScope: true},
		{name: "oversized result", result: func() *mcp.CallToolResult {
			return integrationMCPText(strings.Repeat("目", (4<<20)/3+10))
		}, want: "[truncated: MCP tool result exceeds 4 MiB]", wantTruncated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			t.Setenv("TEST_WORKER_MCP_TOKEN", "test-credential")
			server := mcp.NewServer(&mcp.Implementation{Name: "catalog", Version: "test"}, nil)
			addIntegrationMCPTool(
				server,
				"lookup",
				func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					return test.result(), nil
				},
			)
			if test.rpcError {
				server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
					return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
						if method == "tools/call" {
							return nil, &jsonrpc.Error{Code: -32602, Message: test.want}
						}
						return next(ctx, method, req)
					}
				})
			}
			url, requests := integrationMCPServer(t, server, nil)
			run := submitMCPRun(t, store, durable.MCPServerSpec{
				Name:         "catalog",
				URL:          url,
				TokenEnv:     "TEST_WORKER_MCP_TOKEN",
				ForwardScope: test.forwardScope,
			})
			claim := claimMCPRun(t, store)
			done := executeMCPRun(t, store, claim, &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					last := messages[len(messages)-1]
					if last.Role != schema.Tool {
						reply := toolReply(integrationMCPLookup)
						reply.ToolCalls[0].Function.Arguments = " \t "
						return reply, nil
					}
					if !test.wantTruncated && !strings.Contains(last.Content, test.want) {
						t.Errorf("model feedback: %q", last.Content)
					}
					return schema.AssistantMessage("handled result", nil), nil
				},
			})
			if done.Status != durable.Completed || done.Output != "handled result" {
				t.Fatalf("status=%s output=%q error=%q", done.Status, done.Output, done.Error)
			}
			result, policy := integrationMCPStep(t, store, run.ID, "turn_1_tool_0")
			if !strings.Contains(result, test.want) || policy != durable.Unsafe ||
				len(result) > 4<<20 || !utf8.ValidString(result) {
				t.Fatalf(
					"step result bytes=%d policy=%s, missing valid expected result",
					len(result),
					policy,
				)
			}
			if !test.wantTruncated && !json.Valid([]byte(result)) {
				t.Fatalf("invalid stored feedback: %q", result)
			}
			received := requests()
			if countMCPRequests(received, "tools/call") != 1 ||
				countMCPRequests(received, http.MethodDelete) != 1 {
				t.Fatalf("expected one call and one session close, received %v", received)
			}
			for _, request := range received {
				header := request.header
				wantKey := ""
				if request.method == "tools/call" {
					wantKey = run.ID + ":turn_1_tool_0"
				}
				if header.Get("Authorization") != "Bearer test-credential" ||
					header.Get("Idempotency-Key") != wantKey {
					t.Fatalf("%s: invalid authorization or idempotency headers", request.method)
				}
				var wantRunID, wantEpoch, wantScope string
				if test.forwardScope {
					wantRunID, wantEpoch = run.ID, strconv.FormatInt(claim.Epoch, 10)
					raw, err := json.Marshal(claim.Spec.Scope)
					if err != nil {
						t.Fatal(err)
					}
					wantScope = string(raw)
				}
				if header.Get("X-Agentic-Run-ID") != wantRunID ||
					header.Get("X-Agentic-Epoch") != wantEpoch ||
					header.Get("X-Agentic-Scope") != wantScope {
					t.Fatalf("%s: unexpected scope headers", request.method)
				}
			}
		})
	}
}

func TestMCPUnsafeTransportFailureAndResolve(t *testing.T) {
	store, _ := testDB(t)
	server := mcp.NewServer(&mcp.Implementation{Name: "catalog", Version: "test"}, nil)
	addIntegrationMCPTool(
		server,
		"lookup",
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			t.Error("the transport fault should intercept the call")
			return nil, errors.New("unexpected call")
		},
	)
	url, requests := integrationMCPServer(
		t,
		server,
		func(w http.ResponseWriter, _ *http.Request, method string) bool {
			if method != "tools/call" {
				return false
			}
			http.Error(w, "receipt lost", http.StatusServiceUnavailable)
			return true
		},
	)
	run := submitMCPRun(t, store, durable.MCPServerSpec{Name: "catalog", URL: url})
	llm := &fakeModel{generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
		if last := messages[len(messages)-1]; last.Role == schema.Tool {
			return schema.AssistantMessage("confirmed: "+last.Content, nil), nil
		}
		return toolReply(integrationMCPLookup), nil
	}}
	paused := executeMCPRun(t, store, claimMCPRun(t, store), llm)
	if paused.Status != durable.NeedsReview || paused.WaitingStep != "turn_1_tool_0" ||
		countMCPRequests(requests(), http.MethodDelete) != 1 {
		t.Fatalf(
			"status=%s waiting=%q error=%q or session still open",
			paused.Status,
			paused.WaitingStep,
			paused.Error,
		)
	}
	if err := store.SupplyResult(
		t.Context(),
		"owner",
		run.ID,
		paused.WaitingStep,
		"receipt",
		durable.InteractionReview,
	); err != nil {
		t.Fatal(err)
	}
	done := executeMCPRun(t, store, claimMCPRun(t, store), llm)
	received := requests()
	if done.Status != durable.Completed || done.Output != "confirmed: receipt" ||
		countMCPRequests(received, "tools/call") != 1 || countMCPRequests(received, http.MethodDelete) != 2 {
		t.Fatalf("status=%s output=%q error=%q requests=%v", done.Status, done.Output, done.Error, received)
	}
}

func TestMCPRecoveryReplaysCompletedStepAndHandlesDisappearedTool(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote tools disappeared=%t", drift), func(t *testing.T) {
			store, _ := testDB(t)
			server := mcp.NewServer(&mcp.Implementation{Name: "catalog", Version: "test"}, nil)
			addIntegrationMCPTool(
				server,
				"lookup",
				func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					return integrationMCPText("saved receipt"), nil
				},
			)
			addIntegrationMCPTool(
				server,
				"next",
				func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					return integrationMCPText("second result"), nil
				},
			)
			interrupted, cancel := context.WithCancel(t.Context())
			defer cancel()
			var attempts atomic.Int32
			cancelled := make(chan struct{})
			var notified sync.Once
			url, requests := integrationMCPServer(
				t,
				server,
				func(_ http.ResponseWriter, req *http.Request, method string) bool {
					switch method {
					case "tools/call":
						if attempts.Add(1) == 2 {
							cancel()
							<-req.Context().Done()
							return true
						}
					case "notifications/cancelled":
						if req.Header.Get("Idempotency-Key") != "" {
							t.Error(
								"cancellation notification carried the tool's idempotency key",
							)
						}
						notified.Do(func() { close(cancelled) })
					}
					return false
				},
			)
			run := submitMCPRun(t, store, durable.MCPServerSpec{
				Name:         "catalog",
				URL:          url,
				ToolPolicies: map[string]durable.Policy{"next": durable.ReadOnly},
			})
			first := &engine.Engine{
				Store: store,
				Build: func(ctx context.Context, store *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
					_, bindings, err := adapters.Build(ctx, store, spec)
					if err != nil {
						return nil, nil, err
					}
					next := bindings["mcp__catalog__next"]
					next.Tool = &observeMCPCancellation{
						InvokableTool: next.Tool,
						cancelled:     cancelled,
						test:          t,
					}
					bindings["mcp__catalog__next"] = next
					return &fakeModel{
						generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
							reply := toolReply(integrationMCPLookup)
							reply.ToolCalls = append(
								reply.ToolCalls,
								toolReply("mcp__catalog__next").ToolCalls...)
							return reply, nil
						},
					}, bindings, nil
				},
			}
			if err := first.Execute(interrupted, getClaim(t, store)); !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted execution: %v", err)
			}
			if result, policy := integrationMCPStep(
				t,
				store,
				run.ID,
				"turn_1_tool_0",
			); result != "saved receipt" ||
				policy != durable.Unsafe {
				t.Fatalf("first step: result=%q policy=%s", result, policy)
			}
			if attempts.Load() != 2 || countMCPRequests(requests(), http.MethodDelete) != 1 {
				t.Fatal("execution did not stop at the second tool and close the session")
			}
			if drift {
				server.RemoveTools("lookup", "next")
			}
			wantNext := "second result"
			wantCalls := 3
			if drift {
				wantNext = `{"error":{"code":"TOOL_UNAVAILABLE","message":"MCP tool \"mcp__catalog__next\" is no longer available"}}`
				wantCalls = 2
			}
			done := executeMCPRun(t, store, claimAfterExpiry(t, store), &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					if len(messages) < 2 || messages[len(messages)-2].Role != schema.Tool ||
						messages[len(messages)-2].Content != "saved receipt" || messages[len(messages)-1].Role != schema.Tool ||
						messages[len(messages)-1].Content != wantNext {
						return nil, errors.New(
							"recovery did not replay the receipt and return the second tool result",
						)
					}
					return schema.AssistantMessage("recovered", nil), nil
				},
			})
			if done.Status != durable.Completed || done.Output != "recovered" ||
				countMCPRequests(
					requests(),
					"tools/call",
				) != wantCalls || countMCPRequests(requests(), http.MethodDelete) != 2 {
				t.Fatalf(
					"status=%s output=%q error=%q requests=%v",
					done.Status,
					done.Output,
					done.Error,
					requests(),
				)
			}
			if result, policy := integrationMCPStep(
				t,
				store,
				run.ID,
				"turn_1_tool_1",
			); result != wantNext ||
				policy != durable.ReadOnly {
				t.Fatalf("second step: result=%q policy=%s", result, policy)
			}
		})
	}
}

type observeMCPCancellation struct {
	tool.InvokableTool
	cancelled <-chan struct{}
	test      *testing.T
}

func (t *observeMCPCancellation) InvokableRun(
	ctx context.Context,
	arguments string,
	opts ...tool.Option,
) (string, error) {
	result, err := t.InvokableTool.InvokableRun(ctx, arguments, opts...)
	if errors.Is(err, context.Canceled) {
		select {
		case <-t.cancelled:
		case <-time.After(3 * time.Second):
			t.test.Error("MCP cancellation notification did not arrive")
		}
	}
	return result, err
}

func (t *observeMCPCancellation) CloseSession(ctx context.Context) error {
	return t.InvokableTool.(interface{ CloseSession(context.Context) error }).CloseSession(ctx)
}

func TestMCPBuildFailureClassification(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "http 503", want: "503"},
		{name: "disconnect", want: "EOF"},
		{name: "http 429", want: "429"},
		{name: "missing token", want: "is not configured"},
		{name: "missing tool", want: "available tools"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := testDB(t)
			server := mcp.NewServer(&mcp.Implementation{Name: "catalog", Version: "test"}, nil)
			addIntegrationMCPTool(
				server,
				"lookup",
				func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					return integrationMCPText("result"), nil
				},
			)
			url, requests := integrationMCPServer(
				t,
				server,
				func(w http.ResponseWriter, _ *http.Request, method string) bool {
					if method != "tools/list" {
						return false
					}
					switch test.name {
					case "http 503":
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
					case "http 429":
						http.Error(w, "rate limited", http.StatusTooManyRequests)
					case "disconnect":
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return false
						}
						_ = conn.Close()
					default:
						return false
					}
					return true
				},
			)
			remote := durable.MCPServerSpec{Name: "catalog", URL: url}
			wantStatus := durable.Queued
			switch test.name {
			case "missing token":
				t.Setenv("TEST_WORKER_MCP_MISSING", "")
				remote.TokenEnv = "TEST_WORKER_MCP_MISSING"
				wantStatus = durable.Failed
			case "missing tool":
				remote.AllowedTools = []string{"missing"}
				wantStatus = durable.Failed
			}
			run := submitMCPRun(t, store, remote)
			done := executeMCPRun(t, store, claimMCPRun(t, store), nil)
			if done.Status != wantStatus || !strings.Contains(done.Error, test.want) {
				t.Fatalf(
					"status=%s, want %s, error=%q, want %q",
					done.Status,
					wantStatus,
					done.Error,
					test.want,
				)
			}
			events, err := store.Events(t.Context(), "owner", run.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			retried := false
			for _, event := range events {
				retried = retried || event.Kind == "RunRetry"
			}
			if retried != (wantStatus == durable.Queued) {
				t.Fatalf("RunRetry=%t, status=%s", retried, done.Status)
			}
			wantDeletes := 1
			if test.name == "missing token" {
				wantDeletes = 0
				if len(requests()) != 0 {
					t.Fatal("missing token did not prevent connecting")
				}
			}
			if countMCPRequests(requests(), http.MethodDelete) != wantDeletes {
				t.Fatalf("session cleanup: %v", requests())
			}
		})
	}
}
