package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const testMCPTool = "mcp__catalog__lookup"

type testMCPRequest struct {
	Method string
	Header http.Header
}

func newTestMCPServer(
	t *testing.T,
	server *mcp.Server,
	intercept func(http.ResponseWriter, *http.Request, string) bool,
) *httptest.Server {
	t.Helper()
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{JSONResponse: true})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var message struct{ Method string }
			if err := json.Unmarshal(body, &message); err != nil {
				t.Error(err)
				return
			}
			method = message.Method
		}
		if intercept == nil || !intercept(w, r, method) {
			handler.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(httpServer.Close)
	return httpServer
}

func testMCPServerWithLookup(handler mcp.ToolHandler) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "catalog", Version: "1"}, nil)
	if handler == nil {
		handler = func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: string(req.Params.Arguments)}},
			}, nil
		}
	}
	server.AddTool(&mcp.Tool{Name: "lookup", InputSchema: map[string]any{"type": "object"}}, handler)
	return server
}

func testMCPSpec(url string) durable.Spec {
	return durable.Spec{
		Model:      durable.ModelSpec{Name: "test", BaseURL: "http://model.invalid/v1"},
		MCPServers: []durable.MCPServerSpec{{Name: "catalog", URL: url}},
	}
}

func buildTestMCP(t *testing.T, spec durable.Spec) map[string]engine.Binding {
	t.Helper()
	_, bindings, err := Build(t.Context(), nil, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, binding := range bindings {
			if session, ok := binding.Tool.(interface{ CloseSession(context.Context) error }); ok {
				if err := session.CloseSession(context.Background()); err != nil {
					t.Error(err)
				}
			}
		}
	})
	return bindings
}

func serveMCPToolPages(server *mcp.Server, page func(string) *mcp.ListToolsResult) {
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				return page(req.(*mcp.ListToolsRequest).Params.Cursor), nil
			}
			return next(ctx, method, req)
		}
	})
}

func TestMCPDiscoveryAndPolicies(t *testing.T) {
	server := testMCPServerWithLookup(nil)
	var pages atomic.Int32
	longName := strings.Repeat("n", 64)
	serveMCPToolPages(server, func(cursor string) *mcp.ListToolsResult {
		pages.Add(1)
		if cursor == "" {
			return &mcp.ListToolsResult{NextCursor: "page-two", Tools: []*mcp.Tool{
				{Name: "lookup", Description: strings.Repeat("查", 700), InputSchema: map[string]any{}},
				{Name: "contains space"},
				{Name: longName},
				{Name: "write", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}},
			}}
		}
		return &mcp.ListToolsResult{Tools: []*mcp.Tool{
			{
				Name: "small-schema",
				InputSchema: map[string]any{
					"type":    "object",
					"$schema": strings.Repeat("x", mcpMaxSchema),
				},
			},
			{
				Name: "large-schema",
				InputSchema: map[string]any{
					"type":        "object",
					"description": strings.Repeat("x", mcpMaxSchema),
				},
			},
			{Name: "broken-schema", InputSchema: map[string]any{"type": "object", "$ref": "#/missing"}},
		}}
	})
	httpServer := newTestMCPServer(t, server, nil)
	spec := testMCPSpec(httpServer.URL)
	spec.MCPServers[0].ToolPolicies = map[string]durable.Policy{"lookup": durable.ReadOnly}
	bindings := buildTestMCP(t, spec)
	if pages.Load() != 2 || len(bindings) != 3 {
		t.Fatalf("pages=%d bindings=%v", pages.Load(), bindings)
	}
	for name, want := range map[string]durable.Policy{
		testMCPTool: durable.ReadOnly, "mcp__catalog__write": durable.Unsafe, "mcp__catalog__small-schema": durable.Unsafe,
	} {
		binding, ok := bindings[name]
		if !ok || binding.Policy != want || binding.EndRun || binding.ReturnDirectly {
			t.Fatalf("%s binding=%+v, want policy %s", name, binding, want)
		}
		info, err := binding.Tool.Info(t.Context())
		if err != nil || info.Name != name {
			t.Fatalf("info=%+v err=%v", info, err)
		}
		params, err := info.ParamsOneOf.ToJSONSchema()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(params)
		if err != nil || params.Type != "object" || strings.Contains(string(raw), "$schema") {
			t.Fatalf("model schema=%s err=%v", raw, err)
		}
		if name != "mcp__catalog__small-schema" && string(raw) != `{"properties":{},"type":"object"}` &&
			string(raw) != `{"type":"object","properties":{}}` {
			t.Fatalf("empty schema normalized to %s", raw)
		}
		if name == testMCPTool && (len(info.Desc) != 2046 || !utf8.ValidString(info.Desc)) {
			t.Fatalf("description bytes=%d", len(info.Desc))
		}
		if err := binding.Validate("   "); err != nil {
			t.Fatal(err)
		}
	}
	spec.MCPServers[0].AllowedTools = []string{"lookup", "small-schema"}
	spec.MCPServers[0].DefaultPolicy = durable.Idempotent
	bindings = buildTestMCP(t, spec)
	if len(bindings) != 2 || bindings[testMCPTool].Policy != durable.ReadOnly ||
		bindings["mcp__catalog__small-schema"].Policy != durable.Idempotent {
		t.Fatalf("allowlist/default/override: %+v", bindings)
	}
}

func TestMCPDiscoveryRejectsInvalidConfiguration(t *testing.T) {
	remote := []*mcp.Tool{
		{Name: "lookup"},
		{Name: "bad/name"},
		{Name: strings.Repeat("x", 64)},
		{Name: "large", InputSchema: map[string]any{"description": strings.Repeat("x", mcpMaxSchema)}},
		{Name: "broken", InputSchema: map[string]any{"type": "object", "$ref": "#/missing"}},
	}
	var tooMany []*mcp.Tool
	for index := range mcpMaxServerTools + 1 {
		tooMany = append(tooMany, &mcp.Tool{Name: fmt.Sprintf("tool-%d", index)})
	}
	for _, test := range []struct {
		name      string
		allow     string
		list      func(string) *mcp.ListToolsResult
		want      string
		wantPages int32
	}{
		{name: "missing allowlist", allow: "gone", want: "available tools:", wantPages: 1},
		{name: "invalid allowlist name", allow: "bad/name", want: "invalid MCP tool name", wantPages: 1},
		{name: "full name exceeds provider limit", allow: strings.Repeat("x", 64), want: "invalid MCP tool name", wantPages: 1},
		{name: "large schema", allow: "large", want: "exceeds 16 KiB", wantPages: 1},
		{name: "invalid schema", allow: "broken", want: "compile MCP input schema", wantPages: 1},
		{name: "per-server limit", list: func(string) *mcp.ListToolsResult {
			return &mcp.ListToolsResult{Tools: tooMany}
		}, want: "more than 64", wantPages: 1},
		{name: "duplicate tools", list: func(cursor string) *mcp.ListToolsResult {
			if cursor == "" {
				return &mcp.ListToolsResult{Tools: remote[:1], NextCursor: "more"}
			}
			return &mcp.ListToolsResult{Tools: remote[:1]}
		}, want: "duplicate remote tool", wantPages: 2},
		{name: "repeated cursor", list: func(string) *mcp.ListToolsResult {
			return &mcp.ListToolsResult{NextCursor: "repeated"}
		}, want: "repeated next cursor", wantPages: 2},
		{name: "page limit", list: func(cursor string) *mcp.ListToolsResult {
			return &mcp.ListToolsResult{NextCursor: cursor + "x"}
		}, want: "exceeds 10 pages", wantPages: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := testMCPServerWithLookup(nil)
			var pages, deletes atomic.Int32
			serveMCPToolPages(server, func(cursor string) *mcp.ListToolsResult {
				pages.Add(1)
				if test.list != nil {
					return test.list(cursor)
				}
				return &mcp.ListToolsResult{Tools: remote}
			})
			httpServer := newTestMCPServer(
				t,
				server,
				func(_ http.ResponseWriter, _ *http.Request, method string) bool {
					if method == http.MethodDelete {
						deletes.Add(1)
					}
					return false
				},
			)
			spec := testMCPSpec(httpServer.URL)
			if test.allow != "" {
				spec.MCPServers[0].AllowedTools = []string{test.allow}
			}
			_, _, err := Build(t.Context(), nil, spec)
			var retryable interface{ Retryable() bool }
			if err == nil || !strings.Contains(err.Error(), test.want) || errors.As(err, &retryable) {
				t.Fatalf("error=%v, want non-retryable %q", err, test.want)
			}
			if test.allow != "" && !strings.Contains(err.Error(), `"lookup"`) {
				t.Fatalf("missing available tool names: %v", err)
			}
			if pages.Load() != test.wantPages || deletes.Load() != 1 {
				t.Fatalf("pages=%d deletes=%d", pages.Load(), deletes.Load())
			}
		})
	}
}

func TestMCPGlobalToolLimitClosesEverySession(t *testing.T) {
	var deletes atomic.Int32
	server := testMCPServerWithLookup(nil)
	httpServer := newTestMCPServer(t, server, func(_ http.ResponseWriter, _ *http.Request, method string) bool {
		if method == http.MethodDelete {
			deletes.Add(1)
		}
		return false
	})
	spec := testMCPSpec(httpServer.URL)
	spec.MCPServers = append(spec.MCPServers, durable.MCPServerSpec{Name: "other", URL: httpServer.URL})
	for index := range 126 {
		spec.Tools = append(spec.Tools, durable.ToolSpec{
			Name: fmt.Sprintf("static_%d", index), Parameters: json.RawMessage(`{"type":"object"}`),
		})
	}
	_, _, err := Build(t.Context(), nil, spec)
	if err == nil || !strings.Contains(err.Error(), "tool count 129 exceeds limit of 128") || deletes.Load() != 2 {
		t.Fatalf("global tool limit: err=%v deletes=%d", err, deletes.Load())
	}
	spec.Tools = spec.Tools[:125]
	if bindings := buildTestMCP(t, spec); len(bindings) != 127 {
		t.Fatalf("at the limit: bindings=%d", len(bindings))
	}
}

func TestMCPToolCallFormatsResults(t *testing.T) {
	server := testMCPServerWithLookup(
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			switch string(req.Params.Arguments) {
			case `{}`:
				return &mcp.CallToolResult{
					StructuredContent: map[string]any{"found": true},
					Content:           []mcp.Content{&mcp.TextContent{Text: "duplicate"}},
				}, nil
			case `{"mixed":true}`:
				return &mcp.CallToolResult{Content: []mcp.Content{
					&mcp.TextContent{Text: "first"},
					&mcp.ResourceLink{Name: "item", URI: "catalog://item"},
					&mcp.ImageContent{MIMEType: "image/png", Data: []byte("image bytes")},
					&mcp.AudioContent{MIMEType: "audio/wav", Data: []byte("audio bytes")},
					&mcp.EmbeddedResource{
						Resource: &mcp.ResourceContents{
							URI:  "catalog://embedded",
							Text: "hidden",
						},
					},
				}}, nil
			}
			return &mcp.CallToolResult{
				IsError: true,
				Content: []mcp.Content{&mcp.TextContent{Text: "try again"}},
			}, nil
		},
	)
	httpServer := newTestMCPServer(t, server, nil)
	lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
	for _, test := range []struct{ arguments, want string }{
		{arguments: `   `, want: `{"found":true}`},
		{arguments: `{"mixed":true}`, want: "first\ncatalog://item\n[unsupported content: image (image/png)]\n" +
			"[unsupported content: audio (audio/wav)]\n[unsupported content: embedded resource]"},
		{arguments: `{"error":true}`, want: `{"error":{"code":"MCP_TOOL_ERROR","message":"try again"}}`},
	} {
		got, err := lookup.InvokableRun(t.Context(), test.arguments)
		if err != nil || got != test.want {
			t.Fatalf("arguments=%s output=%s err=%v", test.arguments, got, err)
		}
	}
	message := mcpErrorResult(strings.Repeat("查&\n", mcpMaxResult/2))
	if len(message) > mcpMaxResult || !json.Valid([]byte(message)) || !utf8.ValidString(message) ||
		!strings.Contains(message, "[truncated:") {
		t.Fatalf("large error payload bytes=%d is invalid or missing the marker", len(message))
	}
}

func TestMCPReconnectsOnlyMissingPOSTSession(t *testing.T) {
	for _, test := range []struct {
		name string
		sse  bool
	}{
		{name: "POST 404 reconnects"}, {name: "GET 404 after dispatch does not reexecute", sse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var connects, calls, resumed atomic.Int32
			server := testMCPServerWithLookup(nil)
			httpServer := newTestMCPServer(
				t,
				server,
				func(w http.ResponseWriter, r *http.Request, method string) bool {
					switch method {
					case "initialize":
						connects.Add(1)
					case "tools/call":
						if calls.Add(1) != 1 {
							break
						}
						if test.sse {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "id: last-event\n\n")
						} else {
							http.Error(w, "session expired", http.StatusNotFound)
						}
						return true
					case http.MethodGet:
						if r.Header.Get("Last-Event-ID") == "last-event" {
							resumed.Add(1)
						}
						http.Error(w, "session expired after dispatch", http.StatusNotFound)
						return true
					}
					return false
				},
			)
			lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := lookup.InvokableRun(ctx, `{}`)
			if test.sse {
				if err == nil || connects.Load() != 1 || calls.Load() != 1 || resumed.Load() == 0 {
					t.Fatalf(
						"err=%v connects=%d calls=%d resumed=%d",
						err,
						connects.Load(),
						calls.Load(),
						resumed.Load(),
					)
				}
			} else if err != nil || result != `{}` || connects.Load() != 2 || calls.Load() != 2 {
				t.Fatalf(
					"result=%q err=%v connects=%d calls=%d",
					result,
					err,
					connects.Load(),
					calls.Load(),
				)
			}
		})
	}
}

func TestMCPResponseBodyLimit(t *testing.T) {
	for _, contentType := range []string{"application/json", "text/event-stream"} {
		t.Run(contentType, func(t *testing.T) {
			httpServer := newTestMCPServer(t, testMCPServerWithLookup(nil),
				func(w http.ResponseWriter, _ *http.Request, method string) bool {
					if method != "tools/call" {
						return false
					}
					w.Header().Set("Content-Type", contentType)
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, strings.Repeat("x", mcpMaxResponse+1))
					return true
				})
			lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := lookup.InvokableRun(ctx, "{}"); !errors.Is(err, mcpBodyLimit) {
				t.Fatalf("oversized HTTP body: %v", err)
			}
		})
	}
}

func TestMCPHTTPHeadersAndRedirect(t *testing.T) {
	t.Setenv("TEST_MCP_CREDENTIAL", "mcp-secret")
	for _, forward := range []bool{false, true} {
		t.Run(fmt.Sprintf("forwardScope=%t", forward), func(t *testing.T) {
			var requests []testMCPRequest
			var mu sync.Mutex
			server := testMCPServerWithLookup(nil)
			httpServer := newTestMCPServer(
				t,
				server,
				func(_ http.ResponseWriter, r *http.Request, method string) bool {
					mu.Lock()
					requests = append(
						requests,
						testMCPRequest{Method: method, Header: r.Header.Clone()},
					)
					mu.Unlock()
					return false
				},
			)
			spec := testMCPSpec(httpServer.URL)
			spec.MCPServers[0].TokenEnv = "TEST_MCP_CREDENTIAL"
			spec.MCPServers[0].ForwardScope = forward
			spec.Scope = &durable.Scope{TenantID: "tenant"}
			lookup := buildTestMCP(t, spec)[testMCPTool].Tool.(*mcpTool)
			if _, err := lookup.InvokableRun(t.Context(), "{}"); err != nil {
				t.Fatal(err)
			}
			if err := lookup.CloseSession(t.Context()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(requests) != 5 {
				t.Fatalf("want handshake, list, call and delete, got %v", requests)
			}
			for _, request := range requests {
				header := request.Header
				if header.Get("Authorization") != "Bearer mcp-secret" ||
					header.Get("Idempotency-Key") != "" {
					t.Fatalf("%s: authorization/idempotency headers: %v", request.Method, header)
				}
				if forward && (!strings.Contains(header.Get("X-Agentic-Scope"), `"tenant"`) ||
					header.Get("X-Agentic-Epoch") != "0") {
					t.Fatalf("%s: scope was not forwarded: %v", request.Method, header)
				}
				if !forward &&
					(header.Get("X-Agentic-Scope") != "" || header.Get("X-Agentic-Run-ID") != "" ||
						header.Get("X-Agentic-Epoch") != "") {
					t.Fatalf("%s: scope leaked: %v", request.Method, header)
				}
			}
		})
	}
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	spec := testMCPSpec(redirect.URL)
	spec.MCPServers[0].TokenEnv = "TEST_MCP_CREDENTIAL"
	if _, _, err := Build(t.Context(), nil, spec); err == nil || followed.Load() != 0 {
		t.Fatalf("redirect result: err=%v followed=%d", err, followed.Load())
	}
}

func TestMCPRPCValidationErrorsAreCorrectable(t *testing.T) {
	for _, code := range []int64{-32601, -32602} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			server := testMCPServerWithLookup(nil)
			server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
				return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
					if method == "tools/call" {
						return nil, &jsonrpc.Error{Code: code, Message: "invalid call"}
					}
					return next(ctx, method, req)
				}
			})
			httpServer := newTestMCPServer(t, server, nil)
			lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
			_, err := lookup.InvokableRun(t.Context(), "{}")
			var correctable interface{ Correctable() bool }
			if !errors.As(err, &correctable) || !correctable.Correctable() {
				t.Fatalf("RPC %d: %v", code, err)
			}
		})
	}
}

func TestMCPInputSchemaTypeValidation(t *testing.T) {
	server := testMCPServerWithLookup(nil)
	serveMCPToolPages(server, func(string) *mcp.ListToolsResult {
		return &mcp.ListToolsResult{Tools: []*mcp.Tool{
			{
				Name: "missing-type",
				InputSchema: map[string]any{
					"properties": map[string]any{"q": map[string]any{"type": "string"}},
				},
			},
			{Name: "string-type", InputSchema: map[string]any{"type": "string"}},
		}}
	})
	httpServer := newTestMCPServer(t, server, nil)
	spec := testMCPSpec(httpServer.URL)
	bindings := buildTestMCP(t, spec)
	if len(bindings) != 1 {
		t.Fatalf("expected only object-compatible tool, got %+v", bindings)
	}
	info, err := bindings["mcp__catalog__missing-type"].Tool.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	params, err := info.ParamsOneOf.ToJSONSchema()
	if err != nil || params.Type != "object" {
		t.Fatalf("missing type was not defaulted to object: %+v err=%v", params, err)
	}

	spec.MCPServers[0].AllowedTools = []string{"string-type"}
	if _, _, err := Build(
		t.Context(),
		nil,
		spec,
	); err == nil ||
		!strings.Contains(err.Error(), `must have type "object"`) {
		t.Fatalf("allowlisted non-object schema error=%v", err)
	}
}

func TestMCPProtocolVersionDowngradeAndSSEResume(t *testing.T) {
	t.Run("negotiates older 2025-03-26 protocol", func(t *testing.T) {
		var sawVersion atomic.Value
		server := testMCPServerWithLookup(nil)
		httpServer := newTestMCPServer(
			t,
			server,
			func(w http.ResponseWriter, r *http.Request, method string) bool {
				if method == "initialize" {
					var req struct {
						ID any `json:"id"`
					}
					body, _ := io.ReadAll(r.Body)
					_ = json.Unmarshal(body, &req)
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Mcp-Session-Id", "legacy-session")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"jsonrpc": "2.0",
						"id":      req.ID,
						"result": map[string]any{
							"protocolVersion": "2025-03-26",
							"capabilities":    map[string]any{"tools": map[string]any{}},
							"serverInfo": map[string]any{
								"name":    "legacy",
								"version": "1",
							},
						},
					})
					return true
				}
				if method == "tools/list" {
					sawVersion.Store(r.Header.Get("Mcp-Protocol-Version"))
					var req struct {
						ID any `json:"id"`
					}
					body, _ := io.ReadAll(r.Body)
					_ = json.Unmarshal(body, &req)
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{
						"jsonrpc": "2.0",
						"id":      req.ID,
						"result": map[string]any{
							"tools": []map[string]any{
								{
									"name":        "lookup",
									"inputSchema": map[string]any{"type": "object"},
								},
							},
						},
					})
					return true
				}
				if r.Method == http.MethodDelete || method == "notifications/initialized" {
					w.WriteHeader(http.StatusAccepted)
					return true
				}
				return false
			},
		)
		bindings := buildTestMCP(t, testMCPSpec(httpServer.URL))
		if _, ok := bindings[testMCPTool]; !ok {
			t.Fatalf("missing negotiated tool: %+v", bindings)
		}
		if got, _ := sawVersion.Load().(string); got != "2025-03-26" {
			t.Fatalf("Mcp-Protocol-Version=%q, want 2025-03-26", got)
		}
	})

	t.Run("POST 404 with JSON-RPC error body still reconnects", func(t *testing.T) {
		var connects, calls atomic.Int32
		server := testMCPServerWithLookup(nil)
		httpServer := newTestMCPServer(
			t,
			server,
			func(w http.ResponseWriter, r *http.Request, method string) bool {
				switch method {
				case "initialize":
					connects.Add(1)
				case "tools/call":
					if calls.Add(1) == 1 {
						var req struct {
							ID any `json:"id"`
						}
						body, _ := io.ReadAll(r.Body)
						_ = json.Unmarshal(body, &req)
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusNotFound)
						_ = json.NewEncoder(w).Encode(map[string]any{
							"jsonrpc": "2.0",
							"id":      req.ID,
							"error": map[string]any{
								"code":    -32000,
								"message": "Session not found",
							},
						})
						return true
					}
				}
				return false
			},
		)
		lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
		got, err := lookup.InvokableRun(t.Context(), `{}`)
		if err != nil || got != `{}` || connects.Load() != 2 || calls.Load() != 2 {
			t.Fatalf("got=%q err=%v connects=%d calls=%d", got, err, connects.Load(), calls.Load())
		}
	})

	t.Run("SSE disconnect resumes via GET Last-Event-ID without reexecuting POST", func(t *testing.T) {
		var calls, resumed atomic.Int32
		var callID atomic.Value
		server := testMCPServerWithLookup(nil)
		httpServer := newTestMCPServer(
			t,
			server,
			func(w http.ResponseWriter, r *http.Request, method string) bool {
				switch method {
				case "tools/call":
					calls.Add(1)
					var req struct {
						ID any `json:"id"`
					}
					body, _ := io.ReadAll(r.Body)
					_ = json.Unmarshal(body, &req)
					callID.Store(req.ID)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "id: prime-1\nretry: 10\n\n")
					w.(http.Flusher).Flush()
					return true
				case http.MethodGet:
					if r.Header.Get("Last-Event-ID") != "prime-1" {
						t.Errorf("unexpected Last-Event-ID: %q", r.Header.Get("Last-Event-ID"))
					}
					resumed.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					payload, _ := json.Marshal(map[string]any{
						"jsonrpc": "2.0",
						"id":      callID.Load(),
						"result": map[string]any{
							"content": []map[string]any{
								{"type": "text", "text": "resumed ok"},
							},
						},
					})
					_, _ = fmt.Fprintf(w, "id: done-2\ndata: %s\n\n", payload)
					w.(http.Flusher).Flush()
					return true
				}
				return false
			},
		)
		lookup := buildTestMCP(t, testMCPSpec(httpServer.URL))[testMCPTool].Tool
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		got, err := lookup.InvokableRun(ctx, `{}`)
		if err != nil || got != "resumed ok" || calls.Load() != 1 || resumed.Load() != 1 {
			t.Fatalf("got=%q err=%v calls=%d resumed=%d", got, err, calls.Load(), resumed.Load())
		}
	})
}
