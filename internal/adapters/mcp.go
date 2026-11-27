package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/agent"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/errgroup"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const (
	mcpDiscoveryTimeout = 30 * time.Second
	mcpCallTimeout      = 2 * time.Minute
	mcpCloseTimeout     = 5 * time.Second
	mcpMaxPages         = 10
	mcpMaxServerTools   = 64
	mcpMaxDescription   = 2 << 10
	mcpMaxSchema        = 16 << 10
	mcpMaxResult        = 4 << 20
	mcpMaxResponse      = 8 << 20
	mcpTruncation       = "\n[truncated: MCP tool result exceeds 4 MiB]"
)

var (
	mcpToolName      = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	mcpBodyLimit     = errors.New("MCP HTTP response exceeds 8 MiB")
	mcpHTTPTransport = func() *http.Transport {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = mcpCallTimeout
		return transport
	}()
)

type retryableMCPError struct{ err error }

func (e retryableMCPError) Error() string { return e.err.Error() }

func (e retryableMCPError) Unwrap() error { return e.err }

func (retryableMCPError) Retryable() bool { return true }

type correctableMCPError struct{ err error }

func (e correctableMCPError) Error() string { return e.err.Error() }

func (e correctableMCPError) Unwrap() error { return e.err }

func (correctableMCPError) Correctable() bool { return true }

type mcpRequestKey struct{}

type mcpRequestState struct {
	call        bool
	sent        atomic.Bool
	sessionGone atomic.Bool
	interrupted atomic.Bool
	overLimit   atomic.Bool
}

type mcpTransport struct {
	token        string
	forwardScope bool
	scope        string
	execution    engine.Execution
}

func (t mcpTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	req = req.Clone(ctx)
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	if t.forwardScope {
		req.Header.Set("X-Agentic-Run-ID", t.execution.RunID)
		req.Header.Set("X-Agentic-Epoch", strconv.FormatInt(t.execution.Epoch, 10))
		if t.scope != "" {
			req.Header.Set("X-Agentic-Scope", t.scope)
		}
	}
	state, _ := ctx.Value(mcpRequestKey{}).(*mcpRequestState)
	toolCall := state != nil && state.call && isMCPToolCall(req)
	if toolCall {
		state.sent.Store(true)
		if operationID := engine.OperationID(ctx); operationID != "" {
			req.Header.Set("Idempotency-Key", operationID)
		}
		req.GetBody = nil
	}
	resp, err := mcpHTTPTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if toolCall && resp.StatusCode == http.StatusNotFound && req.Header.Get("Mcp-Session-Id") != "" {
		state.sessionGone.Store(true)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		resp.Body.Close()
		return nil, retryableMCPError{
			err: fmt.Errorf("MCP HTTP status %d %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
		}
	}
	if resp.ContentLength > mcpMaxResponse {
		resp.Body.Close()
		if state != nil {
			state.overLimit.Store(true)
		}
		return nil, mcpBodyLimit
	}
	resp.Body = &mcpResponseBody{ReadCloser: resp.Body, remaining: mcpMaxResponse, state: state}
	return resp, nil
}

func isMCPToolCall(req *http.Request) bool {
	if req.Method != http.MethodPost || req.GetBody == nil {
		return false
	}
	body, err := req.GetBody()
	if err != nil {
		return false
	}
	defer body.Close()
	var message struct {
		Method string `json:"method"`
	}
	return json.NewDecoder(body).Decode(&message) == nil && message.Method == "tools/call"
}

type mcpResponseBody struct {
	io.ReadCloser
	remaining int
	state     *mcpRequestState
}

func (body *mcpResponseBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if body.remaining == 0 {
		var extra [1]byte
		count, err := body.ReadCloser.Read(extra[:])
		if count > 0 {
			if body.state != nil {
				body.state.overLimit.Store(true)
			}
			return 0, mcpBodyLimit
		}
		return 0, err
	}
	count, err := body.ReadCloser.Read(buffer[:min(len(buffer), body.remaining)])
	body.remaining -= count
	if body.state != nil && transientMCPNetworkError(err) {
		body.state.interrupted.Store(true)
	}
	return count, err
}

func transientMCPNetworkError(err error) bool {
	var networkError net.Error
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &networkError)
}

func mcpDiscoveryError(err error, state *mcpRequestState) error {
	if state.overLimit.Load() {
		return mcpBodyLimit
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		if transientMCPNetworkError(urlError.Err) || errors.Is(urlError.Err, io.EOF) {
			return retryableMCPError{err: err}
		}
		return err
	}
	if state.interrupted.Load() || errors.Is(err, mcp.ErrSessionMissing) ||
		errors.Is(
			err,
			context.DeadlineExceeded,
		) || strings.HasSuffix(err.Error(), "request terminated without response") {
		return retryableMCPError{err: err}
	}
	return err
}

type mcpSession struct {
	client    *mcp.Client
	transport *mcp.StreamableClientTransport

	mu        sync.Mutex
	current   *mcp.ClientSession
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newMCPSession(
	ctx context.Context,
	server durable.MCPServerSpec,
	scope *durable.Scope,
	credentials CredentialPolicy,
) (*mcpSession, error) {
	var token, scopeHeader string
	if server.TokenEnv != "" {
		var err error
		token, err = credentials.Resolve(server.TokenEnv, "MCP credential")
		if err != nil {
			return nil, err
		}
	}
	if server.ForwardScope && scope != nil {
		raw, err := json.Marshal(scope)
		if err != nil {
			return nil, err
		}
		scopeHeader = string(raw)
	}
	handle := &mcpSession{
		client: mcp.NewClient(
			&mcp.Implementation{Name: "agentic-kit-worker", Version: "1.0.0"},
			&mcp.ClientOptions{Capabilities: &mcp.ClientCapabilities{}},
		),
		transport: &mcp.StreamableClientTransport{
			Endpoint: server.URL,
			HTTPClient: &http.Client{
				Transport: mcpTransport{
					token: token, forwardScope: server.ForwardScope,
					scope: scopeHeader, execution: engine.ExecutionContext(ctx),
				},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			},
			MaxRetries: 2, DisableStandaloneSSE: true,
		},
		closeDone: make(chan struct{}),
	}
	session, err := handle.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	handle.current = session
	return handle, nil
}

func (s *mcpSession) connect(ctx context.Context) (*mcp.ClientSession, error) {
	ctx, cancel := context.WithTimeout(ctx, mcpDiscoveryTimeout)
	defer cancel()
	state := &mcpRequestState{}
	session, err := s.client.Connect(context.WithValue(ctx, mcpRequestKey{}, state), s.transport,
		&mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
	if err != nil {
		return nil, mcpDiscoveryError(err, state)
	}
	return session, nil
}

func (s *mcpSession) CloseSession(ctx context.Context) error {
	s.closeOnce.Do(func() {
		go func() {
			defer close(s.closeDone)
			s.mu.Lock()
			s.closed = true
			session := s.current
			s.current = nil
			s.mu.Unlock()
			if session != nil {
				s.closeErr = session.Close()
			}
		}()
	})
	ctx, cancel := context.WithTimeout(ctx, mcpCloseTimeout)
	defer cancel()
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *mcpSession) reconnect(ctx context.Context, expired *mcp.ClientSession) (*mcp.ClientSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, mcp.ErrConnectionClosed
	}
	if s.current != nil && s.current != expired {
		return s.current, nil
	}
	if s.current == expired {
		_ = expired.Close()
		s.current = nil
	}
	session, err := s.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconnect MCP session: %w", err)
	}
	s.current = session
	return session, nil
}

func (s *mcpSession) call(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, mcpCallTimeout)
	defer cancel()
	s.mu.Lock()
	session := s.current
	s.mu.Unlock()
	if session == nil {
		return nil, mcp.ErrConnectionClosed
	}
	state := &mcpRequestState{call: true}
	result, err := session.CallTool(context.WithValue(ctx, mcpRequestKey{}, state), params)
	if err != nil && (state.sessionGone.Load() || (errors.Is(err, mcp.ErrSessionMissing) && !state.sent.Load())) {
		session, err = s.reconnect(ctx, session)
		if err != nil {
			return nil, err
		}
		state = &mcpRequestState{call: true}
		result, err = session.CallTool(context.WithValue(ctx, mcpRequestKey{}, state), params)
	}
	if err != nil && state.overLimit.Load() {
		return nil, mcpBodyLimit
	}
	var rpcError *jsonrpc.Error
	if errors.As(err, &rpcError) && (rpcError.Code == -32601 || rpcError.Code == -32602) {
		return nil, correctableMCPError{err: err}
	}
	return result, err
}

type mcpDiscovery struct {
	session  *mcpSession
	bindings map[string]engine.Binding
}

func discoverMCPServers(
	ctx context.Context,
	spec durable.Spec,
	credentials CredentialPolicy,
) ([]mcpDiscovery, error) {
	ctx, cancel := context.WithTimeout(ctx, mcpDiscoveryTimeout)
	defer cancel()
	discovered := make([]mcpDiscovery, len(spec.MCPServers))
	group, ctx := errgroup.WithContext(ctx)
	for index, server := range spec.MCPServers {
		group.Go(func() error {
			session, err := newMCPSession(ctx, server, spec.Scope, credentials)
			if err == nil {
				discovered[index].session = session
				discovered[index].bindings, err = session.discover(ctx, server)
			}
			if err != nil {
				return fmt.Errorf("MCP server %q: %w", server.Name, err)
			}
			return nil
		})
	}
	return discovered, group.Wait()
}

func (s *mcpSession) discover(ctx context.Context, server durable.MCPServerSpec) (map[string]engine.Binding, error) {
	bindings := make(map[string]engine.Binding)
	allowed := make(map[string]bool, len(server.AllowedTools))
	for _, name := range server.AllowedTools {
		allowed[name] = true
	}
	seen := make(map[string]bool)
	cursors := make(map[string]bool)
	var configErr error
	var cursor string
	for range mcpMaxPages {
		state := &mcpRequestState{}
		listCtx, cancel := context.WithTimeout(ctx, mcpDiscoveryTimeout)
		page, err := s.current.ListTools(
			context.WithValue(listCtx, mcpRequestKey{}, state),
			&mcp.ListToolsParams{Cursor: cursor},
		)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("list tools: %w", mcpDiscoveryError(err, state))
		}
		for _, remote := range page.Tools {
			if remote == nil {
				return nil, errors.New("tools/list returned a null tool")
			}
			name := remote.Name
			if seen[name] {
				return nil, fmt.Errorf("duplicate remote tool %q", name)
			}
			seen[name] = true
			if len(allowed) > 0 && !allowed[name] {
				continue
			}
			fullName := agent.FormatMCPToolName(server.Name, name)
			binding, err := s.binding(server, remote, fullName)
			if err != nil {
				if allowed[name] && configErr == nil {
					configErr = fmt.Errorf("allowlisted tool %q: %w", name, err)
				}
				continue
			}
			bindings[fullName] = binding
			if len(bindings) > mcpMaxServerTools {
				return nil, fmt.Errorf("server exposes more than %d MCP tools", mcpMaxServerTools)
			}
		}
		cursor = page.NextCursor
		if cursor == "" {
			for _, name := range server.AllowedTools {
				if configErr == nil && !seen[name] {
					configErr = fmt.Errorf("allowlisted tool %q was not returned", name)
				}
			}
			if configErr != nil {
				names := make([]string, 0, len(seen))
				for name := range seen {
					names = append(names, name)
				}
				slices.Sort(names)
				return nil, fmt.Errorf("%w (available tools: %q)", configErr, names)
			}
			return bindings, nil
		}
		if cursors[cursor] {
			return nil, errors.New("tools/list returned a repeated next cursor")
		}
		cursors[cursor] = true
	}
	return nil, fmt.Errorf("tools/list exceeds %d pages", mcpMaxPages)
}

func (s *mcpSession) binding(server durable.MCPServerSpec, remote *mcp.Tool, name string) (engine.Binding, error) {
	if !mcpToolName.MatchString(remote.Name) || len(name) > 64 {
		return engine.Binding{}, errors.New("invalid MCP tool name")
	}
	raw, err := json.Marshal(remote.InputSchema)
	if err != nil {
		return engine.Binding{}, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return engine.Binding{}, fmt.Errorf("MCP input schema must be a JSON object: %w", err)
	}
	delete(object, "$schema")
	if len(object) == 0 {
		raw = []byte(`{"type":"object","properties":{}}`)
	} else {
		if rawType, ok := object["type"]; !ok {
			object["type"] = json.RawMessage(`"object"`)
		} else {
			var schemaType string
			if err := json.Unmarshal(rawType, &schemaType); err != nil || schemaType != "object" {
				return engine.Binding{}, errors.New(`MCP input schema must have type "object"`)
			}
		}
		raw, err = json.Marshal(object)
		if err != nil {
			return engine.Binding{}, err
		}
	}
	if len(raw) > mcpMaxSchema {
		return engine.Binding{}, errors.New("MCP input schema exceeds 16 KiB")
	}
	var params jsonschema.Schema
	if err := json.Unmarshal(raw, &params); err != nil {
		return engine.Binding{}, err
	}
	info := &schema.ToolInfo{
		Name: name, Desc: truncateMCPString(remote.Description, mcpMaxDescription),
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&params),
	}
	validate, err := engine.CompileArguments(info)
	if err != nil {
		return engine.Binding{}, fmt.Errorf("compile MCP input schema: %w", err)
	}
	return engine.Binding{
		Tool: &mcpTool{mcpSession: s, name: remote.Name, info: info}, Policy: server.PolicyFor(remote.Name),
		Validate: func(args string) error { return validate(normalizeMCPArguments(args)) },
	}, nil
}

type mcpTool struct {
	*mcpSession
	name string
	info *schema.ToolInfo
}

func (t *mcpTool) Info(context.Context) (*schema.ToolInfo, error) { return t.info, nil }

func normalizeMCPArguments(args string) string {
	if strings.TrimSpace(args) == "" {
		return "{}"
	}
	return args
}

func (t *mcpTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (string, error) {
	args = normalizeMCPArguments(args)
	var arguments map[string]any
	decoder := json.NewDecoder(strings.NewReader(args))
	decoder.UseNumber()
	if err := decoder.Decode(&arguments); err != nil {
		return "", correctableMCPError{err: err}
	}
	if arguments == nil || !json.Valid([]byte(args)) {
		return "", correctableMCPError{err: errors.New("MCP tool arguments must be a JSON object")}
	}
	result, err := t.call(ctx, &mcp.CallToolParams{Name: t.name, Arguments: arguments})
	if err != nil {
		return "", err
	}
	return formatMCPResult(result)
}

func formatMCPResult(result *mcp.CallToolResult) (string, error) {
	if result.IsError {
		message := mcpContentText(result.Content)
		if message == "" {
			message = "MCP tool reported an error"
		}
		return mcpErrorResult(message), nil
	}
	var output string
	if result.StructuredContent != nil {
		raw, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return "", err
		}
		output = string(raw)
	} else {
		output = mcpContentText(result.Content)
	}
	if len(output) > mcpMaxResult {
		output = truncateMCPString(output, mcpMaxResult-len(mcpTruncation)) + mcpTruncation
	}
	return output, nil
}

func mcpContentText(content []mcp.Content) string {
	lines := make([]string, 0, len(content))
	for _, item := range content {
		var line string
		switch item := item.(type) {
		case *mcp.TextContent:
			line = item.Text
		case *mcp.ResourceLink:
			line = item.URI
		case *mcp.ImageContent:
			line = "[unsupported content: image (" + item.MIMEType + ")]"
		case *mcp.AudioContent:
			line = "[unsupported content: audio (" + item.MIMEType + ")]"
		case *mcp.EmbeddedResource:
			line = "[unsupported content: embedded resource]"
		default:
			line = "[unsupported content]"
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func truncateMCPString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit]
}

func mcpErrorResult(message string) string {
	const prefix = `{"error":{"code":"MCP_TOOL_ERROR","message":`
	const suffix = `}}`
	quoted, _ := json.Marshal(message)
	if len(prefix)+len(quoted)+len(suffix) <= mcpMaxResult {
		return prefix + string(quoted) + suffix
	}
	marker, _ := json.Marshal(mcpTruncation)
	limit := mcpMaxResult - len(prefix) - len(suffix) - len(marker) + 1
	end := 1
	for end < limit {
		width := 1
		if quoted[end] == '\\' {
			width = 2
			if quoted[end+1] == 'u' {
				width = 6
			}
		} else {
			_, width = utf8.DecodeRune(quoted[end:])
		}
		if end+width > limit {
			break
		}
		end += width
	}
	return prefix + string(quoted[:end]) + string(marker[1:]) + suffix
}
