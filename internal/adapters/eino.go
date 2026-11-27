package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	fsTools "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	aioBackend "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/aio"
	sandboxBackend "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const defaultFirstChunkTimeout = 5 * time.Minute

type BuilderOptions struct {
	Credentials             CredentialPolicy
	StreamFirstChunkTimeout time.Duration
	StreamIdleTimeout       time.Duration
}

type Builder struct {
	credentials        CredentialPolicy
	streamingClient    *http.Client
	nonStreamingClient *http.Client
}

func NewBuilder(opts BuilderOptions) *Builder {
	firstChunk := opts.StreamFirstChunkTimeout
	if firstChunk <= 0 {
		if opts.StreamIdleTimeout > 0 {
			firstChunk = opts.StreamIdleTimeout
		} else {
			firstChunk = defaultFirstChunkTimeout
		}
	}
	headerTimeout := max(firstChunk, 2*time.Minute)
	streamTransport := http.DefaultTransport.(*http.Transport).Clone()
	streamTransport.ResponseHeaderTimeout = headerTimeout
	syncTransport := http.DefaultTransport.(*http.Transport).Clone()
	syncTransport.ResponseHeaderTimeout = headerTimeout
	return &Builder{
		credentials:        opts.Credentials,
		streamingClient:    &http.Client{Transport: streamTransport},
		nonStreamingClient: &http.Client{Transport: syncTransport, Timeout: max(15*time.Minute, headerTimeout)},
	}
}

var defaultBuilder = NewBuilder(BuilderOptions{})

// Build uses the same Eino model/tool interfaces as agentic-kit. OpenAI-compatible private model endpoints are supported.
func Build(
	ctx context.Context,
	store *durable.Store,
	spec durable.Spec,
) (llm model.ToolCallingChatModel, bindings map[string]engine.Binding, err error) {
	return defaultBuilder.Build(ctx, store, spec)
}

func (b *Builder) Build(
	ctx context.Context,
	store *durable.Store,
	spec durable.Spec,
) (llm model.ToolCallingChatModel, bindings map[string]engine.Binding, err error) {
	var environment *sandboxBackend.Backend
	var discovered []mcpDiscovery
	defer func() {
		if err == nil {
			return
		}
		var closing sync.WaitGroup
		for _, server := range discovered {
			if server.session != nil {
				closing.Go(func() { _ = server.session.CloseSession(context.WithoutCancel(ctx)) })
			}
		}
		if environment != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			err = errors.Join(err, environment.Close(closeCtx))
		}
		closing.Wait()
	}()
	credentials := b.credentials
	if spec.Filesystem != nil {
		credentials = credentials.WithDenied(spec.Filesystem.TokenEnv)
	}
	llm, err = b.buildModel(ctx, spec.Model, credentials)
	if err != nil {
		return nil, nil, err
	}
	bindings = make(map[string]engine.Binding, len(spec.Tools))
	for _, t := range spec.Tools {
		if t.Kind != "" && t.Kind != "HTTP" {
			return nil, nil, fmt.Errorf("unsupported tool kind %q for %s", t.Kind, t.Name)
		}
		if err = credentials.Validate(t.TokenEnv); err != nil {
			return nil, nil, fmt.Errorf("tool %s: %w", t.Name, err)
		}
		var params jsonschema.Schema
		if err = json.Unmarshal(t.Parameters, &params); err != nil {
			return nil, nil, err
		}
		bindings[t.Name] = engine.Binding{
			Tool: &httpTool{
				scope:       spec.Scope,
				spec:        t,
				params:      &params,
				credentials: credentials,
				client: &http.Client{
					Timeout:       2 * time.Minute,
					CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
				},
			},
			Policy: t.Policy,
			EndRun: t.EndRun,
		}
	}
	if len(spec.Toolsets) > 0 {
		if spec.Filesystem == nil {
			return nil, nil, errors.New("bound filesystem is required")
		}
		var backend interface {
			filesystem.Backend
			filesystem.Shell
		}
		switch spec.Filesystem.Provider {
		case "AIO":
			b, bErr := aioBackend.NewWithClient(
				spec.Filesystem.URL,
				os.Getenv(spec.Filesystem.TokenEnv),
				spec.Filesystem.Workspace,
				&http.Client{
					Transport:     executionTransport{scope: spec.Scope},
					CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
				},
			)
			if bErr != nil {
				return nil, nil, bErr
			}
			if bErr = b.Ready(ctx); bErr != nil {
				return nil, nil, bErr
			}
			backend = b
		case "Sandbox":
			b, bErr := buildSandboxBackend(ctx, store, spec)
			if bErr != nil {
				return nil, nil, bErr
			}
			backend = b
		default:
			return nil, nil, fmt.Errorf("unsupported filesystem provider %q", spec.Filesystem.Provider)
		}
		environment, _ = backend.(*sandboxBackend.Backend)
		middleware, err := fsTools.New(ctx, &fsTools.MiddlewareConfig{Backend: backend, Shell: backend})
		if err != nil {
			return nil, nil, err
		}
		_, toolCtx, err := middleware.BeforeAgent(ctx, &adk.ChatModelAgentContext{})
		if err != nil {
			return nil, nil, err
		}
		for _, base := range toolCtx.Tools {
			info, err := base.Info(ctx)
			if err != nil {
				return nil, nil, err
			}
			if _, exists := bindings[info.Name]; exists {
				return nil, nil, fmt.Errorf("duplicate filesystem tool: %s", info.Name)
			}
			binding := engine.Binding{Policy: durable.ReadOnly}
			switch info.Name {
			case fsTools.ToolNameWriteFile:
				binding.Policy, binding.FileAction = durable.Unsafe, "written"
			case fsTools.ToolNameEditFile:
				binding.Policy, binding.FileAction = durable.Unsafe, "edited"
			case fsTools.ToolNameExecute:
				binding.Policy = durable.Unsafe
			}
			invokable := base.(tool.InvokableTool)
			if environment != nil {
				invokable = &sandboxTool{InvokableTool: invokable, Backend: environment}
			}
			binding.Tool = invokable
			bindings[info.Name] = binding
		}
	}
	if len(spec.MCPServers) > 0 {
		discovered, err = discoverMCPServers(ctx, spec, credentials)
		if err != nil {
			return nil, nil, err
		}
		for _, server := range discovered {
			if len(server.bindings) == 0 {
				_ = server.session.CloseSession(context.WithoutCancel(ctx))
			}
			for name, binding := range server.bindings {
				if _, exists := bindings[name]; exists {
					return nil, nil, fmt.Errorf("duplicate MCP tool: %s", name)
				}
				bindings[name] = binding
			}
		}
		count := len(bindings)
		if _, exists := bindings[durable.ReadStepResultToolName]; !exists {
			count++
		}
		rc, _ := engine.BuildRunContext(ctx)
		if (len(spec.SubAgents) > 0 || spec.SelfDelegate) && rc.ParentRunID == "" {
			if _, exists := bindings[durable.DelegateSubAgentToolName]; !exists {
				count++
			}
		}
		if len(spec.Skills) > 0 {
			if _, exists := bindings[durable.LoadSkillToolName]; !exists {
				count++
			}
		}
		if count > 128 {
			return nil, nil, fmt.Errorf("tool count %d exceeds limit of 128", count)
		}
	}
	for name, binding := range bindings {
		if binding.Validate != nil {
			continue
		}
		info, err := binding.Tool.Info(ctx)
		if err != nil {
			return nil, nil, err
		}
		binding.Validate, err = engine.CompileArguments(info)
		if err != nil {
			return nil, nil, fmt.Errorf("compile %s arguments: %w", name, err)
		}
		bindings[name] = binding
	}
	return llm, bindings, nil
}

func buildModel(ctx context.Context, spec durable.ModelSpec) (model.ToolCallingChatModel, error) {
	return defaultBuilder.buildModel(ctx, spec, defaultBuilder.credentials)
}

func (b *Builder) buildModel(
	ctx context.Context,
	spec durable.ModelSpec,
	credentials CredentialPolicy,
) (model.ToolCallingChatModel, error) {
	client := b.streamingClient
	if !spec.Streaming() {
		client = b.nonStreamingClient
	}
	return newUsageModel(ctx, spec, client, credentials)
}

func newUsageModel(
	ctx context.Context,
	spec durable.ModelSpec,
	client *http.Client,
	credentials CredentialPolicy,
) (model.ToolCallingChatModel, error) {
	key, err := credentials.Resolve(spec.APIKeyEnv, "model credential")
	if err != nil {
		return nil, err
	}
	llm, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
		BaseURL:     spec.BaseURL,
		APIKey:      key,
		Model:       spec.Name,
		HTTPClient:  client,
		Temperature: spec.Temperature,
		TopP:        spec.TopP,
		MaxTokens:   spec.MaxTokens,
	})
	if err != nil {
		return nil, err
	}
	return &usageModel{ToolCallingChatModel: llm}, nil
}

type usageModel struct {
	model.ToolCallingChatModel
}

func (m *usageModel) IsCallbacksEnabled() bool {
	return components.IsCallbacksEnabled(m.ToolCallingChatModel)
}

func (m *usageModel) Generate(
	ctx context.Context,
	input []*schema.Message,
	opts ...model.Option,
) (*schema.Message, error) {
	opts = append(opts, openai.WithResponseMessageModifier(preserveUnknownUsage))
	return m.ToolCallingChatModel.Generate(ctx, input, opts...)
}

func (m *usageModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	bound, err := m.ToolCallingChatModel.WithTools(tools)
	if err != nil {
		return nil, err
	}
	return &usageModel{ToolCallingChatModel: bound}, nil
}

func preserveUnknownUsage(_ context.Context, message *schema.Message, body []byte) (*schema.Message, error) {
	var raw struct {
		Usage *json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, errors.Join(engine.ErrModelUsageUnavailable, err)
	}
	if raw.Usage == nil && message != nil && message.ResponseMeta != nil {
		message.ResponseMeta.Usage = nil
	}
	return message, nil
}

type sandboxTool struct {
	tool.InvokableTool
	*sandboxBackend.Backend
}

type httpTool struct {
	scope       *durable.Scope
	spec        durable.ToolSpec
	params      *jsonschema.Schema
	credentials CredentialPolicy
	client      *http.Client
}

func (t *httpTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name:        t.spec.Name,
		Desc:        t.spec.Description,
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(t.params),
	}, nil
}

func (t *httpTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (string, error) {
	if t.spec.Name == "ask_for_clarification" {
		return "", errors.New("clarification must be handled by the durable execution loop")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.spec.URL, bytes.NewReader([]byte(args)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", engine.OperationID(ctx))
	execution := engine.ExecutionContext(ctx)
	req.Header.Set("X-Agentic-Run-ID", execution.RunID)
	req.Header.Set("X-Agentic-Epoch", strconv.FormatInt(execution.Epoch, 10))
	if t.scope != nil {
		scope, err := json.Marshal(t.scope)
		if err != nil {
			return "", err
		}
		req.Header.Set("X-Agentic-Scope", string(scope))
	}
	if t.spec.TokenEnv != "" {
		token, err := t.credentials.Resolve(t.spec.TokenEnv, "tool credential")
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024+1))
	if err != nil {
		return "", err
	}
	if len(b) > 4*1024*1024 {
		return "", errors.New("tool result exceeds 4 MiB; return an artifact reference")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure struct {
			Error json.RawMessage `json:"error"`
		}
		var message string
		if json.Unmarshal(b, &failure) == nil {
			if json.Unmarshal(failure.Error, &message) != nil {
				var detail struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				}
				if json.Unmarshal(failure.Error, &detail) == nil && detail.Message != "" {
					message = detail.Message
					if detail.Code != "" {
						message = detail.Code + ": " + message
					}
				}
			}
		}
		if message != "" {
			text := []rune(message)
			if len(text) > 512 {
				message = string(text[:512]) + "…"
			}
			return "", fmt.Errorf("tool %s returned HTTP %d: %s", t.spec.Name, resp.StatusCode, message)
		}
		return "", fmt.Errorf("tool %s returned HTTP %d", t.spec.Name, resp.StatusCode)
	}
	return string(b), nil
}

// executionTransport binds every sandbox request to this trusted Run identity.
type executionTransport struct{ scope *durable.Scope }

func (t executionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	identity := engine.ExecutionContext(req.Context())
	copy.Header.Set("X-Agentic-Run-ID", identity.RunID)
	copy.Header.Set("X-Agentic-Epoch", strconv.FormatInt(identity.Epoch, 10))
	copy.Header.Set("Idempotency-Key", engine.OperationID(req.Context()))
	if t.scope != nil {
		raw, err := json.Marshal(t.scope)
		if err != nil {
			return nil, err
		}
		copy.Header.Set("X-Agentic-Scope", string(raw))
	}
	return http.DefaultTransport.RoundTrip(copy)
}

// buildSandboxBackend resolves the sandbox-center environment for this run
// under the caller's claim. The durable environment intent is recorded before
// any network call so a crash mid-acquire is reconciled, not leaked.
func buildSandboxBackend(
	ctx context.Context,
	store *durable.Store,
	spec durable.Spec,
) (*sandboxBackend.Backend, error) {
	rc, ok := engine.BuildRunContext(ctx)
	if !ok {
		return nil, errors.New("sandbox filesystem requires the worker run context")
	}
	if spec.Filesystem == nil {
		return nil, errors.New("sandbox filesystem binding is required")
	}
	backend, err := sandboxBackend.New(ctx, store, sandboxEnvironmentConfig(spec, rc))
	if err != nil {
		return nil, err
	}
	if err = backend.Ready(ctx); err != nil {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return nil, errors.Join(err, backend.Close(closeCtx))
	}
	return backend, nil
}

func sandboxEnvironmentConfig(spec durable.Spec, rc engine.RunContext) sandboxBackend.Config {
	tenantID := ""
	if spec.Scope != nil {
		tenantID = spec.Scope.TenantID
	}
	return sandboxBackend.Config{
		Endpoint:  spec.Filesystem.URL,
		Token:     os.Getenv(spec.Filesystem.TokenEnv),
		Workspace: spec.Filesystem.Workspace,
		ProfileID: spec.Filesystem.ProfileID,
		RequiredSave: (rc.GoalID != "" && rc.ParentRunID == "") ||
			spec.Filesystem.RestoreFromSandboxID != "",
		RestoreFromSandboxID: spec.Filesystem.RestoreFromSandboxID,
		TenantID:             tenantID,
		OwnerID:              rc.OwnerID,
		AcquireKey:           fmt.Sprintf("%s:env:%d", rc.RunID, rc.Epoch),
		RunID:                rc.RunID,
		WorkerID:             rc.WorkerID,
		Epoch:                rc.Epoch,
	}
}
