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
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	fsTools "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	aioBackend "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/aio"
	sandboxBackend "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// Build uses the same Eino model/tool interfaces as agentic-kit. OpenAI-compatible private model endpoints are supported.
func Build(
	ctx context.Context,
	store *durable.Store,
	spec durable.Spec,
) (llm model.ToolCallingChatModel, bindings map[string]engine.Binding, err error) {
	var environment *sandboxBackend.Backend
	defer func() {
		if err != nil && environment != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			err = errors.Join(err, environment.Close(closeCtx))
		}
	}()
	llm, err = buildModel(ctx, spec.Model)
	if err != nil {
		return nil, nil, err
	}
	bindings = make(map[string]engine.Binding, len(spec.Tools))
	for _, t := range spec.Tools {
		if t.Kind != "" && t.Kind != "HTTP" {
			return nil, nil, fmt.Errorf("unsupported tool kind %q for %s", t.Kind, t.Name)
		}
		var params jsonschema.Schema
		if err = json.Unmarshal(t.Parameters, &params); err != nil {
			return nil, nil, err
		}
		bindings[t.Name] = engine.Binding{
			Tool: &httpTool{
				spec:   t,
				scope:  spec.Scope,
				params: &params,
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
			policy := durable.ReadOnly
			if info.Name == "write_file" || info.Name == "edit_file" || info.Name == "execute" {
				policy = durable.Unsafe
			}
			invokable := base.(tool.InvokableTool)
			if environment != nil {
				invokable = &sandboxTool{InvokableTool: invokable, Backend: environment}
			}
			bindings[info.Name] = engine.Binding{Tool: invokable, Policy: policy}
		}
	}
	for name, binding := range bindings {
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
	key := os.Getenv(spec.APIKeyEnv)
	if spec.APIKeyEnv != "" && key == "" {
		return nil, errors.New("model credential environment variable is not configured")
	}
	return openai.NewChatModel(ctx, &openai.ChatModelConfig{
		BaseURL:     spec.BaseURL,
		APIKey:      key,
		Model:       spec.Name,
		Timeout:     2 * time.Minute,
		Temperature: spec.Temperature,
		TopP:        spec.TopP,
		MaxTokens:   spec.MaxTokens,
	})
}

type sandboxTool struct {
	tool.InvokableTool
	*sandboxBackend.Backend
}

type httpTool struct {
	scope  *durable.Scope
	spec   durable.ToolSpec
	params *jsonschema.Schema
	client *http.Client
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
		token := os.Getenv(t.spec.TokenEnv)
		if token == "" {
			return "", errors.New("tool credential is not configured")
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
	tenantID := ""
	if spec.Scope != nil {
		tenantID = spec.Scope.TenantID
	}
	backend, err := sandboxBackend.New(ctx, store, sandboxBackend.Config{
		Endpoint:             spec.Filesystem.URL,
		Token:                os.Getenv(spec.Filesystem.TokenEnv),
		Workspace:            spec.Filesystem.Workspace,
		ProfileID:            spec.Filesystem.ProfileID,
		RequiredSave:         rc.GoalID != "" || spec.Filesystem.RestoreFromSandboxID != "",
		RestoreFromSandboxID: spec.Filesystem.RestoreFromSandboxID,
		TenantID:             tenantID,
		OwnerID:              rc.OwnerID,
		AcquireKey:           fmt.Sprintf("%s:env:%d", rc.RunID, rc.Epoch),
		RunID:                rc.RunID,
		WorkerID:             rc.WorkerID,
		Epoch:                rc.Epoch,
	})
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
