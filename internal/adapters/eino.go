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
	fsTools "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"

	aioBackend "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/aio"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// Build uses the same Eino model/tool interfaces as agentic-kit. OpenAI-compatible private model endpoints are supported.
func Build(ctx context.Context, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
	key := os.Getenv(spec.Model.APIKeyEnv)
	if spec.Model.APIKeyEnv != "" && key == "" {
		return nil, nil, errors.New("model credential environment variable is not configured")
	}
	llm, err := openai.NewChatModel(
		ctx,
		&openai.ChatModelConfig{
			BaseURL:     spec.Model.BaseURL,
			APIKey:      key,
			Model:       spec.Model.Name,
			Timeout:     2 * time.Minute,
			Temperature: spec.Model.Temperature,
			TopP:        spec.Model.TopP,
			MaxTokens:   spec.Model.MaxTokens,
		},
	)
	if err != nil {
		return nil, nil, err
	}
	bindings := make(map[string]engine.Binding, len(spec.Tools))
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
		if spec.Filesystem == nil || spec.Filesystem.Provider != "AIO" {
			return nil, nil, errors.New("bound AIO filesystem is required")
		}
		b, err := aioBackend.NewWithClient(
			spec.Filesystem.URL,
			os.Getenv(spec.Filesystem.TokenEnv),
			spec.Filesystem.Workspace,
			&http.Client{
				Transport:     executionTransport{scope: spec.Scope},
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
			},
		)
		if err != nil {
			return nil, nil, err
		}
		if err = b.Ready(ctx); err != nil {
			return nil, nil, err
		}
		middleware, err := fsTools.New(ctx, &fsTools.MiddlewareConfig{Backend: b, Shell: b})
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
			bindings[info.Name] = engine.Binding{Tool: base.(tool.InvokableTool), Policy: policy}
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
