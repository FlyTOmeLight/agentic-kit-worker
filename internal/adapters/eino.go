package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

// Build uses the same Eino model/tool interfaces as agentic-kit. OpenAI-compatible private model endpoints are supported.
func Build(ctx context.Context, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
	key := os.Getenv(spec.Model.APIKeyEnv)
	if spec.Model.APIKeyEnv != "" && key == "" {
		return nil, nil, fmt.Errorf("model credential environment variable is not configured")
	}
	llm, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{BaseURL: spec.Model.BaseURL, APIKey: key, Model: spec.Model.Name, Timeout: 2 * time.Minute, Temperature: spec.Model.Temperature, TopP: spec.Model.TopP, MaxTokens: spec.Model.MaxTokens})
	if err != nil {
		return nil, nil, err
	}
	bindings := make(map[string]engine.Binding, len(spec.Tools))
	for _, t := range spec.Tools {
		var params jsonschema.Schema
		if err = json.Unmarshal(t.Parameters, &params); err != nil {
			return nil, nil, err
		}
		bindings[t.Name] = engine.Binding{Tool: &httpTool{spec: t, params: &params, client: &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, Policy: t.Policy}
	}
	return llm, bindings, nil
}

type httpTool struct {
	spec   durable.ToolSpec
	params *jsonschema.Schema
	client *http.Client
}

func (t *httpTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: t.spec.Name, Desc: t.spec.Description, ParamsOneOf: schema.NewParamsOneOfByJSONSchema(t.params)}, nil
}
func (t *httpTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (string, error) {
	if t.spec.Name == "ask_for_clarification" {
		return "", fmt.Errorf("clarification must be handled by the durable execution loop")
	}
	endpoint := t.spec.URL
	body := []byte(args)
	switch t.spec.Kind {
	case "AppForgeMCP":
		endpoint = strings.TrimRight(endpoint, "/") + "/v1/RunMcpTool"
		var input map[string]any
		if err := json.Unmarshal(body, &input); err != nil {
			return "", err
		}
		var err error
		body, err = json.Marshal(map[string]any{"toolGroupID": t.spec.ResourceID, "slug": t.spec.ToolID, "args": input})
		if err != nil {
			return "", err
		}
	case "AppForgeOpenAPI":
		endpoint = strings.TrimRight(endpoint, "/") + "/v1/RunTool"
		var err error
		body, err = json.Marshal(map[string]any{"groupID": t.spec.ResourceID, "id": t.spec.ToolID, "parameters": args, "operation": 1})
		if err != nil {
			return "", err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", engine.OperationID(ctx))
	if t.spec.TokenEnv != "" {
		token := os.Getenv(t.spec.TokenEnv)
		if token == "" {
			return "", fmt.Errorf("tool credential is not configured")
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
		return "", fmt.Errorf("tool result exceeds 4 MiB; return an artifact reference")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("tool returned HTTP %d", resp.StatusCode)
	}
	switch t.spec.Kind {
	case "AppForgeMCP":
		var response struct {
			Content json.RawMessage `json:"content"`
			IsError bool            `json:"isError"`
		}
		if err = json.Unmarshal(b, &response); err != nil {
			return "", err
		}
		if response.IsError || len(response.Content) == 0 {
			return "", fmt.Errorf("AppForge MCP invocation failed")
		}
		return string(response.Content), nil
	case "AppForgeOpenAPI":
		var response struct {
			Success     bool   `json:"success"`
			RawResponse string `json:"rawResponse"`
		}
		if err = json.Unmarshal(b, &response); err != nil {
			return "", err
		}
		if !response.Success {
			return "", fmt.Errorf("AppForge OpenAPI invocation failed")
		}
		return response.RawResponse, nil
	}
	return string(b), nil
}
