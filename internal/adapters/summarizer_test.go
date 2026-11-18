package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestBuildModelHTTPUsage(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		usage     string
		wantUsage *schema.TokenUsage
	}{
		{name: "missing"},
		{name: "null", usage: "null"},
		{name: "known zero", usage: `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`, wantUsage: &schema.TokenUsage{}},
		{name: "known total", usage: `{"prompt_tokens":30,"completion_tokens":12,"total_tokens":42}`, wantUsage: &schema.TokenUsage{PromptTokens: 30, CompletionTokens: 12, TotalTokens: 42}},
	} {
		for _, stream := range []bool{false, true} {
			for _, bindTools := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/tools=%t", testCase.name, stream, bindTools), func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
						var body struct {
							Stream        bool `json:"stream"`
							StreamOptions struct {
								IncludeUsage bool `json:"include_usage"`
							} `json:"stream_options"`
							Tools []json.RawMessage `json:"tools"`
						}
						if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
							t.Error(err)
							return
						}
						if body.Stream != stream || (stream && !body.StreamOptions.IncludeUsage) || (len(body.Tools) != 0) != bindTools {
							t.Errorf("unexpected request: %+v", body)
						}
						usage := ""
						if testCase.usage != "" {
							usage = `,"usage":` + testCase.usage
						}
						if stream {
							response.Header().Set("Content-Type", "text/event-stream")
							fmt.Fprint(response, "data: {\"id\":\"reply\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\",\"reasoning_content\":\"reason\"}}]}\n\n")
							fmt.Fprint(response, "data: {\"id\":\"reply\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
							fmt.Fprintf(response, "data: {\"id\":\"reply\",\"choices\":[]%s}\n\ndata: [DONE]\n\n", usage)
							return
						}
						response.Header().Set("Content-Type", "application/json")
						fmt.Fprintf(response, `{"id":"reply","choices":[{"index":0,"message":{"role":"assistant","content":"hello","reasoning_content":"reason"},"finish_reason":"stop"}]%s}`, usage)
					}))
					defer server.Close()
					ctx := context.Background()
					llm, err := buildModel(ctx, durable.ModelSpec{Name: "model", BaseURL: server.URL})
					if err != nil {
						t.Fatal(err)
					}
					if bindTools {
						llm, err = llm.WithTools([]*schema.ToolInfo{{Name: "lookup", Desc: "lookup", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{})}})
						if err != nil {
							t.Fatal(err)
						}
					}
					input := []*schema.Message{schema.UserMessage("query")}
					var result *schema.Message
					if stream {
						reader, streamErr := llm.Stream(ctx, input)
						if streamErr != nil {
							t.Fatal(streamErr)
						}
						defer reader.Close()
						var chunks []*schema.Message
						for {
							chunk, recvErr := reader.Recv()
							if errors.Is(recvErr, io.EOF) {
								break
							}
							if recvErr != nil {
								t.Fatal(recvErr)
							}
							chunks = append(chunks, chunk)
						}
						result, err = schema.ConcatMessages(chunks)
					} else {
						result, err = llm.Generate(ctx, input)
					}
					if err != nil {
						t.Fatal(err)
					}
					if result == nil || result.Content != "hello" || result.ReasoningContent != "reason" || result.ResponseMeta == nil || result.ResponseMeta.FinishReason != "stop" {
						t.Fatalf("response metadata lost: %+v", result)
					}
					usage := result.ResponseMeta.Usage
					if testCase.wantUsage == nil {
						if usage != nil {
							t.Fatalf("unknown usage synthesized: %+v", usage)
						}
					} else if usage == nil || usage.TotalTokens != testCase.wantUsage.TotalTokens || usage.PromptTokens != testCase.wantUsage.PromptTokens || usage.CompletionTokens != testCase.wantUsage.CompletionTokens {
						t.Fatalf("usage=%+v want=%+v", usage, testCase.wantUsage)
					}
				})
			}
		}
	}
}

func TestSummarizerUsesIndependentModelWithoutTools(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		message   string
		finish    string
		wantError bool
	}{
		{name: "summary", message: `{"role":"assistant","content":"Confirmed facts: task remains open."}`, finish: "stop", wantError: false},
		{name: "empty", message: `{"role":"assistant","content":" "}`, finish: "stop", wantError: true},
		{name: "truncated", message: `{"role":"assistant","content":"partial"}`, finish: "length", wantError: true},
		{name: "tool call", message: `{"role":"assistant","content":"summary","tool_calls":[{"id":"call","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`, finish: "tool_calls", wantError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(
				http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
					requests++
					if request.Method != http.MethodPost ||
						request.URL.Path != "/chat/completions" {
						t.Errorf("unexpected endpoint: %s %s", request.Method, request.URL.Path)
					}
					var body struct {
						Model      string            `json:"model"`
						Tools      []json.RawMessage `json:"tools"`
						ToolChoice json.RawMessage   `json:"tool_choice"`
						MaxTokens  int               `json:"max_tokens"`
						Messages   []struct {
							Role    string `json:"role"`
							Content string `json:"content"`
						} `json:"messages"`
					}
					if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body.Model != "frozen-model" || len(body.Tools) != 0 ||
						len(body.ToolChoice) != 0 ||
						body.MaxTokens != 2048 ||
						len(body.Messages) != 2 ||
						body.Messages[0].Role != "system" {
						t.Errorf("unexpected summary request: %+v", body)
					}
					var payload struct {
						Task            string            `json:"task"`
						PreviousSummary string            `json:"previousSummary"`
						Messages        []durable.Message `json:"messages"`
					}
					if err := json.Unmarshal(
						[]byte(body.Messages[1].Content),
						&payload,
					); err != nil ||
						payload.Task != "frozen task" || payload.PreviousSummary != "previous facts" ||
						len(payload.Messages) != 1 ||
						payload.Messages[0].Content != "new fact" {
						t.Errorf("summary lost context: %+v %v", payload, err)
					}
					response.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(
						response,
						`{"id":"summary","usage":{"prompt_tokens":30,"completion_tokens":12,"total_tokens":42},"choices":[{"index":0,"message":%s,"finish_reason":%q}]}`,
						testCase.message,
						testCase.finish,
					)
				}),
			)
			defer server.Close()
			maxTokens := 8192
			spec := durable.Spec{
				Query: "frozen task",
				Model: durable.ModelSpec{
					Name:      "frozen-model",
					BaseURL:   server.URL,
					MaxTokens: &maxTokens,
				},
			}
			recorded, usageCalls := int64(0), 0
			summary, err := (Summarizer{}).Summarize(context.Background(), engine.SummaryInput{
				Spec:            spec,
				PreviousSummary: "previous facts",
				Messages:        []durable.Message{{Role: "user", Content: "new fact"}},
				RecordUsage: func(_ context.Context, _ *schema.TokenUsage, tokens int64) error {
					recorded = tokens
					usageCalls++
					return nil
				},
			})
			if recorded != 42 || usageCalls != 1 {
				t.Fatalf("usage=%d calls=%d", recorded, usageCalls)
			}
			if (err != nil) != testCase.wantError || (!testCase.wantError && summary == "") ||
				requests != 1 || *spec.Model.MaxTokens != 8192 {
				t.Fatalf(
					"summary=%q error=%v requests=%d maxTokens=%d",
					summary,
					err,
					requests,
					*spec.Model.MaxTokens,
				)
			}
		})
	}
}

func TestSummarizerHTTPUsage(t *testing.T) {
	recordErr := errors.New("usage persistence failed")
	for _, testCase := range []struct {
		name       string
		usage      string
		status     int
		recordErr  error
		wantErr    error
		wantCalls  int
		wantTokens int64
	}{
		{name: "total", usage: `{"prompt_tokens":30,"completion_tokens":12,"total_tokens":45}`, wantCalls: 1, wantTokens: 45},
		{name: "prompt plus completion", usage: `{"prompt_tokens":30,"completion_tokens":12}`, wantCalls: 1, wantTokens: 42},
		{name: "explicit zero", usage: `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`, wantCalls: 1},
		{name: "missing usage", wantErr: engine.ErrModelUsageUnavailable},
		{name: "null usage", usage: `null`, wantErr: engine.ErrModelUsageUnavailable},
		{name: "request failure", status: http.StatusBadRequest, wantErr: engine.ErrModelUsageUnavailable},
		{name: "record failure", usage: `{"total_tokens":42}`, wantCalls: 1, wantTokens: 42, recordErr: recordErr, wantErr: recordErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				if testCase.status != 0 {
					response.WriteHeader(testCase.status)
					fmt.Fprint(response, `{"error":{"message":"summary rejected","type":"invalid_request_error"}}`)
					return
				}
				usage := ""
				if testCase.usage != "" {
					usage = `,"usage":` + testCase.usage
				}
				fmt.Fprintf(response, `{"id":"summary","choices":[{"index":0,"message":{"role":"assistant","content":"summary"},"finish_reason":"stop"}]%s}`, usage)
			}))
			defer server.Close()
			calls, tokens := 0, int64(0)
			summary, err := (Summarizer{}).Summarize(context.Background(), engine.SummaryInput{
				Spec: durable.Spec{Model: durable.ModelSpec{Name: "summary-model", BaseURL: server.URL}},
				RecordUsage: func(_ context.Context, _ *schema.TokenUsage, value int64) error {
					calls++
					tokens = value
					return testCase.recordErr
				},
			})
			if !errors.Is(err, testCase.wantErr) || calls != testCase.wantCalls || tokens != testCase.wantTokens {
				t.Fatalf("summary=%q err=%v calls=%d tokens=%d", summary, err, calls, tokens)
			}
			if testCase.wantErr != nil && summary != "" {
				t.Fatalf("failed summary returned content: %q", summary)
			}
		})
	}
}

func TestSummaryResultRecordsUsageAlongsideError(t *testing.T) {
	modelErr := errors.New("model failed after response")
	recordErr := errors.New("usage persistence failed")
	for _, testCase := range []struct {
		name      string
		response  *schema.Message
		recordErr error
		wantCalls int
		wantErr   error
	}{
		{name: "response and error", response: &schema.Message{ResponseMeta: &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 17}}}, wantCalls: 1},
		{name: "response and recording error", response: &schema.Message{ResponseMeta: &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 17}}}, wantCalls: 1, recordErr: recordErr, wantErr: recordErr},
		{name: "no response", wantErr: engine.ErrModelUsageUnavailable},
		{name: "no metadata", response: &schema.Message{}, wantErr: engine.ErrModelUsageUnavailable},
		{name: "no usage", response: &schema.Message{ResponseMeta: &schema.ResponseMeta{}}, wantErr: engine.ErrModelUsageUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			calls := 0
			_, err := summaryResult(context.Background(), testCase.response, modelErr, func(_ context.Context, _ *schema.TokenUsage, tokens int64) error {
				calls++
				if tokens != 17 {
					t.Errorf("tokens=%d, want 17", tokens)
				}
				return testCase.recordErr
			})
			if !errors.Is(err, modelErr) || (testCase.wantErr != nil && !errors.Is(err, testCase.wantErr)) || calls != testCase.wantCalls {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
		})
	}
}
