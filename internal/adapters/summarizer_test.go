package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

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
						`{"id":"summary","choices":[{"index":0,"message":%s,"finish_reason":%q}]}`,
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
			summary, err := (Summarizer{}).Summarize(context.Background(), engine.SummaryInput{
				Spec:            spec,
				PreviousSummary: "previous facts",
				Messages:        []durable.Message{{Role: "user", Content: "new fact"}},
			})
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
