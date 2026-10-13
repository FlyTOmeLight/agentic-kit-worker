package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

func TestHTTPToolContract(t *testing.T) {
	for _, kind := range []string{"", "HTTP"} {
		t.Run("kind="+kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/tools/search" ||
					r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("unexpected request: %s %s %v", r.Method, r.URL.Path, r.Header)
				}
				var body map[string]string
				if err := json.NewDecoder(r.Body).
					Decode(&body); err != nil || body["q"] != "value" ||
					len(body) != 1 {
					t.Errorf("arguments changed: %v, %v", body, err)
				}
				fmt.Fprint(w, `{"result":"ok"}`)
			}))
			defer server.Close()
			_, bindings, err := Build(
				context.Background(),
				durable.Spec{
					Model: durable.ModelSpec{Name: "model", BaseURL: server.URL},
					Tools: []durable.ToolSpec{
						{
							Name:       "search",
							Kind:       kind,
							URL:        server.URL + "/tools/search",
							Policy:     durable.ReadOnly,
							Parameters: json.RawMessage(`{"type":"object"}`),
						},
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			got, err := bindings["search"].Tool.InvokableRun(context.Background(), `{"q":"value"}`)
			if err != nil || got != `{"result":"ok"}` {
				t.Fatalf("got %q, error %v", got, err)
			}
		})
	}
}

func TestBuildRejectsUnsupportedToolKind(t *testing.T) {
	_, _, err := Build(
		context.Background(),
		durable.Spec{
			Model: durable.ModelSpec{Name: "model", BaseURL: "http://localhost"},
			Tools: []durable.ToolSpec{
				{
					Name:       "search",
					Kind:       "unsupported",
					Policy:     durable.ReadOnly,
					Parameters: json.RawMessage(`{"type":"object"}`),
				},
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unsupported tool kind") {
		t.Fatalf("expected unsupported kind error, got %v", err)
	}
}

func TestToolScopeComesFromFrozenRunNotModelArguments(t *testing.T) {
	scope := durable.Scope{
		TenantID:  "tenant",
		ProjectID: "project",
		ActorID:   "actor",
		TaskID:    "task",
		RoundID:   "round",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got durable.Scope
		if err := json.Unmarshal([]byte(r.Header.Get("X-Agentic-Scope")), &got); err != nil || got != scope {
			t.Errorf("scope: %+v %v", got, err)
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	_, bindings, err := Build(
		context.Background(),
		durable.Spec{
			Scope: &scope,
			Model: durable.ModelSpec{Name: "model", BaseURL: server.URL},
			Tools: []durable.ToolSpec{
				{
					Name:       "read_file",
					URL:        server.URL,
					Policy:     durable.ReadOnly,
					Parameters: json.RawMessage(`{"type":"object"}`),
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bindings["read_file"].Tool.InvokableRun(
		context.Background(),
		`{"roundID":"forged","path":"source.go"}`,
	); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPToolRetainsStructuredFailureReason(t *testing.T) {
	cases := []struct{ body, want string }{
		{body: `{"error":"before must match exactly once"}`, want: "before must match exactly once"},
		{
			body: `{"error":{"code":"EXECUTION_REVOKED","message":"execution stopped"}}`,
			want: "EXECUTION_REVOKED: execution stopped",
		},
		{body: `<html>unavailable</html>`, want: "HTTP 409"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(409); fmt.Fprint(w, tc.body) },
				),
			)
			defer server.Close()
			tool := &httpTool{
				spec:   durable.ToolSpec{Name: "edit_file", Kind: "HTTP", URL: server.URL},
				client: server.Client(),
			}
			_, err := tool.InvokableRun(context.Background(), `{}`)
			if err == nil || !strings.Contains(err.Error(), tc.want) ||
				!strings.Contains(err.Error(), "edit_file") {
				t.Fatalf("failure reason lost: %v", err)
			}
		})
	}
}
