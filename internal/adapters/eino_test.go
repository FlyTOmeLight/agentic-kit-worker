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

func TestAppForgeToolContracts(t *testing.T) {
	cases := []struct{ kind, path, response, want string }{
		{"AppForgeMCP", "/v1/RunMcpTool", `{"content":[{"type":"text","text":"ok"}],"isError":false}`, `[{"type":"text","text":"ok"}]`},
		{"AppForgeOpenAPI", "/v1/RunTool", `{"success":true,"rawResponse":"ok"}`, "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Errorf("path %s", r.URL.Path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if tc.kind == "AppForgeMCP" {
					if body["toolGroupID"] != "group" || body["slug"] != "tool" {
						t.Errorf("body %#v", body)
					}
				} else {
					if body["groupID"] != "group" || body["id"] != "tool" || body["parameters"] != `{"q":"value"}` || body["operation"] != float64(1) {
						t.Errorf("body %#v", body)
					}
				}
				fmt.Fprint(w, tc.response)
			}))
			defer srv.Close()
			_, bindings, err := Build(context.Background(), durable.Spec{Model: durable.ModelSpec{Name: "model", BaseURL: srv.URL}, Tools: []durable.ToolSpec{{Name: "selected", Kind: tc.kind, URL: srv.URL, ResourceID: "group", ToolID: "tool", Policy: durable.ReadOnly, Parameters: json.RawMessage(`{"type":"object"}`)}}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := bindings["selected"].Tool.InvokableRun(context.Background(), `{"q":"value"}`)
			if err != nil || got != tc.want {
				t.Fatalf("got %q err=%v", got, err)
			}
		})
	}
}
func TestAppForgeBusinessErrorIsNotSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"isError":true,"content":"failed"}`) }))
	defer srv.Close()
	tool := &httpTool{spec: durable.ToolSpec{Kind: "AppForgeMCP", URL: srv.URL}, client: srv.Client()}
	if _, err := tool.InvokableRun(context.Background(), `{}`); err == nil {
		t.Fatal("business error accepted")
	}
}

func TestToolScopeComesFromFrozenRunNotModelArguments(t *testing.T) {
	scope := durable.Scope{TenantID: "tenant", ProjectID: "project", ActorID: "actor", TaskID: "task", RoundID: "round"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got durable.Scope
		if err := json.Unmarshal([]byte(r.Header.Get("X-Agentic-Scope")), &got); err != nil || got != scope {
			t.Errorf("scope: %+v %v", got, err)
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	_, bindings, err := Build(context.Background(), durable.Spec{Scope: &scope, Model: durable.ModelSpec{Name: "model", BaseURL: server.URL}, Tools: []durable.ToolSpec{{Name: "read_file", URL: server.URL, Policy: durable.ReadOnly, Parameters: json.RawMessage(`{"type":"object"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bindings["read_file"].Tool.InvokableRun(context.Background(), `{"roundID":"forged","path":"source.go"}`); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPToolRetainsStructuredFailureReason(t *testing.T) {
	cases := []struct{ body, want string }{
		{`{"error":"before must match exactly once"}`, "before must match exactly once"},
		{`{"error":{"code":"EXECUTION_REVOKED","message":"execution stopped"}}`, "EXECUTION_REVOKED: execution stopped"},
		{`<html>unavailable</html>`, "HTTP 409"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(409); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			tool := &httpTool{spec: durable.ToolSpec{Name: "edit_file", Kind: "HTTP", URL: server.URL}, client: server.Client()}
			_, err := tool.InvokableRun(context.Background(), `{}`)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "edit_file") {
				t.Fatalf("failure reason lost: %v", err)
			}
		})
	}
}
