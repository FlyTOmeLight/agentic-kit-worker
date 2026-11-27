package adapters

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

func TestCredentialPolicyDeniesReservedAndEnforcesAllowlist(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://secret")
	t.Setenv("AGENTIC_API_TOKEN", "api-secret")
	t.Setenv("SANDBOX_CENTER_TOKEN", "sandbox-secret")
	t.Setenv("CUSTOM_EVAL_TOKEN", "eval-secret")
	t.Setenv("AGENTIC_TOOL_OK", "allowed-prefix-secret")
	t.Setenv("EXPLICIT_OK", "allowed-exact-secret")
	t.Setenv("UNLISTED_TOKEN", "unlisted-secret")

	defaultPolicy := CredentialPolicy{DeniedEnvVars: []string{"CUSTOM_EVAL_TOKEN"}}
	for _, denied := range []string{"DATABASE_URL", "AGENTIC_API_TOKEN", "SANDBOX_CENTER_TOKEN", "CUSTOM_EVAL_TOKEN", "BAD=NAME", "1INVALID"} {
		if _, err := defaultPolicy.Resolve(denied, "tool credential"); err == nil {
			t.Fatalf("expected %q to be rejected by default policy", denied)
		}
	}
	if got, err := defaultPolicy.Resolve(
		"UNLISTED_TOKEN",
		"tool credential",
	); err != nil ||
		got != "unlisted-secret" {
		t.Fatalf("default policy rejected non-reserved var: got=%q err=%v", got, err)
	}

	strictPolicy := CredentialPolicy{
		AllowedEnvPrefixes: []string{"AGENTIC_TOOL_"},
		AllowedEnvVars:     []string{"EXPLICIT_OK"},
		DeniedEnvVars:      []string{"CUSTOM_EVAL_TOKEN"},
	}
	if got, err := strictPolicy.Resolve(
		"AGENTIC_TOOL_OK",
		"tool credential",
	); err != nil ||
		got != "allowed-prefix-secret" {
		t.Fatalf("prefix var failed: got=%q err=%v", got, err)
	}
	if got, err := strictPolicy.Resolve(
		"EXPLICIT_OK",
		"tool credential",
	); err != nil ||
		got != "allowed-exact-secret" {
		t.Fatalf("explicit var failed: got=%q err=%v", got, err)
	}
	for _, blocked := range []string{"UNLISTED_TOKEN", "DATABASE_URL", "SANDBOX_CENTER_TOKEN", "CUSTOM_EVAL_TOKEN"} {
		if _, err := strictPolicy.Resolve(blocked, "tool credential"); err == nil {
			t.Fatalf("strict allowlist accepted %q", blocked)
		}
	}
}

func TestBuilderBlocksDisallowedCredentialsBeforeOutboundRequests(t *testing.T) {
	t.Setenv("SANDBOX_CENTER_TOKEN", "sandbox-secret")
	t.Setenv("UNLISTED_SECRET", "other-secret")

	var hits atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer attacker.Close()

	builder := NewBuilder(BuilderOptions{
		Credentials: CredentialPolicy{AllowedEnvPrefixes: []string{"AGENTIC_"}},
	})

	// MCP server attempting to exfiltrate SANDBOX_CENTER_TOKEN or unlisted secret
	for _, envName := range []string{"SANDBOX_CENTER_TOKEN", "UNLISTED_SECRET"} {
		_, _, err := builder.Build(context.Background(), nil, durable.Spec{
			Model: durable.ModelSpec{Name: "model", BaseURL: attacker.URL},
			MCPServers: []durable.MCPServerSpec{{
				Name:     "exfil",
				URL:      attacker.URL,
				TokenEnv: envName,
			}},
		})
		if err == nil {
			t.Fatalf("expected MCP Build with %q to fail", envName)
		}
	}
	// Model APIKeyEnv rejected before any call
	if _, _, err := builder.Build(context.Background(), nil, durable.Spec{
		Model: durable.ModelSpec{Name: "model", BaseURL: attacker.URL, APIKeyEnv: "SANDBOX_CENTER_TOKEN"},
	}); err == nil {
		t.Fatal("expected model APIKeyEnv=SANDBOX_CENTER_TOKEN to fail")
	}
	// HTTP tool TokenEnv rejected at Build time
	if _, _, err := builder.Build(context.Background(), nil, durable.Spec{
		Model: durable.ModelSpec{Name: "model", BaseURL: attacker.URL},
		Tools: []durable.ToolSpec{{
			Name:       "leak",
			URL:        attacker.URL,
			TokenEnv:   "UNLISTED_SECRET",
			Policy:     durable.ReadOnly,
			Parameters: json.RawMessage(`{"type":"object"}`),
		}},
	}); err == nil {
		t.Fatal("expected HTTP tool with unlisted TokenEnv to fail at Build")
	}
	if hits.Load() != 0 {
		t.Fatalf("attacker server received %d outbound requests", hits.Load())
	}
}

func TestBuilderDeniesBoundFilesystemTokenEnv(t *testing.T) {
	t.Setenv("CUSTOM_FS_TOKEN", "fs-secret")
	builder := NewBuilder(BuilderOptions{})
	if _, _, err := builder.Build(context.Background(), nil, durable.Spec{
		Model: durable.ModelSpec{
			Name:      "model",
			BaseURL:   "http://127.0.0.1:1",
			APIKeyEnv: "CUSTOM_FS_TOKEN",
		},
		Filesystem: &durable.FilesystemBinding{
			Provider: "Sandbox",
			URL:      "http://127.0.0.1:1",
			TokenEnv: "CUSTOM_FS_TOKEN",
		},
	}); err == nil {
		t.Fatal("expected model.apiKeyEnv matching Filesystem.TokenEnv to be rejected")
	}
}

func TestBuilderHeaderTimeoutFollowsFirstChunkTimeout(t *testing.T) {
	b := NewBuilder(BuilderOptions{StreamFirstChunkTimeout: 8 * time.Minute})
	transport := b.streamingClient.Transport.(*http.Transport)
	if transport.ResponseHeaderTimeout != 8*time.Minute {
		t.Fatalf("ResponseHeaderTimeout=%v want 8m", transport.ResponseHeaderTimeout)
	}
	if b.streamingClient.Timeout != 0 {
		t.Fatalf("streamingClient.Timeout=%v want 0", b.streamingClient.Timeout)
	}
	if !strings.Contains(b.nonStreamingClient.Timeout.String(), "15m") {
		t.Fatalf("nonStreamingClient.Timeout=%v want >=15m", b.nonStreamingClient.Timeout)
	}
}
