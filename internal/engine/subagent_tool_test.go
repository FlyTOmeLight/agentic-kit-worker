package engine

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

func TestSubAgentToolValidation(t *testing.T) {
	configured := []durable.SubAgentSnapshot{{Name: "review", Description: "Review changes"}}
	for _, test := range []struct {
		name    string
		spec    durable.Spec
		valid   []string
		invalid []string
	}{
		{
			name:  "configured",
			spec:  durable.Spec{SubAgents: configured},
			valid: []string{`{"subagent":"review","task":"x"}`},
			invalid: []string{
				`{"task":"x"}`, `{"subagent":"other","task":"x"}`,
				`{"subagent":"review","task":"x","instruction":"role"}`,
			},
		},
		{
			name:  "self",
			spec:  durable.Spec{SelfDelegate: true},
			valid: []string{`{"task":"x"}`, `{"task":"x","instruction":"role","timeout_seconds":1}`},
			invalid: []string{
				`{"task":"x","subagent":"review"}`, `{"task":"x","subagent":""}`,
				`{"task":" "}`, `{"task":null}`, `{"task":`, `[]`,
				`{"task":"x","timeout_seconds":0}`, `{"task":"x","timeout_seconds":-1}`,
				`{"task":"x","timeout_seconds":1.2}`, `{"task":"x","timeout_seconds":9223372036854775807}`,
				`{"task":"x","instruction":` + strconv.Quote(strings.Repeat("目", 1366)) + `}`,
			},
		},
		{
			name:    "dual",
			spec:    durable.Spec{SubAgents: configured, SelfDelegate: true},
			valid:   []string{`{"subagent":"review","task":"x"}`, `{"task":"x","instruction":"role"}`},
			invalid: []string{`{"subagent":"review","task":"x","instruction":"role"}`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding, err := delegateSubAgentBinding(&execution{run: &durable.Run{Spec: test.spec}})
			if err != nil || !binding.IsDelegate || binding.Policy != durable.Idempotent {
				t.Fatalf("delegate=%t policy=%s err=%v", binding.IsDelegate, binding.Policy, err)
			}
			for _, raw := range test.valid {
				if err := binding.Validate(raw); err != nil {
					t.Errorf("valid input %s: %v", raw, err)
				}
			}
			for _, raw := range test.invalid {
				if err := binding.Validate(raw); err == nil {
					t.Errorf("accepted invalid input with %d bytes: %.80s", len(raw), raw)
				}
			}
			if _, err := delegateSubAgentBinding(&execution{run: &durable.Run{
				Spec: test.spec, ParentRunID: "parent",
			}}); err == nil {
				t.Error("child was able to register delegation")
			}
		})
	}
	request, err := parseSubAgentRequest(`{"task":"keep whitespace ","timeout_seconds":90}`)
	if err != nil || request.Query != "keep whitespace " || request.TTL != 90*time.Second {
		t.Fatalf("parsed request=%+v err=%v", request, err)
	}
}
