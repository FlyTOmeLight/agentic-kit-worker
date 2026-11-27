package adapters

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
)

var credentialEnvPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

var reservedPlatformEnvVars = map[string]struct{}{
	"DATABASE_URL":                   {},
	"AGENTIC_API_TOKEN":              {},
	"SANDBOX_CENTER_TOKEN":           {},
	"REDIS_PASSWORD":                 {},
	"FORGE_SERVICE_API_KEY":          {},
	"TENANT_SERVICE_API_KEY":         {},
	"AGENT_ARTIFACT_SERVICE_API_KEY": {},
	"CALLBACK_SIGNING_SECRET":        {},
}

// CredentialPolicy controls which Worker environment variables tenant-authored
// Agent configs (model.apiKeyEnv, HTTP tool tokenEnv, MCP server tokenEnv) may read.
// Trusted server-side bindings (Filesystem.TokenEnv, Evaluator.TokenEnv) are outside
// this policy.
type CredentialPolicy struct {
	AllowedEnvPrefixes []string
	AllowedEnvVars     []string
	DeniedEnvVars      []string
}

func (p CredentialPolicy) WithDenied(names ...string) CredentialPolicy {
	var extra []string
	for _, name := range names {
		if name != "" && !slices.Contains(p.DeniedEnvVars, name) && !slices.Contains(extra, name) {
			extra = append(extra, name)
		}
	}
	if len(extra) == 0 {
		return p
	}
	next := p
	next.DeniedEnvVars = append(slices.Clone(p.DeniedEnvVars), extra...)
	return next
}

func (p CredentialPolicy) Validate(name string) error {
	if name == "" {
		return nil
	}
	if !credentialEnvPattern.MatchString(name) {
		return fmt.Errorf("credential environment variable %q has invalid name", name)
	}
	if _, reserved := reservedPlatformEnvVars[name]; reserved || slices.Contains(p.DeniedEnvVars, name) {
		return fmt.Errorf("credential environment variable %q is not allowed", name)
	}
	if len(p.AllowedEnvPrefixes) == 0 && len(p.AllowedEnvVars) == 0 {
		return nil
	}
	if slices.Contains(p.AllowedEnvVars, name) {
		return nil
	}
	for _, prefix := range p.AllowedEnvPrefixes {
		if prefix != "" && strings.HasPrefix(name, prefix) {
			return nil
		}
	}
	return fmt.Errorf("credential environment variable %q is not in worker allowlist", name)
}

func (p CredentialPolicy) Resolve(name, label string) (string, error) {
	if name == "" {
		return "", nil
	}
	if err := p.Validate(name); err != nil {
		return "", err
	}
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s environment variable %q is not configured", label, name)
	}
	return value, nil
}
