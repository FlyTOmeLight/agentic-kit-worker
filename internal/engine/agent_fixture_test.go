package engine_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"gorm.io/gorm"
)

// createPublishedAgentFixture writes the local asset rows needed by run
// submission. The version body lives in artifact in production, so tests pass
// the same inline config to PrepareChatSubmission instead of reading it back.
func createPublishedAgentFixture(
	t *testing.T,
	s *durable.Store,
	owner, name string,
	config durable.AgentConfig,
) (string, int64) {
	t.Helper()
	ctx := context.Background()
	agentID := durable.NewID()
	draft, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DB().WithContext(ctx).Exec(
		`INSERT INTO agent
		 (id, tenant_id, project_id, owner_id, name, draft, revision, published_revision, published_version, created_at, updated_at)
		 VALUES (?, 'tenant', '', ?, ?, CAST(? AS jsonb), 1, 1, 1, clock_timestamp(), clock_timestamp())`,
		agentID, owner, name, string(draft),
	).Error; err != nil {
		t.Fatal(err)
	}
	if err = s.DB().WithContext(ctx).Exec(
		`INSERT INTO agent_version
		 (agent_id, number, artifact_version_id, artifact_version_uri, created_at)
		 VALUES (?, 1, ?, ?, clock_timestamp())`,
		agentID, "fixture-version-"+agentID, "artifact://fixture/"+agentID,
	).Error; err != nil {
		t.Fatal(err)
	}
	return agentID, 1
}

func submitChatFixture(
	t *testing.T,
	s *durable.Store,
	owner, name string,
	config durable.AgentConfig,
	query string,
	ttl time.Duration,
	maxAttempts int,
	configureSpec ...func(*durable.Spec),
) (*durable.Session, *durable.Run) {
	t.Helper()
	ctx := context.Background()
	agentID, version := createPublishedAgentFixture(t, s, owner, name, config)
	session, err := s.CreateSession(ctx, owner, agentID, "")
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(struct{ Session, Query string }{Session: session.ID, Query: query})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := s.Accept(ctx, execution.SubmitCommand{
		Owner:       owner,
		RequestKey:  durable.NewID(),
		RequestHash: durable.Hash(input),
		TTL:         ttl,
		MaxAttempts: maxAttempts,
		Kind:        execution.SubmitChat,
		SessionID:   session.ID,
		Query:       query,
	}, func(ctx context.Context, tx *gorm.DB, facts execution.SubmissionFacts) (execution.PreparedSubmission, error) {
		prepared, err := durable.PrepareChatSubmission(config, agentID, version, nil)(ctx, tx, facts)
		if err != nil {
			return prepared, err
		}
		prepared.Spec.PricingFrozen = true
		for _, configure := range configureSpec {
			configure(&prepared.Spec)
		}
		return prepared, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return session, run
}

func pricedSpec(input, output float64) func(*durable.Spec) {
	return func(spec *durable.Spec) {
		spec.PricingFrozen = true
		spec.PricingSnapshot = &execution.PricingSnapshot{
			Version: "test-v1", Model: spec.Model.Name, BaseURL: spec.Model.BaseURL,
			InputPerMillion: input, OutputPerMillion: output,
		}
	}
}
