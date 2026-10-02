package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/jackc/pgx/v5"
)

func testDB(t *testing.T) (*durable.Store, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + durable.NewID()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	store, err := durable.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		_ = admin.Close(ctx)
	})
	return store, u.String()
}

type fakeModel struct {
	generate func(context.Context, []*schema.Message) (*schema.Message, error)
}

func (m *fakeModel) Generate(ctx context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.generate(ctx, in)
}
func (m *fakeModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, errors.New("not used")
}
func (m *fakeModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) { return m, nil }

type fakeTool struct {
	name   string
	invoke func(context.Context, string) (string, error)
}

func (t *fakeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: t.name}, nil
}
func (t *fakeTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (string, error) {
	return t.invoke(ctx, args)
}
func toolReply(name string) *schema.Message {
	return &schema.Message{Role: schema.Assistant, ToolCalls: []schema.ToolCall{{ID: "provider_id", Type: "function", Function: schema.FunctionCall{Name: name, Arguments: `{"question":"which version?"}`}}}}
}
func submit(t *testing.T, s *durable.Store, name string, policy durable.Policy) *durable.Run {
	t.Helper()
	r, _, err := s.Create(context.Background(), "owner", durable.NewID(), durable.NewID(), durable.Spec{Query: "do work", MaxTurns: 5, Tools: []durable.ToolSpec{{Name: name, Policy: policy}}}, time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func getClaim(t *testing.T, s *durable.Store) *durable.Run {
	t.Helper()
	r, err := s.Claim(context.Background(), durable.NewID(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestClarificationSurvivesNewEngine(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "ask_for_clarification", durable.ReadOnly)
	var models atomic.Int32
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			models.Add(1)
			if msgs[len(msgs)-1].Role == schema.Tool {
				return schema.AssistantMessage("answer "+msgs[len(msgs)-1].Content, nil), nil
			}
			return toolReply("ask_for_clarification"), nil
		}}, map[string]engine.Binding{"ask_for_clarification": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "ask_for_clarification", invoke: func(context.Context, string) (string, error) {
			t.Fatal("clarification invoked as remote tool")
			return "", nil
		}}}}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	paused, err := s.Get(ctx, "owner", r.ID)
	if err != nil || paused.Status != durable.WaitingClarification {
		t.Fatal(err)
	}
	if err = s.SupplyResult(ctx, "owner", r.ID, paused.WaitingStep, "version 2", durable.WaitingClarification); err != nil {
		t.Fatal(err)
	}
	other := &engine.Engine{Store: s, Build: build}
	if err = other.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Output != "answer version 2" || models.Load() != 2 {
		t.Fatalf("%+v models=%d err=%v", done, models.Load(), err)
	}
}
func TestIdempotentToolRetriesKeepOperationID(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "submit", durable.Idempotent)
	var keys []string
	var modelCalls int
	build := func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			modelCalls++
			if msgs[len(msgs)-1].Role == schema.Tool {
				return schema.AssistantMessage("done", nil), nil
			}
			return toolReply("submit"), nil
		}}, map[string]engine.Binding{"submit": {Policy: durable.Idempotent, Tool: &fakeTool{name: "submit", invoke: func(ctx context.Context, _ string) (string, error) {
			keys = append(keys, engine.OperationID(ctx))
			if len(keys) == 1 {
				return "", errors.New("connection lost after commit")
			}
			return "receipt", nil
		}}}}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var next *durable.Run
	for time.Now().Before(deadline) {
		var err error
		next, err = s.Claim(ctx, "b", time.Second)
		if err == nil {
			break
		}
		if !errors.Is(err, durable.ErrNoWork) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if next == nil {
		t.Fatal("retry not claimable")
	}
	if err := e.Execute(ctx, next); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] || modelCalls != 2 {
		t.Fatalf("keys=%v modelCalls=%d", keys, modelCalls)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed {
		t.Fatalf("%+v %v", done, err)
	}
}
func TestUnsafeErrorPausesInsteadOfRetrying(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "submit", durable.Unsafe)
	e := &engine.Engine{Store: s, Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) { return toolReply("submit"), nil }}, map[string]engine.Binding{"submit": {Policy: durable.Unsafe, Tool: &fakeTool{name: "submit", invoke: func(context.Context, string) (string, error) { return "", errors.New("unknown external outcome") }}}}, nil
	}}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "owner", r.ID)
	if err != nil || got.Status != durable.NeedsReview {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = s.Claim(ctx, "b", time.Second); !errors.Is(err, durable.ErrNoWork) {
		t.Fatal("unsafe task auto-retried", err)
	}
	var state engine.State
	if err = json.Unmarshal(got.State, &state); err != nil {
		t.Fatal(err)
	}
	if state.WaitingStep == "" {
		t.Fatal("no step available for reconciliation")
	}
}
