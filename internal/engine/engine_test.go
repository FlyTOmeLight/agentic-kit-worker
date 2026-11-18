package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func testDB(t *testing.T) (*durable.Store, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + durable.NewID()
	if err = admin.WithContext(ctx).Exec("CREATE SCHEMA " + name).Error; err != nil {
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
	if err = store.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		err := admin.WithContext(ctx).Exec("DROP SCHEMA " + name + " CASCADE").Error
		if err != nil {
			t.Error(err)
		}
		pool, _ := admin.DB()
		_ = pool.Close()
	})
	return store, u.String()
}

type fakeModel struct {
	generate func(context.Context, []*schema.Message) (*schema.Message, error)
}

func (m *fakeModel) Generate(ctx context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	message, err := m.generate(ctx, in)
	if message != nil {
		if message.ResponseMeta == nil {
			message.ResponseMeta = &schema.ResponseMeta{}
		}
		if message.ResponseMeta.Usage == nil {
			message.ResponseMeta.Usage = &schema.TokenUsage{TotalTokens: 1}
		}
	}
	return message, err
}

func (m *fakeModel) Stream(
	context.Context,
	[]*schema.Message,
	...model.Option,
) (*schema.StreamReader[*schema.Message], error) {
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

var toolCallSeq atomic.Int64

func toolReply(name string) *schema.Message {
	id := fmt.Sprintf("tool_reply_%d", toolCallSeq.Add(1))
	return &schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{
			{
				ID:       id,
				Type:     "function",
				Function: schema.FunctionCall{Name: name, Arguments: `{"question":"which version?"}`},
			},
		},
	}
}

func submit(
	t *testing.T,
	s *durable.Store,
	name string,
	policy durable.Policy,
	configureSpec ...func(*durable.Spec),
) *durable.Run {
	t.Helper()
	_, r := submitChatFixture(t, s, "owner", "test", durable.AgentConfig{
		Model:    durable.ModelSpec{Name: "test", BaseURL: "http://localhost:11434/v1"},
		MaxTurns: 5,
		Tools: []durable.ToolSpec{
			{
				Name:       name,
				Policy:     policy,
				URL:        "http://localhost/tool",
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}, "do work", time.Hour, 4, configureSpec...)
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
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
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
	if err = s.SupplyResult(
		ctx,
		"owner",
		r.ID,
		paused.WaitingStep,
		"version 2",
		durable.InteractionClarification,
	); err != nil {
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
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
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
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(context.Context, []*schema.Message) (*schema.Message, error) { return toolReply("submit"), nil },
			}, map[string]engine.Binding{
				"submit": {
					Policy: durable.Unsafe,
					Tool: &fakeTool{
						name:   "submit",
						invoke: func(context.Context, string) (string, error) { return "", errors.New("unknown external outcome") },
					},
				},
			}, nil
		},
	}
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
	if got.WaitingStep == "" || got.WaitingKind != durable.InteractionReview {
		t.Fatalf("no step available for reconciliation: %+v", got)
	}
}

// closingTool implements the environmentBackend release contract so the
// engine closes it when the run ends.
type closingTool struct {
	fakeTool
	released bool
	closed   int
}

func (t *closingTool) Released() bool { return t.released }

func (t *closingTool) Detach() {}

func (t *closingTool) Close(context.Context) error {
	t.closed++
	t.released = true
	return nil
}

func TestEngineReleasesEnvironmentBackendOnFinish(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	submit(t, s, "submit", durable.Idempotent)
	env := &closingTool{fakeTool: fakeTool{name: "submit", invoke: func(context.Context, string) (string, error) {
		return "receipt", nil
	}}}
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			if msgs[len(msgs)-1].Role == schema.Tool {
				return schema.AssistantMessage("done", nil), nil
			}
			return toolReply("submit"), nil
		}}, map[string]engine.Binding{"submit": {Policy: durable.Idempotent, Tool: env}}, nil
	}
	e := &engine.Engine{Store: s, Build: build}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	if env.closed != 1 {
		t.Fatalf("environment backend must be closed once on finish, got %d", env.closed)
	}
}

func TestSteeringInjectedIntoNextModelTurn(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "lookup", durable.ReadOnly)
	goal, _, err := s.CreateGoal(ctx, durable.Goal{
		OwnerID:     "owner",
		SessionID:   r.SessionID,
		RequestKey:  "steer-engine-key",
		RequestHash: "steer-engine-hash",
		Objective:   "complete lookup with steering",
		Evaluator:   durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindGoalToRun(ctx, "owner", goal.ID, r.ID); err != nil {
		t.Fatal(err)
	}

	var secondTurnLastUserMsg string
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
					for _, m := range msgs {
						if m.Role == schema.Tool {
							if last := msgs[len(msgs)-1]; last.Role == schema.User {
								secondTurnLastUserMsg = last.Content
							}
							return schema.AssistantMessage("steered answer", nil), nil
						}
					}
					return toolReply("lookup"), nil
				},
			}, map[string]engine.Binding{
				"lookup": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "lookup",
						invoke: func(ctx context.Context, _ string) (string, error) {
							// While the tool is running in turn 1, queue a steering instruction for turn 2.
							if err := s.SteerGoal(
								ctx,
								"owner",
								goal.ID,
								"prefer cached index",
							); err != nil {
								return "", err
							}
							return "lookup-ok", nil
						},
					},
				},
			}, nil
		},
	}
	if err := e.Execute(ctx, getClaim(t, s)); err != nil {
		t.Fatal(err)
	}
	if secondTurnLastUserMsg != "prefer cached index" {
		t.Fatalf("expected steering message on turn 2, got %q", secondTurnLastUserMsg)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "steered answer" {
		t.Fatalf("unexpected run state: %+v %v", done, err)
	}
}

func TestSteeringSurvivesWorkerTakeover(t *testing.T) {
	store, dsn := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	goal, _, err := store.CreateGoal(ctx, durable.Goal{
		OwnerID: "owner", SessionID: run.SessionID, RequestKey: "steer-recovery",
		RequestHash: "steer-recovery", Objective: "recover steering",
		Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(ctx, "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	claimed := getClaim(t, store)
	if err = store.SteerGoal(ctx, "owner", goal.ID, "keep the public API"); err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var calls int
	var builds int
	build := func(bctx context.Context, _ *durable.Store, _ durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		builds++
		if builds == 1 {
			// The worker is lost before any model dispatch: no inflight
			// marker is persisted, so a replacement may safely resume.
			cancel()
			return &fakeModel{
				generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
					return nil, bctx.Err()
				},
			}, map[string]engine.Binding{
				"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}},
			}, nil
		}
		return &fakeModel{
			generate: func(genCtx context.Context, messages []*schema.Message) (*schema.Message, error) {
				calls++
				var count int
				for _, message := range messages {
					if message.Role == schema.User && message.Content == "keep the public API" {
						count++
					}
				}
				if count != 1 {
					return nil, fmt.Errorf("attempt %d: steering count = %d", calls, count)
				}
				return schema.AssistantMessage("recovered", nil), nil
			},
		}, map[string]engine.Binding{
			"lookup": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "lookup"}},
		}, nil
	}
	first := &engine.Engine{Store: store, Build: build}
	if err = first.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("first attempt: %v", err)
	}
	if err = store.DB().Exec(
		"UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID,
	).Error; err != nil {
		t.Fatal(err)
	}
	reopened, err := durable.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replacement, err := reopened.Claim(ctx, "replacement-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second := &engine.Engine{Store: reopened, Build: build}
	if err = second.Execute(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	done, err := reopened.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "recovered" || calls != 1 || builds != 2 {
		t.Fatalf("recovery: run=%+v calls=%d builds=%d err=%v", done, calls, builds, err)
	}
}

func TestInflightModelBlocksReplacement(t *testing.T) {
	store, dsn := testDB(t)
	ctx := context.Background()
	run := submit(t, store, "lookup", durable.ReadOnly)
	claimed := getClaim(t, store)
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var toolCalls int
	build := func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		return &fakeModel{
			generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				// The worker is lost after the model was dispatched but before
				// its usage was persisted; consumption is unknown.
				cancel()
				return nil, context.Canceled
			},
		}, map[string]engine.Binding{
			"lookup": {
				Policy: durable.ReadOnly,
				Tool: &fakeTool{
					name: "lookup",
					invoke: func(context.Context, string) (string, error) {
						toolCalls++
						return "ok", nil
					},
				},
			},
		}, nil
	}
	first := &engine.Engine{Store: store, Build: build}
	if err := first.Execute(attemptCtx, claimed); !errors.Is(err, context.Canceled) {
		t.Fatalf("first attempt: %v", err)
	}
	if err := store.DB().Exec(
		"UPDATE durable_run SET lease_until = clock_timestamp() - interval '1 second' WHERE id = ?", run.ID,
	).Error; err != nil {
		t.Fatal(err)
	}
	reopened, err := durable.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replacement, err := reopened.Claim(ctx, "replacement-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var builds int
	second := &engine.Engine{
		Store: reopened,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			builds++
			return build(context.Background(), nil, durable.Spec{})
		},
	}
	if err = second.Execute(ctx, replacement); err != nil {
		t.Fatalf("replacement must commit the block boundary: %v", err)
	}
	done, err := reopened.Get(ctx, "owner", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != durable.Blocked {
		t.Fatalf("expected blocked run, got %+v", done)
	}
	if !strings.Contains(done.Error, "unknown consumption") ||
		!strings.Contains(done.Error, "automatic replay is disabled") {
		t.Fatalf("unexpected block detail: %q", done.Error)
	}
	if builds != 0 || toolCalls != 0 {
		t.Fatalf(
			"replacement must not build, call the model, or run tools: builds=%d toolCalls=%d",
			builds,
			toolCalls,
		)
	}
	if _, err = reopened.Claim(ctx, "third-worker", time.Minute); !errors.Is(err, durable.ErrNoWork) {
		t.Fatal("blocked run must not be claimable")
	}
}
