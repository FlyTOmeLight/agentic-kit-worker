package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func testSkillSpec(skills []durable.SkillSnapshot) func(*durable.Spec) {
	return func(spec *durable.Spec) {
		spec.Skills = skills
	}
}

func skillSnapshotEnvelope(
	t *testing.T,
	budget contextbuilder.Budget,
	skills []contextbuilder.SkillSummary,
) durable.ContextSnapshot {
	t.Helper()
	envelope, err := contextbuilder.EncodeSnapshot(contextbuilder.Snapshot{
		MemoryMode: contextbuilder.MemoryOff,
		Policy:     contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4},
		Budget:     budget,
		Materials: contextbuilder.FrozenMaterials{
			SystemInstruction: "You are helpful.",
			Skills:            skills,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return durable.ContextSnapshot{
		Format:        envelope.Format,
		PolicyVersion: envelope.PolicyVersion,
		Digest:        envelope.Digest,
		Payload:       envelope.Payload,
	}
}

func TestSkillOnDemandLoadReplayAndErrors(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	const versionURI = "artifact://tenant/projects/p1/artifacts/agent-skill/s1/versions/v1"
	skills := []durable.SkillSnapshot{{
		Name:        "go-review",
		Description: "Review Go code carefully",
		ArtifactURI: versionURI,
		VersionURI:  versionURI,
		EntryPath:   "SKILL.md",
		Files: []durable.SkillFileSnapshot{
			{
				Path:      "SKILL.md",
				FileID:    "f-md",
				FileURI:   versionURI + "/files/SKILL.md",
				MediaType: "text/markdown",
				SizeBytes: 24,
			},
			{
				Path:      "references/rules.md",
				FileID:    "f-ref",
				FileURI:   versionURI + "/files/references/rules.md",
				MediaType: "text/markdown",
				SizeBytes: 17,
			},
		},
	}}

	run := submit(t, store, "noop", durable.ReadOnly, testSkillSpec(skills))
	claimed := getClaim(t, store)
	claimed.ContextSnapshot = skillSnapshotEnvelope(t, contextbuilder.Budget{
		MaxInputBytes: 64 << 10, CounterVersion: contextbuilder.CJKWeightedCounterVersion,
	}, []contextbuilder.SkillSummary{
		{
			Name:        "go-review",
			Description: "Review Go code carefully",
			EntryPath:   "SKILL.md",
			Files:       []string{"SKILL.md", "references/rules.md"},
		},
	})

	var apiCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls.Add(1)
		var req struct {
			WorkerID string `json:"workerID"`
			Epoch    int64  `json:"epoch"`
			Skill    string `json:"skill"`
			Path     string `json:"path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.Path {
		case "", "SKILL.md":
			_ = json.NewEncoder(w).Encode(sdk.SkillFileContent{
				Skill: req.Skill, Path: "SKILL.md", VersionURI: versionURI, MediaType: "text/markdown",
				SizeBytes: 24, Content: "# Go Review\nCheck error.",
			})
		case "references/rules.md":
			_ = json.NewEncoder(w).Encode(sdk.SkillFileContent{
				Skill:      req.Skill,
				Path:       "references/rules.md",
				VersionURI: versionURI,
				MediaType:  "text/markdown",
				SizeBytes:  17,
				Content:    "- No naked panic.",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NotFound","message":"missing"}`))
		}
	}))
	defer apiSrv.Close()

	sdkClient, err := sdk.NewClient(apiSrv.URL, "worker-service-token-at-least-24-chars", apiSrv.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sdkClient.Close() }()

	modelCalls := 0
	var sawDirectory, sawHelper, sawNotFound, sawFileNotFound bool
	executor := &engine.Engine{
		Store:       store,
		SkillReader: engine.HTTPSkillReader{Client: sdkClient.Executions},
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					modelCalls++
					switch modelCalls {
					case 1:
						for _, m := range messages {
							if strings.Contains(m.Content, "go-review") &&
								strings.Contains(m.Content, "references/rules.md") {
								sawDirectory = true
							}
						}
						return &schema.Message{
							Role: schema.Assistant,
							ToolCalls: []schema.ToolCall{
								{
									ID:   "call-md",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"go-review"}`,
									},
								},
							},
						}, nil
					case 2:
						last := messages[len(messages)-1]
						if !strings.Contains(last.Content, "Check error.") ||
							!strings.Contains(last.Content, "references/rules.md") {
							t.Errorf("unexpected SKILL.md tool result: %s", last.Content)
						}
						return &schema.Message{
							Role: schema.Assistant,
							ToolCalls: []schema.ToolCall{
								{
									ID:   "call-ref",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"go-review","path":"references/rules.md"}`,
									},
								},
								{
									ID:   "call-bad-skill",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"unknown-skill"}`,
									},
								},
								{
									ID:   "call-bad-file",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"go-review","path":"missing.md"}`,
									},
								},
							},
						}, nil
					default:
						for _, m := range messages {
							if m.Role != schema.Tool {
								continue
							}
							if strings.Contains(m.Content, "No naked panic.") {
								sawHelper = true
							}
							if strings.Contains(m.Content, "SKILL_NOT_FOUND") {
								sawNotFound = true
							}
							if strings.Contains(m.Content, "SKILL_FILE_NOT_FOUND") {
								sawFileNotFound = true
							}
						}
						return schema.AssistantMessage("review complete", nil), nil
					}
				},
			}, map[string]engine.Binding{
				"noop": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "noop"}},
			}, nil
		},
	}

	if err := executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := store.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed || done.Output != "review complete" {
		t.Fatalf("done=%+v err=%v", done, err)
	}
	if !sawDirectory || !sawHelper || !sawNotFound || !sawFileNotFound {
		t.Fatalf(
			"flags dir=%t helper=%t notFound=%t fileNotFound=%t",
			sawDirectory,
			sawHelper,
			sawNotFound,
			sawFileNotFound,
		)
	}
	// Only 2 HTTP calls (SKILL.md + references/rules.md); unknown skill/file rejected locally without calling API.
	if apiCalls.Load() != 2 {
		t.Fatalf("apiCalls = %d, want 2", apiCalls.Load())
	}
}

func TestSkillLeaseLostPropagatesAndUnavailableDegrades(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	const versionURI = "artifact://tenant/projects/p1/artifacts/agent-skill/s1/versions/v1"
	skills := []durable.SkillSnapshot{{
		Name:        "go-review",
		Description: "Review Go code",
		ArtifactURI: versionURI,
		VersionURI:  versionURI,
		EntryPath:   "SKILL.md",
		Files: []durable.SkillFileSnapshot{
			{
				Path:      "SKILL.md",
				FileID:    "f-md",
				FileURI:   versionURI + "/files/SKILL.md",
				MediaType: "text/markdown",
				SizeBytes: 12,
			},
		},
	}}

	// Case 1: 503 unavailable -> structured SKILL_UNAVAILABLE feedback, Run completes.
	run1 := submit(t, store, "noop", durable.ReadOnly, testSkillSpec(skills))
	claimed1 := getClaim(t, store)
	srv503 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"DependencyUnavailable","message":"artifact down"}`))
	}))
	defer srv503.Close()
	c503, err := sdk.NewClient(srv503.URL, "worker-token-at-least-24-chars", srv503.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c503.Close() }()

	calls := 0
	sawUnavailable := false
	exec1 := &engine.Engine{
		Store:       store,
		SkillReader: engine.HTTPSkillReader{Client: c503.Executions},
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					calls++
					if calls == 1 {
						return &schema.Message{
							Role: schema.Assistant,
							ToolCalls: []schema.ToolCall{
								{
									ID:   "call-1",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"go-review"}`,
									},
								},
							},
						}, nil
					}
					if strings.Contains(messages[len(messages)-1].Content, "SKILL_UNAVAILABLE") {
						sawUnavailable = true
					}
					return schema.AssistantMessage("proceeded without skill", nil), nil
				},
			}, map[string]engine.Binding{
				"noop": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "noop"}},
			}, nil
		},
	}
	if err := exec1.Execute(ctx, claimed1); err != nil {
		t.Fatal(err)
	}
	done1, err := store.Get(ctx, "owner", run1.ID)
	if err != nil || done1.Status != durable.Completed || !sawUnavailable {
		t.Fatalf("done1=%+v sawUnavailable=%t err=%v", done1, sawUnavailable, err)
	}

	// Case 2: 409 LeaseLost -> propagates durable.ErrLeaseLost immediately.
	_ = submit(t, store, "noop", durable.ReadOnly, testSkillSpec(skills))
	claimed2 := getClaim(t, store)
	srvLease := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"LeaseLost","message":"execution lease lost"}`))
	}))
	defer srvLease.Close()
	cLease, err := sdk.NewClient(srvLease.URL, "worker-token-at-least-24-chars", srvLease.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cLease.Close() }()

	exec2 := &engine.Engine{
		Store:       store,
		SkillReader: engine.HTTPSkillReader{Client: cLease.Executions},
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				return &schema.Message{
					Role: schema.Assistant,
					ToolCalls: []schema.ToolCall{
						{
							ID:   "call-lease",
							Type: "function",
							Function: schema.FunctionCall{
								Name:      durable.LoadSkillToolName,
								Arguments: `{"name":"go-review"}`,
							},
						},
					},
				}, nil
			}}, map[string]engine.Binding{
				"noop": {Policy: durable.ReadOnly, Tool: &fakeTool{name: "noop"}},
			}, nil
		},
	}
	if err := exec2.Execute(ctx, claimed2); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expected durable.ErrLeaseLost, got %v", err)
	}
}

type stubSkillReader struct {
	content string
	calls   atomic.Int32
}

func (r *stubSkillReader) ReadSkillFile(
	_ context.Context,
	_ durable.Lease,
	skill, path string,
) (*durable.SkillFileContent, error) {
	r.calls.Add(1)
	if path == "" {
		path = "SKILL.md"
	}
	return &durable.SkillFileContent{
		Skill:      skill,
		Path:       path,
		VersionURI: "artifact://tenant/projects/p1/artifacts/agent-skill/s1/versions/v1",
		MediaType:  "text/markdown",
		SizeBytes:  int64(len(r.content)),
		Content:    r.content,
	}, nil
}

func TestSkillCompactionCarriesPinAndToolCap128(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	const versionURI = "artifact://tenant/projects/p1/artifacts/agent-skill/s1/versions/v1"
	skills := []durable.SkillSnapshot{{
		Name:        "go-review",
		Description: "Review Go code",
		ArtifactURI: versionURI,
		VersionURI:  versionURI,
		EntryPath:   "SKILL.md",
		Files: []durable.SkillFileSnapshot{
			{
				Path:      "SKILL.md",
				FileID:    "f-md",
				FileURI:   versionURI + "/files/SKILL.md",
				MediaType: "text/markdown",
				SizeBytes: 40,
			},
		},
	}}

	run := submit(t, store, "work", durable.ReadOnly, testSkillSpec(skills))
	claimed := getClaim(t, store)
	claimed.ContextSnapshot = skillSnapshotEnvelope(t, contextbuilder.Budget{
		MaxInputBytes: 4096, CounterVersion: contextbuilder.CJKWeightedCounterVersion,
	}, []contextbuilder.SkillSummary{{Name: "go-review", Description: "Review Go code"}})

	reader := &stubSkillReader{content: "# Go Review\nCRITICAL_SKILL_RULE_42"}
	modelCalls := 0
	summaries := 0
	sawPinnedSkillAfterCompact := false
	executor := &engine.Engine{
		Store:              store,
		SkillReader:        reader,
		CompactionMaxBytes: 2400,
		Summarizer: summaryFunc(func(summaryCtx context.Context, input engine.SummaryInput) (string, error) {
			summaries++
			return "compacted summary", input.RecordUsage(summaryCtx, &schema.TokenUsage{
				PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
			}, 15)
		}),
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					modelCalls++
					switch modelCalls {
					case 1:
						return &schema.Message{
							Role: schema.Assistant,
							ToolCalls: []schema.ToolCall{
								{
									ID:   "skill-1",
									Type: "function",
									Function: schema.FunctionCall{
										Name:      durable.LoadSkillToolName,
										Arguments: `{"name":"go-review"}`,
									},
								},
							},
						}, nil
					case 2, 3:
						return &schema.Message{
							Role: schema.Assistant,
							ToolCalls: []schema.ToolCall{
								{
									ID: fmt.Sprintf(
										"work-%d",
										modelCalls,
									),
									Type: "function",
									Function: schema.FunctionCall{
										Name:      "work",
										Arguments: `{}`,
									},
								},
							},
						}, nil
					default:
						if summaries > 0 {
							for _, m := range messages {
								if strings.Contains(
									m.Content,
									"CRITICAL_SKILL_RULE_42",
								) {
									sawPinnedSkillAfterCompact = true
								}
							}
						}
						return schema.AssistantMessage("all done", nil), nil
					}
				},
			}, map[string]engine.Binding{
				"work": {Policy: durable.ReadOnly, Tool: &fakeTool{
					name: "work",
					invoke: func(context.Context, string) (string, error) {
						return strings.Repeat("verbose log output ", 70), nil
					},
				}},
			}, nil
		},
	}

	if err := executor.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := store.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed {
		t.Fatalf("done=%+v err=%v", done, err)
	}
	if summaries == 0 || !sawPinnedSkillAfterCompact {
		t.Fatalf(
			"expected compaction to preserve skill pin: summaries=%d sawPin=%t",
			summaries,
			sawPinnedSkillAfterCompact,
		)
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("skill reader calls = %d, want 1", reader.calls.Load())
	}

	// Verify 128 tool cap counts load_skill + read_step_result.
	runCap := submit(t, store, "t0", durable.ReadOnly, func(spec *durable.Spec) {
		spec.Skills = skills
		spec.Tools = make([]durable.ToolSpec, 127)
		for i := range 127 {
			spec.Tools[i] = durable.ToolSpec{
				Name:       fmt.Sprintf("t%d", i),
				URL:        "https://tool.test/call",
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			}
		}
	})
	claimedCap := getClaim(t, store)
	execCap := &engine.Engine{
		Store: store,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			b := make(map[string]engine.Binding, 127)
			for i := range 127 {
				name := fmt.Sprintf("t%d", i)
				b[name] = engine.Binding{Policy: durable.ReadOnly, Tool: &fakeTool{name: name}}
			}
			return &fakeModel{}, b, nil
		},
	}
	if err := execCap.Execute(ctx, claimedCap); err != nil {
		t.Fatal(err)
	}
	failed, err := store.Get(ctx, "owner", runCap.ID)
	if err != nil || failed.Status != durable.Failed || !strings.Contains(failed.Error, "exceeds limit of 128") {
		t.Fatalf("expected tool cap failure, got run=%+v err=%v", failed, err)
	}
}

func TestSkillStepReplayDoesNotRefetchAfterResume(t *testing.T) {
	store, _ := testDB(t)
	ctx := context.Background()
	const versionURI = "artifact://tenant/projects/p1/artifacts/agent-skill/s1/versions/v1"
	skills := []durable.SkillSnapshot{{
		Name:        "go-review",
		Description: "Review Go code",
		ArtifactURI: versionURI,
		VersionURI:  versionURI,
		EntryPath:   "SKILL.md",
		Files: []durable.SkillFileSnapshot{
			{
				Path:      "SKILL.md",
				FileID:    "f-md",
				FileURI:   versionURI + "/files/SKILL.md",
				MediaType: "text/markdown",
				SizeBytes: 24,
			},
		},
	}}

	run := submit(t, store, "ask_for_clarification", durable.ReadOnly, testSkillSpec(skills))
	claimed := getClaim(t, store)

	reader := &stubSkillReader{content: "# Go Review\nReplayable instructions."}
	firstModelCalls := 0
	exec1 := &engine.Engine{
		Store:       store,
		SkillReader: reader,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				firstModelCalls++
				// Batch load_skill + ask_for_clarification in the same turn so load_skill completes,
				// then ask_for_clarification pauses and checkpoints the run.
				return &schema.Message{
					Role: schema.Assistant,
					ToolCalls: []schema.ToolCall{
						{
							ID:   "call-skill",
							Type: "function",
							Function: schema.FunctionCall{
								Name:      durable.LoadSkillToolName,
								Arguments: `{"name":"go-review"}`,
							},
						},
						{
							ID:   "call-ask",
							Type: "function",
							Function: schema.FunctionCall{
								Name:      "ask_for_clarification",
								Arguments: `{"question":"continue?"}`,
							},
						},
					},
				}, nil
			}}, map[string]engine.Binding{
				"ask_for_clarification": {
					Policy: durable.ReadOnly,
					Tool:   &fakeTool{name: "ask_for_clarification"},
				},
			}, nil
		},
	}
	if err := exec1.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	paused, err := store.Get(ctx, "owner", run.ID)
	if err != nil || paused.Status != durable.WaitingClarification || reader.calls.Load() != 1 {
		t.Fatalf("paused=%+v calls=%d err=%v", paused, reader.calls.Load(), err)
	}

	// Answer clarification and resume with a replacement worker whose SkillReader is nil:
	// the completed load_skill step MUST replay from durable_step without calling SkillReader again.
	if err := store.SupplyResult(
		ctx,
		"owner",
		run.ID,
		paused.WaitingStep,
		"yes proceed",
		durable.InteractionClarification,
	); err != nil {
		t.Fatal(err)
	}
	resumed := getClaim(t, store)
	sawReplayedSkill := false
	exec2 := &engine.Engine{
		Store:       store,
		SkillReader: nil,
		Build: func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{
				generate: func(_ context.Context, messages []*schema.Message) (*schema.Message, error) {
					for _, m := range messages {
						if m.Role == schema.Tool &&
							strings.Contains(m.Content, "Replayable instructions.") {
							sawReplayedSkill = true
						}
					}
					return schema.AssistantMessage("resumed cleanly", nil), nil
				},
			}, map[string]engine.Binding{
				"ask_for_clarification": {
					Policy: durable.ReadOnly,
					Tool:   &fakeTool{name: "ask_for_clarification"},
				},
			}, nil
		},
	}
	if err := exec2.Execute(ctx, resumed); err != nil {
		t.Fatal(err)
	}
	done, err := store.Get(ctx, "owner", run.ID)
	if err != nil || done.Status != durable.Completed || !sawReplayedSkill || reader.calls.Load() != 1 {
		t.Fatalf(
			"done=%+v sawReplayedSkill=%t calls=%d err=%v",
			done,
			sawReplayedSkill,
			reader.calls.Load(),
			err,
		)
	}
}
