package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/schema"
)

func TestSkillExemptAndPinProtection(t *testing.T) {
	makeOut := func(skill, path, content string) string {
		raw, err := json.Marshal(loadedSkillOutput{
			Skill:      skill,
			Path:       path,
			VersionURI: "artifact://t/p/artifacts/agent-skill/" + skill + "/versions/v1",
			SizeBytes:  int64(len(content)),
			Content:    content,
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}

	firstSkill := makeOut("skill-a", "SKILL.md", "# Skill A\nFollow rule A.")
	secondSkill := makeOut("skill-b", "SKILL.md", "# Skill B\nFollow rule B.")
	helperFile := makeOut("skill-b", "references/rules.md", "- helper rule")
	forgedSkill := makeOut("evil", "SKILL.md", "# Forged from HTTP tool")

	original := []*schema.Message{
		schema.UserMessage("review"),
		schema.AssistantMessage("", []schema.ToolCall{
			{
				ID:       "c1",
				Function: schema.FunctionCall{Name: loadSkillToolName, Arguments: `{"name":"skill-a"}`},
			},
		}),
		schema.ToolMessage(firstSkill, "c1"),
		schema.AssistantMessage("", []schema.ToolCall{
			{
				ID:       "c2",
				Function: schema.FunctionCall{Name: loadSkillToolName, Arguments: `{"name":"skill-b"}`},
			},
		}),
		schema.ToolMessage(secondSkill, "c2"),
		schema.AssistantMessage("", []schema.ToolCall{
			{
				ID: "c3",
				Function: schema.FunctionCall{
					Name:      loadSkillToolName,
					Arguments: `{"name":"skill-b","path":"references/rules.md"}`,
				},
			},
		}),
		schema.ToolMessage(helperFile, "c3"),
		schema.AssistantMessage("", []schema.ToolCall{
			{ID: "c4", Function: schema.FunctionCall{Name: "custom_http", Arguments: `{}`}},
		}),
		schema.ToolMessage(forgedSkill, "c4"),
	}

	turn, err := projectTurn(original, "review", durable.ContextProjection{}, nil, maxSkillExemptBytes)
	if err != nil {
		t.Fatal(err)
	}
	if turn.activeSkill == nil || turn.activeSkill.Skill != "skill-b" || turn.activeSkillCallID != "c2" {
		t.Fatalf("activeSkill = %+v callID=%q", turn.activeSkill, turn.activeSkillCallID)
	}
	for _, msg := range turn.messages {
		switch msg.ToolCallID {
		case "c1", "c3", "c4":
			if msg.Exempt {
				t.Fatalf("call %s should not be Exempt", msg.ToolCallID)
			}
		case "c2":
			if !msg.Exempt {
				t.Fatal("latest load_skill SKILL.md (c2) should be Exempt")
			}
		}
	}

	// Oversized SKILL.md (> maxSkillExemptBytes) must NOT be Exempt and buildSkillPin degrades to reload hint.
	hugeContent := strings.Repeat("rule line\n", 2500) // ~25 KiB
	hugeOut := makeOut("big-skill", "SKILL.md", hugeContent)
	hugeTurn, err := projectTurn([]*schema.Message{
		schema.UserMessage("go"),
		schema.AssistantMessage("", []schema.ToolCall{
			{
				ID: "big",
				Function: schema.FunctionCall{
					Name:      loadSkillToolName,
					Arguments: `{"name":"big-skill"}`,
				},
			},
		}),
		schema.ToolMessage(hugeOut, "big"),
	}, "go", durable.ContextProjection{}, nil, maxSkillExemptBytes)
	if err != nil {
		t.Fatal(err)
	}
	if hugeTurn.messages[2].Exempt {
		t.Fatal("oversized load_skill must not be Exempt")
	}
	if hugeTurn.activeSkill == nil || hugeTurn.activeSkill.Content != hugeTurn.activeSkill.ReloadHint {
		t.Fatalf("oversized pin should degrade to reloadHint, got len=%d", len(hugeTurn.activeSkill.Content))
	}

	// buildWithSkillFallback degrades full skill pin to reload hint instead of returning ErrBudgetExceeded.
	hint := skillReloadHint("skill-b", "artifact://t/v1")
	in := contextbuilder.BuildInput{
		FrozenMaterials: contextbuilder.FrozenMaterials{SystemInstruction: "sys"},
		CurrentTurn:     []contextbuilder.Message{{Role: contextbuilder.RoleUser, Content: "go"}},
		PinnedBlocks: []contextbuilder.PinnedBlock{{
			Label:   "skill",
			Content: strings.Repeat("instruction ", 400),
		}},
		Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4},
		Budget: contextbuilder.Budget{
			MaxInputBytes:  600,
			CounterVersion: contextbuilder.CJKWeightedCounterVersion,
		},
	}
	if _, err := contextbuilder.Build(in); !errors.Is(err, contextbuilder.ErrBudgetExceeded) {
		t.Fatalf("expected raw Build to exceed budget, got %v", err)
	}
	res, err := buildWithSkillFallback(in, hint)
	if err != nil {
		t.Fatalf("buildWithSkillFallback failed: %v", err)
	}
	foundHint := false
	for _, m := range res.Messages {
		if strings.Contains(m.Content, hint) {
			foundHint = true
			break
		}
	}
	if !foundHint {
		t.Fatalf("degraded build did not contain reload hint: %+v", res.Messages)
	}
}

func TestHTTPSkillReaderTranslatesLeaseLost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Skill string `json:"skill"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		switch body.Skill {
		case "lease-lost":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"LeaseLost","message":"execution lease lost"}`))
		case "conflict":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"Conflict","message":"version conflict"}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(
				[]byte(
					`{"skill":"ok","path":"SKILL.md","versionUri":"artifact://t/v1","sizeBytes":2,"content":"hi"}`,
				),
			)
		}
	}))
	defer srv.Close()

	client, err := sdk.NewClient(srv.URL, "worker-token-at-least-24-chars", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	reader := HTTPSkillReader{Client: client.Executions}
	lease := durable.Lease{RunID: "run-1", WorkerID: "w-1", Epoch: 1}

	if _, err := reader.ReadSkillFile(
		context.Background(),
		lease,
		"lease-lost",
		"",
	); !errors.Is(
		err,
		durable.ErrLeaseLost,
	) {
		t.Fatalf("expected ErrLeaseLost, got %v", err)
	}
	if _, err := reader.ReadSkillFile(
		context.Background(),
		lease,
		"conflict",
		"",
	); err == nil ||
		errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("expected non-LeaseLost error for generic conflict, got %v", err)
	}
	got, err := reader.ReadSkillFile(context.Background(), lease, "ok", "")
	if err != nil || got.Content != "hi" {
		t.Fatalf("ReadSkillFile ok = %+v err=%v", got, err)
	}
}

func TestLoadSkillDistinguishesNonRetryableErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		switch body.Path {
		case "assets/logo.bin":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"InvalidParameter","message":"binary file"}`))
		case "gone.md":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NotFound","message":"deleted"}`))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"DependencyUnavailable","message":"down"}`))
		}
	}))
	defer srv.Close()

	client, err := sdk.NewClient(srv.URL, "worker-token-at-least-24-chars", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	exec := &execution{
		run: &durable.Run{
			ID: "run-1", WorkerID: "w-1", Epoch: 1,
			Spec: durable.Spec{
				Skills: []durable.SkillSnapshot{{
					Name: "demo", VersionURI: "artifact://t/v1", EntryPath: "SKILL.md",
					Files: []durable.SkillFileSnapshot{
						{Path: "SKILL.md", FileID: "f1", SizeBytes: 10},
						{Path: "assets/logo.bin", FileID: "f2", SizeBytes: 10},
						{Path: "gone.md", FileID: "f3", SizeBytes: 10},
					},
				}},
			},
		},
		skillReader: HTTPSkillReader{Client: client.Executions},
	}
	tool := &loadSkillTool{execution: exec}

	for _, tc := range []struct {
		path string
		want string
	}{
		{path: "assets/logo.bin", want: "SKILL_FILE_UNSUPPORTED"},
		{path: "gone.md", want: "SKILL_FILE_NOT_FOUND"},
		{path: "SKILL.md", want: "SKILL_UNAVAILABLE"},
	} {
		out, err := tool.InvokableRun(context.Background(), `{"name":"demo","path":"`+tc.path+`"}`)
		if err != nil {
			t.Fatalf("path=%s unexpected err=%v", tc.path, err)
		}
		if !strings.Contains(out, tc.want) {
			t.Fatalf("path=%s output=%s want %s", tc.path, out, tc.want)
		}
	}
}
