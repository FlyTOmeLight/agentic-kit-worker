package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/workspace"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

type attachmentModelRequest struct {
	Stream   bool `json:"stream"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
}

func (r attachmentModelRequest) users() []json.RawMessage {
	var users []json.RawMessage
	for _, m := range r.Messages {
		if m.Role == "user" {
			users = append(users, m.Content)
		}
	}
	return users
}

func attachmentModelServer(
	t *testing.T, reply func(int, attachmentModelRequest) subAgentModelReply,
) (string, func() []attachmentModelRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []attachmentModelRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request attachmentModelRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode OpenAI request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, request)
		index := len(requests) - 1
		mu.Unlock()

		result := reply(index, request)
		reason := "stop"
		message := map[string]any{"role": "assistant", "content": result.content}
		if len(result.calls) > 0 {
			reason = "tool_calls"
			if request.Stream {
				calls := make([]map[string]any, len(result.calls))
				for i, call := range result.calls {
					calls[i] = map[string]any{
						"index":    i,
						"id":       call.ID,
						"type":     call.Type,
						"function": call.Function,
					}
				}
				message["tool_calls"] = calls
			} else {
				message["tool_calls"] = result.calls
			}
		}
		if request.Stream {
			startModelStream(w)
			delta, err := json.Marshal(message)
			if err != nil {
				t.Error(err)
				return
			}
			writeModelChunk(w, string(delta))
			finishModelStream(w, reason, modelStreamUsage)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		response := map[string]any{
			"id": "completion", "usage": json.RawMessage(modelStreamUsage),
			"choices": []map[string]any{{"index": 0, "finish_reason": reason, "message": message}},
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, func() []attachmentModelRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]attachmentModelRequest(nil), requests...)
	}
}

func acceptAttachmentRun(
	t *testing.T, store *durable.Store, cmd execution.SubmitCommand, prepare durable.PrepareSubmission,
) *durable.Run {
	t.Helper()
	cmd.Owner, cmd.TTL, cmd.MaxAttempts = "owner", time.Hour, 4
	cmd.RequestKey = durable.NewID()
	cmd.RequestHash = cmd.RequestKey
	run, _, err := store.Accept(
		t.Context(),
		cmd,
		func(ctx context.Context, tx *gorm.DB, facts execution.SubmissionFacts) (execution.PreparedSubmission, error) {
			prepared, err := prepare(ctx, tx, facts)
			prepared.Spec.PricingFrozen = true
			return prepared, err
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func newAttachmentSession(t *testing.T, store *durable.Store, config durable.AgentConfig) *durable.Session {
	t.Helper()
	id, _ := createPublishedAgentFixture(t, store, "owner", "attachment agent", config)
	session, err := store.CreateSession(t.Context(), "owner", id, "")
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func attachmentChat(
	t *testing.T, store *durable.Store, session *durable.Session, config durable.AgentConfig,
	query string, attachments ...durable.Attachment,
) *durable.Run {
	t.Helper()
	return acceptAttachmentRun(t, store, execution.SubmitCommand{
		Kind: execution.SubmitChat, SessionID: session.ID, Query: query, Attachments: attachments,
	}, durable.PrepareChatSubmission(config, session.AgentID, 1, nil))
}

func imageAttachment(name string) durable.Attachment {
	return durable.Attachment{
		Kind:      "image",
		URI:       "https://assets.example/" + name,
		Name:      name,
		MediaType: "image/png",
	}
}

func onlyText(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
		t.Fatalf("expected JSON string content, got %s", raw)
	}
	return value
}

func pinnedFiles(t *testing.T, req attachmentModelRequest) string {
	t.Helper()
	for _, raw := range req.users() {
		if len(raw) == 0 || raw[0] != '"' {
			continue
		}
		_, pin, ok := strings.Cut(onlyText(t, raw), "[Pinned label=\"files\"]\n")
		if ok {
			content, _, _ := strings.Cut(pin, "\n[/Pinned]")
			return content
		}
	}
	t.Fatal("model request has no files pin")
	return ""
}

func assertMediaParts(
	t *testing.T, raw json.RawMessage, image, video string,
) string {
	t.Helper()
	var parts []struct {
		Type  string `json:"type"`
		Text  string `json:"text"`
		Image struct {
			URL string `json:"url"`
		} `json:"image_url"`
		Video struct {
			URL string `json:"url"`
		} `json:"video_url"`
	}
	wantLength := 2
	if video != "" {
		wantLength++
	}
	if err := json.Unmarshal(raw, &parts); err != nil || len(parts) != wantLength ||
		parts[0].Type != "text" || parts[1].Type != "image_url" || parts[1].Image.URL != image {
		t.Fatalf("expected text and image parts: %s (%v)", raw, err)
	}
	if video != "" && (parts[2].Type != "video_url" || parts[2].Video.URL != video) {
		t.Fatalf("expected direct video part: %s", raw)
	}
	return parts[0].Text
}

func assertRunCompleted(t *testing.T, worker *engine.Engine, run *durable.Run) *durable.Run {
	t.Helper()
	done := executeSubAgentRun(t, worker, claimSubAgentRun(t, worker.Store, run.ID))
	if done.Status != durable.Completed || done.Output != "done" {
		t.Fatalf("run status=%s output=%q error=%q", done.Status, done.Output, done.Error)
	}
	return done
}

func TestAttachmentOpenAIWire(t *testing.T) {
	for _, tc := range []struct {
		name            string
		stream, resume  bool
		history         bool
		inputModalities []string
	}{
		{name: "Generate", history: true},
		{name: "Stream", stream: true, inputModalities: []string{"text", "image", "video"}},
		{name: "Resume", resume: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := testDB(t)
			url, capture := attachmentModelServer(
				t,
				func(i int, _ attachmentModelRequest) subAgentModelReply {
					if tc.resume && i == 0 {
						return subAgentModelReply{calls: []schema.ToolCall{
							subAgentReplyCall(
								"ask_for_clarification",
								`{"question":"Which page?"}`,
							),
						}}
					}
					return subAgentModelReply{content: "done"}
				},
			)
			config := durable.AgentConfig{
				Model: durable.ModelSpec{
					Name:            "attachments",
					BaseURL:         url,
					Stream:          &tc.stream,
					InputModalities: tc.inputModalities,
				},
				MaxTurns: 5,
				Tools: []durable.ToolSpec{
					{
						Name:       "ask_for_clarification",
						URL:        url,
						Policy:     durable.ReadOnly,
						Parameters: json.RawMessage(`{"type":"object"}`),
					},
				},
			}
			session := newAttachmentSession(t, store, config)
			worker := &engine.Engine{Store: store, Build: adapters.Build}
			old := imageAttachment("old.png")
			if tc.history {
				prior := attachmentChat(t, store, session, config, "prior question", old)
				assertRunCompleted(t, worker, prior)
			}
			image := imageAttachment("current.png")
			video := durable.Attachment{
				Kind:      "video",
				URI:       "https://assets.example/movie.mp4",
				Name:      "movie.mp4",
				MediaType: "video/mp4",
			}
			fallbacks := []durable.Attachment{
				{
					Kind:      "document",
					URI:       "https://assets.example/report.pdf",
					Name:      "report.pdf",
					MediaType: "application/pdf",
				},
				{
					Kind:      "file",
					URI:       "https://assets.example/notes.zip",
					Name:      "notes.zip",
					MediaType: "application/zip",
				},
				{
					Kind:      "audio",
					URI:       "https://assets.example/audio.mp3",
					Name:      "audio.mp3",
					MediaType: "audio/mpeg",
				},
				{
					Kind:        "image",
					URI:         "artifact://bucket/stored.png",
					ArtifactURI: "artifact://bucket/stored.png",
					Name:        "stored.png",
					MediaType:   "image/png",
				},
			}
			run := attachmentChat(
				t,
				store,
				session,
				config,
				"inspect these files",
				append([]durable.Attachment{image, video}, fallbacks...)...)
			if tc.resume {
				waiting := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, run.ID))
				if waiting.Status != durable.WaitingClarification {
					t.Fatalf("status=%s error=%q", waiting.Status, waiting.Error)
				}
				if err := store.SupplyResult(
					t.Context(),
					"owner",
					run.ID,
					waiting.WaitingStep,
					"page 2",
					durable.InteractionClarification,
				); err != nil {
					t.Fatal(err)
				}
				worker = &engine.Engine{Store: store, Build: adapters.Build}
			}
			assertRunCompleted(t, worker, run)
			requests := capture()
			wantRequests := 1
			if tc.history || tc.resume {
				wantRequests++
			}
			if len(requests) != wantRequests {
				t.Fatalf("got %d model requests, want %d", len(requests), wantRequests)
			}
			if tc.history {
				requests = requests[1:]
			}
			for _, req := range requests {
				if req.Stream != tc.stream {
					t.Fatalf("stream=%t, want %t", req.Stream, tc.stream)
				}
				users := req.users()
				wantUsers := 2 // Files pin, followed by the original first question.
				if tc.history {
					wantUsers++
				}
				if len(users) != wantUsers {
					t.Fatalf("unexpected user messages: %s", users)
				}
				if tc.history {
					history := onlyText(t, users[0])
					if !strings.Contains(history, "prior question\n\n[Attachments]") ||
						!strings.Contains(history, old.URI) {
						t.Fatalf("missing text-only historical attachment: %s", history)
					}
				}
				pin := pinnedFiles(t, req)
				expectedVideo := ""
				metadata := append([]durable.Attachment(nil), fallbacks...)
				if len(tc.inputModalities) != 0 {
					expectedVideo = video.URI
				} else {
					metadata = append(metadata, video)
				}
				body := assertMediaParts(t, users[len(users)-1], image.URI, expectedVideo)
				if !strings.HasPrefix(body, "inspect these files\n\n[Attachments]") ||
					(expectedVideo != "" && strings.Contains(body, expectedVideo)) {
					t.Fatalf("unexpected text part: %s", body)
				}
				for _, att := range metadata {
					if !strings.Contains(body, att.URI) || strings.Count(pin, att.URI) != 1 {
						t.Fatalf("missing attachment %s: text=%s files=%s", att.URI, body, pin)
					}
				}
				if strings.Count(pin, image.URI) != 1 || strings.Count(pin, video.URI) != 1 {
					t.Fatalf("incorrect direct media index: %s", pin)
				}
			}
		})
	}
}

func TestAttachmentOpenAIWireAfterDroppingRounds(t *testing.T) {
	store, _ := testDB(t)
	const query = "keep working"
	url, capture := attachmentModelServer(t, func(i int, _ attachmentModelRequest) subAgentModelReply {
		switch i {
		case 0:
			return subAgentModelReply{calls: []schema.ToolCall{subAgentReplyCall("lookup", "{}")}}
		case 1:
			return subAgentModelReply{calls: []schema.ToolCall{
				subAgentReplyCall("ask_for_clarification", `{"question":"Which page?"}`),
			}}
		default:
			return subAgentModelReply{content: "done"}
		}
	})
	config := durable.AgentConfig{
		Model: durable.ModelSpec{Name: "attachments", BaseURL: url, Stream: new(false)}, MaxTurns: 5,
		Tools: []durable.ToolSpec{
			{
				Name:       "lookup",
				URL:        url,
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
			{
				Name:       "ask_for_clarification",
				URL:        url,
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}
	session := newAttachmentSession(t, store, config)
	image := imageAttachment("current.png")
	run := attachmentChat(t, store, session, config, query, image)
	goal, _, err := store.CreateGoal(t.Context(), durable.Goal{
		OwnerID: "owner", SessionID: session.ID, RequestKey: "steer", RequestHash: "steer",
		Objective: query, Evaluator: durable.EvaluatorSpec{Kind: durable.EvaluatorNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindGoalToRun(t.Context(), "owner", goal.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	worker := &engine.Engine{
		Store: store,
		Build: func(ctx context.Context, store *durable.Store, spec durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			llm, bindings, err := adapters.Build(ctx, store, spec)
			if err != nil {
				return nil, nil, err
			}
			binding := bindings["lookup"]
			binding.Tool = &fakeTool{
				name: "lookup",
				invoke: func(ctx context.Context, _ string) (string, error) {
					return strings.Repeat(
							"old output ",
							600,
						), store.SteerGoal(
							ctx,
							"owner",
							goal.ID,
							query,
						)
				},
			}
			bindings["lookup"] = binding
			return llm, bindings, nil
		},
	}
	waiting := executeSubAgentRun(t, worker, claimSubAgentRun(t, store, run.ID))
	if waiting.Status != durable.WaitingClarification {
		t.Fatalf("status=%s error=%q", waiting.Status, waiting.Error)
	}
	if err = store.SupplyResult(
		t.Context(),
		"owner",
		run.ID,
		waiting.WaitingStep,
		"page 2",
		durable.InteractionClarification,
	); err != nil {
		t.Fatal(err)
	}
	replacement := &engine.Engine{Store: store, Build: adapters.Build, CompactionMaxBytes: 3000}
	assertRunCompleted(t, replacement, run)
	requests := capture()
	if len(requests) != 3 {
		t.Fatalf("got %d model requests, want 3", len(requests))
	}
	before := requests[1].users()
	if len(before) != 3 || assertMediaParts(t, before[1], image.URI, "") != query ||
		onlyText(t, before[2]) != query {
		t.Fatalf("expected original question with image followed by text-only steer: %s", before)
	}
	after := requests[2].users()
	if len(after) != 2 || !strings.Contains(onlyText(t, after[0]), "[Pinned label=\"query\"]\n"+query) ||
		onlyText(t, after[1]) != query || !strings.Contains(pinnedFiles(t, requests[2]), image.URI) {
		t.Fatalf("dropped question must be a text pin with its file index and same-text steer: %s", after)
	}
	done, _, events := compressionResult(t, store, run)
	if done.FirstKeptSeq != 0 || len(events.contexts) != 3 || events.contexts[2].DroppedRounds < 1 {
		t.Fatalf(
			"expected round drop without summary, firstKeptSeq=%d contexts=%+v",
			done.FirstKeptSeq,
			events.contexts,
		)
	}
}

func TestAttachmentOpenAIWireThroughSummary(t *testing.T) {
	store, _ := testDB(t)
	const summaryText = "## Objective & Constraints\nInspect the resources.\n## Confirmed Facts\nThe user supplied resources.\n## Next Actions\nReview the files."
	url, capture := attachmentModelServer(t, func(i int, _ attachmentModelRequest) subAgentModelReply {
		if i == 1 {
			return subAgentModelReply{content: summaryText}
		}
		return subAgentModelReply{content: "done"}
	})
	config := durable.AgentConfig{
		Model: durable.ModelSpec{
			Name: "attachments", BaseURL: url, Stream: new(false), ContextWindow: 3700, MaxTokens: new(128),
			InputModalities: []string{"text", "image", "video"},
		},
		MaxTurns: 5,
	}
	// Use an actual frozen history and a current User row created by the API store.
	session := newAttachmentSession(t, store, config)
	worker := &engine.Engine{Store: store, Build: adapters.Build, Summarizer: adapters.Summarizer{}}
	old := imageAttachment("history.png")
	prior := attachmentChat(t, store, session, config, "prior question", old)
	assertRunCompleted(t, worker, prior)
	current := imageAttachment("current.png")
	video := durable.Attachment{
		Kind:      "video",
		URI:       "https://assets.example/current.mp4",
		Name:      "current.mp4",
		MediaType: "video/mp4",
	}
	run := attachmentChat(t, store, session, config, "current question", current, video)
	done := assertRunCompleted(t, worker, run)
	requests := capture()
	if len(requests) != 3 || done.FirstKeptSeq == 0 || done.CompactedSummary != summaryText {
		t.Fatalf("expected a summary before main dispatch: requests=%d firstKeptSeq=%d summary=%q",
			len(requests), done.FirstKeptSeq, done.CompactedSummary)
	}
	summaryUsers := requests[1].users()
	if len(summaryUsers) != 1 {
		t.Fatalf("unexpected summary input: %s", summaryUsers)
	}
	var summary struct {
		Task     string            `json:"task"`
		Messages []durable.Message `json:"messages"`
	}
	if err := json.Unmarshal([]byte(onlyText(t, summaryUsers[0])), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Task != "current question" || len(summary.Messages) != 3 ||
		!strings.Contains(summary.Messages[0].Content, "prior question\n\n[Attachments]") ||
		!strings.Contains(summary.Messages[0].Content, old.URI) ||
		!strings.HasPrefix(summary.Messages[2].Content, "[Attachments]") ||
		!strings.Contains(summary.Messages[2].Content, current.URI) ||
		!strings.Contains(summary.Messages[2].Content, video.URI) {
		t.Fatalf("summary lost attachment context: %+v", summary)
	}
	after := requests[2].users()
	if len(after) != 2 || onlyText(t, after[1]) != "current question" {
		t.Fatalf("expected a text-only synthesized question after compaction: %s", after)
	}
	pin := pinnedFiles(t, requests[2])
	for _, att := range []durable.Attachment{old, current, video} {
		if strings.Count(pin, att.URI) != 1 {
			t.Fatalf("compacted files pin lost or duplicated %q: %s", att.URI, pin)
		}
	}
}

const attachmentWorkspace = "/workspace/project"

func attachmentApplication(
	t *testing.T,
	store *durable.Store,
	config durable.AgentConfig,
	agentID, query string,
	fs *durable.FilesystemBinding,
) *durable.Run {
	t.Helper()
	return acceptAttachmentRun(t, store, execution.SubmitCommand{
		Kind: execution.SubmitApplication,
		ExecutionSpec: execution.ExecutionSpec{
			Scope: &workspace.Scope{
				TenantID: "tenant",
				ActorID:  "owner",
				TaskID:   "files-test",
				RoundID:  durable.NewID(),
			},
			AgentID:      agentID,
			AgentVersion: 1,
			Query:        query,
			Filesystem:   fs,
		},
	}, durable.PrepareApplicationSubmission(config, agentID, 1, nil))
}

func workspaceBinding(provider, endpoint string) *durable.FilesystemBinding {
	return &durable.FilesystemBinding{
		Provider:  provider,
		URL:       endpoint,
		TokenEnv:  "WORKER_ATTACHMENT_TEST_TOKEN",
		Workspace: attachmentWorkspace,
	}
}

func storedFileArtifacts(t *testing.T, store *durable.Store, run *durable.Run) []durable.FileArtifact {
	t.Helper()
	messages, err := store.Messages(t.Context(), "owner", run.SessionID, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []durable.FileArtifact
	for _, message := range messages {
		if message.RunID == run.ID && len(message.Artifacts) > 0 {
			if message.Kind != "Tool" || message.ToolCallID == "" || message.Content == "" {
				t.Fatalf("artifact stored outside a completed Tool message: %+v", message)
			}
			artifacts = append(artifacts, message.Artifacts...)
		}
	}
	return artifacts
}

func TestArtifactFilesAIOAcrossTakeoverAndRuns(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compacted=%t", compact), func(t *testing.T) {
			testArtifactFilesAIO(t, compact)
		})
	}
}

func testArtifactFilesAIO(t *testing.T, compact bool) {
	store, _ := testDB(t)
	t.Setenv("WORKER_ATTACHMENT_TEST_TOKEN", "test")
	attemptCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	report := attachmentWorkspace + "/report.txt"
	draft := attachmentWorkspace + "/draft.txt"
	var writes, edits, barriers atomic.Int32
	aio := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/sandbox":
			fmt.Fprint(w, `{"success":true}`)
		case "/barrier":
			if barriers.Add(1) == 1 {
				cancel() // The write is committed, and the next model has not been dispatched.
			}
			fmt.Fprint(w, `{"ok":true}`)
		case "/v1/file/write", "/v1/file/read", "/v1/file/replace":
			var input struct {
				File string `json:"file"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Errorf("decode AIO file request: %v", err)
			}
			switch r.URL.Path {
			case "/v1/file/write":
				if n := writes.Add(
					1,
				); (n == 1 && input.File != report) ||
					(n == 2 && input.File != draft) {
					t.Errorf("unexpected write %d: %s", n, input.File)
				}
			case "/v1/file/read":
				if input.File != report {
					t.Errorf("unexpected read: %s", input.File)
				}
				fmt.Fprint(w, `{"success":true,"data":{"content":"one"}}`)
				return
			case "/v1/file/replace":
				edits.Add(1)
				if input.File != report {
					t.Errorf("unexpected edit: %s", input.File)
				}
			}
			fmt.Fprint(w, `{"success":true}`)
		default:
			t.Errorf("unexpected AIO request: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(aio.Close)

	var adopted atomic.Pointer[durable.Run]
	url, capture := attachmentModelServer(t, func(i int, _ attachmentModelRequest) subAgentModelReply {
		switch i {
		case 0:
			return subAgentModelReply{calls: []schema.ToolCall{
				subAgentReplyCall("write_file", `{"file_path":"output/../report.txt","content":"one"}`),
				subAgentReplyCall("barrier", "{}"),
			}}
		case 1:
			return subAgentModelReply{calls: []schema.ToolCall{
				subAgentReplyCall("write_file", `{"file_path":"draft.txt","content":"draft"}`),
				subAgentReplyCall(
					"edit_file",
					`{"file_path":"/workspace/project/./report.txt","old_string":"one","new_string":"two","replace_all":false}`,
				),
			}}
		case 2:
			if compact {
				// Complete subsequently publishes this cursor to the Session. It
				// covers the last Tool row so history cannot supply the file.
				lease := adopted.Load().Lease()
				projection, err := store.CompactionContext(t.Context(), lease)
				if err != nil || len(projection.Messages) == 0 {
					t.Errorf("load compactable messages: %v", err)
				} else if err := store.CompactRun(
					t.Context(),
					lease,
					"Prior work completed.",
					projection.Messages[len(projection.Messages)-1].Seq+1,
				); err != nil {
					t.Errorf("compact first run: %v", err)
				}
			}
		}
		return subAgentModelReply{content: "done"}
	})
	config := durable.AgentConfig{
		Model: durable.ModelSpec{Name: "files", BaseURL: url, Stream: new(false)}, MaxTurns: 5,
		Toolsets: []string{"filesystem"},
		Tools: []durable.ToolSpec{
			{
				Name:       "barrier",
				URL:        aio.URL + "/barrier",
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}
	agentID, _ := createPublishedAgentFixture(t, store, "owner", "AIO attachment agent", config)
	fs := workspaceBinding("AIO", aio.URL)
	run := attachmentApplication(t, store, config, agentID, "create the report", fs)
	claim := claimSubAgentRun(t, store, run.ID)
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	if err := worker.Execute(attemptCtx, claim); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected worker to stop before the next model request, got %v", err)
	}
	saved, state, _ := compressionResult(t, store, run)
	if saved.Status != durable.Running || len(state.Checkpoint) == 0 || state.InflightModel != nil ||
		len(state.ArtifactFiles) != 1 || state.ArtifactFiles[0].URI != report ||
		!state.ArtifactFiles[0].Accessible || state.ArtifactFiles[0].Action != "written" {
		t.Fatalf(
			"checkpoint after write: status=%s files=%+v error=%q",
			saved.Status,
			state.ArtifactFiles,
			saved.Error,
		)
	}
	want := []durable.FileArtifact{{Path: report, Name: "report.txt", Provider: "AIO", Action: "written"}}
	if got := storedFileArtifacts(t, store, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("file records before replacement=%+v, want %+v", got, want)
	}
	replacement := expireSubAgentLease(t, store, claim)
	adopted.Store(replacement)
	done := executeSubAgentRun(t, &engine.Engine{Store: store, Build: adapters.Build}, replacement)
	if done.Status != durable.Completed || done.Output != "done" || writes.Load() != 2 ||
		edits.Load() != 1 || barriers.Load() != 2 || (done.FirstKeptSeq > 0) != compact {
		messages, _ := store.Messages(t.Context(), "owner", run.SessionID, 0, 50)
		t.Logf("stored messages: %+v", messages)
		t.Fatalf("takeover status=%s error=%q writes=%d edits=%d barriers=%d firstKeptSeq=%d",
			done.Status, done.Error, writes.Load(), edits.Load(), barriers.Load(), done.FirstKeptSeq)
	}
	want = append(want,
		durable.FileArtifact{Path: draft, Name: "draft.txt", Provider: "AIO", Action: "written"},
		durable.FileArtifact{Path: report, Name: "report.txt", Provider: "AIO", Action: "edited"},
	)
	if got := storedFileArtifacts(t, store, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("file records after takeover=%+v, want %+v", got, want)
	}
	_, state, _ = compressionResult(t, store, run)
	if len(state.ArtifactFiles) != 2 || state.ArtifactFiles[0].URI != report ||
		state.ArtifactFiles[0].Action != "edited" || state.ArtifactFiles[1].URI != draft {
		t.Fatalf("live index should be newest-first and deduplicated: %+v", state.ArtifactFiles)
	}

	next := attachmentApplication(t, store, config, agentID, "continue the report", fs)
	if next.SessionID != run.SessionID {
		t.Fatal("follow-up did not reuse the Session")
	}
	assertRunCompleted(t, worker, next)
	requests := capture()
	if len(requests) != 4 {
		t.Fatalf("got %d model requests, want 4", len(requests))
	}
	writtenPin := `[workspace/accessible] written "report.txt" "` + report + `"`
	editedPin := `[workspace/accessible] edited "report.txt" "` + report + `"`
	draftPin := `[workspace/accessible] written "draft.txt" "` + draft + `"`
	if pin := pinnedFiles(t, requests[1]); pin != writtenPin {
		t.Fatalf("wrong file pin immediately after takeover: %s", pin)
	}
	for _, req := range requests[2:] {
		if pin := pinnedFiles(t, req); pin != editedPin+"\n"+draftPin {
			t.Fatalf("wrong live or frozen file pin: %s", pin)
		}
	}
}

func TestArtifactFilesCustomWriteFileHasNoFileAction(t *testing.T) {
	store, _ := testDB(t)
	url, capture := attachmentModelServer(t, func(i int, _ attachmentModelRequest) subAgentModelReply {
		if i == 0 {
			return subAgentModelReply{
				calls: []schema.ToolCall{subAgentReplyCall("write_file", `{"file_path":"custom.txt"}`)},
			}
		}
		return subAgentModelReply{content: "done"}
	})
	var calls atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"saved":true}`)
	}))
	t.Cleanup(custom.Close)
	config := durable.AgentConfig{
		Model: durable.ModelSpec{Name: "files", BaseURL: url, Stream: new(false)}, MaxTurns: 5,
		Tools: []durable.ToolSpec{
			{
				Name:       "write_file",
				URL:        custom.URL,
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object"}`),
			},
		},
	}
	session := newAttachmentSession(t, store, config)
	run := attachmentChat(t, store, session, config, "use the HTTP tool")
	// Populate the workspace without loading its filesystem toolset so that a
	// name-based hook could otherwise mistake this HTTP tool for a file write.
	claim := claimSubAgentRun(t, store, run.ID)
	claim.Spec.Filesystem = workspaceBinding("AIO", custom.URL)
	done := executeSubAgentRun(t, &engine.Engine{Store: store, Build: adapters.Build}, claim)
	if done.Status != durable.Completed || calls.Load() != 1 {
		t.Fatalf(
			"custom tool did not complete: status=%s calls=%d error=%q",
			done.Status,
			calls.Load(),
			done.Error,
		)
	}
	_, state, _ := compressionResult(t, store, run)
	if len(state.ArtifactFiles) != 0 || len(storedFileArtifacts(t, store, run)) != 0 {
		t.Fatalf("custom HTTP tool produced a workspace artifact: %+v", state.ArtifactFiles)
	}
	requests := capture()
	if len(requests) != 2 || len(requests[1].users()) != 1 ||
		onlyText(t, requests[1].users()[0]) != "use the HTTP tool" {
		t.Fatalf("unexpected custom-tool requests: %v", requests)
	}
}

func TestArtifactFilesFreshSandboxShowsHistoricalPath(t *testing.T) {
	store, _ := testDB(t)
	t.Setenv("WORKER_ATTACHMENT_TEST_TOKEN", "test")
	filePath := attachmentWorkspace + "/sandbox.txt"
	var acquired, writes, released atomic.Int32
	center := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		sandboxID := strings.TrimPrefix(r.URL.Path, "/v1/sandboxes/")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/sandboxes":
			var input sdk.AcquireSandboxRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil ||
				input.RequiredSave || input.RestoreFromSandboxID != "" {
				t.Errorf("invalid fresh workspace acquire: %+v, %v", input, err)
			}
			sandboxID = fmt.Sprintf("sandbox-%d", acquired.Add(1))
			view := sdk.SandboxView{
				Sandbox: &sdk.Sandbox{ID: sandboxID, Status: sdk.SandboxStatusActive, Generation: 1},
				Lease: &sdk.SandboxLease{
					ID:            "lease-" + sandboxID,
					Revision:      1,
					Status:        "Active",
					HardExpiresAt: time.Now().Add(time.Minute),
				},
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.Method == http.MethodGet && strings.HasPrefix(sandboxID, "sandbox-"):
			_ = json.NewEncoder(w).Encode(sdk.SandboxView{
				Sandbox: &sdk.Sandbox{ID: sandboxID, Status: sdk.SandboxStatusActive, Generation: 1},
			})
		case r.Method == http.MethodPost && sandboxID == "sandbox-1/operations":
			var input sdk.ExecSandboxRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil ||
				!strings.Contains(input.Script, filePath) {
				t.Errorf("unexpected write receipt request: %+v, %v", input, err)
			}
			writes.Add(1)
			_ = json.NewEncoder(w).
				Encode(sdk.SandboxOperation{ID: "write", Status: sdk.SandboxOpSucceeded, ExitCode: new(0)})
		case r.Method == http.MethodDelete && strings.HasPrefix(sandboxID, "sandbox-"):
			released.Add(1)
			_ = json.NewEncoder(w).Encode(sdk.Sandbox{ID: sandboxID, Status: sdk.SandboxStatusDestroyed})
		default:
			t.Errorf("unexpected Sandbox request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(center.Close)

	url, capture := attachmentModelServer(t, func(i int, _ attachmentModelRequest) subAgentModelReply {
		if i == 0 {
			return subAgentModelReply{calls: []schema.ToolCall{
				subAgentReplyCall("write_file", `{"file_path":"sandbox.txt","content":"one"}`),
			}}
		}
		return subAgentModelReply{content: "done"}
	})
	config := durable.AgentConfig{
		Model:    durable.ModelSpec{Name: "files", BaseURL: url, Stream: new(false)},
		MaxTurns: 5, Toolsets: []string{"filesystem"},
	}
	agentID, _ := createPublishedAgentFixture(t, store, "owner", "Sandbox attachment agent", config)
	fs := workspaceBinding("Sandbox", center.URL)
	first := attachmentApplication(t, store, config, agentID, "create a file in the sandbox", fs)
	worker := &engine.Engine{Store: store, Build: adapters.Build}
	assertRunCompleted(t, worker, first)
	expected := []durable.FileArtifact{
		{Path: filePath, Name: "sandbox.txt", Action: "written", Provider: "Sandbox"},
	}
	if got := storedFileArtifacts(t, store, first); !reflect.DeepEqual(got, expected) {
		t.Fatalf("Sandbox file records=%+v, want %+v", got, expected)
	}

	next := attachmentApplication(t, store, config, agentID, "find the previous file", fs)
	if next.SessionID != first.SessionID {
		t.Fatal("follow-up did not reuse the Session")
	}
	assertRunCompleted(t, worker, next)
	requests := capture()
	if len(requests) != 3 || acquired.Load() != 2 || released.Load() != 2 || writes.Load() != 1 {
		t.Fatalf(
			"requests=%d acquired=%d released=%d writes=%d",
			len(requests),
			acquired.Load(),
			released.Load(),
			writes.Load(),
		)
	}
	if pin := pinnedFiles(t, requests[1]); pin != `[workspace/accessible] written "sandbox.txt" "`+filePath+`"` {
		t.Fatalf("wrong current Sandbox pin: %s", pin)
	}
	if pin := pinnedFiles(t, requests[2]); pin != `[workspace/historical] written "sandbox.txt" "`+filePath+`"` {
		t.Fatalf("wrong fresh Sandbox pin: %s", pin)
	}
}
