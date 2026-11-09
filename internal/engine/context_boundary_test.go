package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func TestModelBoundaryCountsActualToolDefinitionsBeforeCallingModel(t *testing.T) {
	state := &execution{
		run: &durable.Run{Spec: durable.Spec{Query: "question", MaxTurns: 2}},
		snapshot: &contextbuilder.Snapshot{
			Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV2},
			Budget: contextbuilder.Budget{MaxInputBytes: 200},
		},
	}
	boundary := &boundaryModel{execution: state}
	_, err := boundary.Generate(
		context.Background(),
		[]*schema.Message{schema.UserMessage("question")},
		model.WithTools(
			[]*schema.ToolInfo{{Name: "filesystem", Desc: strings.Repeat("schema description ", 100)}},
		),
	)
	if !errors.Is(err, contextbuilder.ErrBudgetExceeded) {
		t.Fatalf("large dynamic tool definition was not rejected: %v", err)
	}
	if state.retry {
		t.Fatal("deterministic budget failure must not request retry")
	}
}

func TestBuildContextUsesExplicitEmptyTools(t *testing.T) {
	state := &execution{
		snapshot: &contextbuilder.Snapshot{
			Materials: contextbuilder.FrozenMaterials{
				Tools: []contextbuilder.ToolDefinition{
					{Name: "unused", Description: strings.Repeat("unused ", 100)},
				},
			},
			Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV2},
			Budget: contextbuilder.Budget{MaxInputBytes: 100},
		},
	}
	messages, _, err := state.buildContext([]*schema.Message{schema.UserMessage("question")}, nil)
	if err != nil || len(messages) != 1 || messages[0].Content != "question" {
		t.Fatalf("empty active tools must replace frozen tools: messages=%+v error=%v", messages, err)
	}
	if len(state.snapshot.Materials.Tools) != 1 {
		t.Fatal("frozen snapshot mutated")
	}
}

func TestBuildContextProjectsOlderCurrentToolResults(t *testing.T) {
	state := &execution{
		run: &durable.Run{Spec: durable.Spec{Query: "question"}},
		snapshot: &contextbuilder.Snapshot{
			Policy: contextbuilder.Policy{
				Version:               contextbuilder.PolicyVersionV3,
				KeepRecentToolResults: 2,
			},
			Budget: contextbuilder.Budget{CounterVersion: contextbuilder.ByteEstimateVersion},
		},
	}
	content := strings.Repeat("result ", 50)
	messages := []*schema.Message{schema.UserMessage("question")}
	for index := 0; index < 5; index++ {
		id := fmt.Sprintf("call-%d", index)
		messages = append(messages,
			&schema.Message{
				Role: schema.Assistant,
				ToolCalls: []schema.ToolCall{{
					ID:       id,
					Type:     "function",
					Function: schema.FunctionCall{Name: "read"},
				}},
			},
			schema.ToolMessage(content, id),
		)
	}

	_, full, err := state.buildContext(messages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if full.ProjectedToolResults != 0 {
		t.Fatalf("unlimited build projected %d results", full.ProjectedToolResults)
	}
	state.snapshot.Budget.MaxInputBytes = full.InputBytes - 1
	built, info, err := state.buildContext(messages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if info.ProjectedToolResults != 3 {
		t.Fatalf("projected %d, want 3", info.ProjectedToolResults)
	}
	if info.InputBytes > state.snapshot.Budget.MaxInputBytes || info.InputBytes >= full.InputBytes {
		t.Fatalf("usage %d, want within %d and below %d",
			info.InputBytes, state.snapshot.Budget.MaxInputBytes, full.InputBytes)
	}
	var markers, intact int
	for _, message := range built {
		if message.Role != schema.Tool {
			continue
		}
		switch {
		case strings.HasPrefix(message.Content, "[Tool result projected:"):
			markers++
		case message.Content == content:
			intact++
		default:
			t.Fatalf("unexpected tool content: %q", message.Content)
		}
	}
	if markers != 3 || intact != 2 {
		t.Fatalf("markers=%d intact=%d, want 3 and 2", markers, intact)
	}
	if messages[2].Content != content {
		t.Fatal("projection mutated the Eino state messages")
	}
}
