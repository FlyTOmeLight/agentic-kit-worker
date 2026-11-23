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
		run: &durable.Run{Spec: durable.Spec{Query: "question", MaxTurns: 2, PricingFrozen: true}},
		snapshot: &contextbuilder.Snapshot{
			Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4},
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
		run: &durable.Run{},
		snapshot: &contextbuilder.Snapshot{
			Materials: contextbuilder.FrozenMaterials{
				Tools: []contextbuilder.ToolDefinition{
					{Name: "unused", Description: strings.Repeat("unused ", 100)},
				},
			},
			Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4},
			Budget: contextbuilder.Budget{MaxInputBytes: 100},
		},
	}
	messages, _, _, err := state.prepareModelInput(
		context.Background(),
		[]*schema.Message{schema.UserMessage("question")},
		nil,
		0,
		0,
		0,
	)
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
				Version:               contextbuilder.PolicyVersionV4,
				KeepRecentToolResults: 2,
			},
			Budget: contextbuilder.Budget{CounterVersion: contextbuilder.CJKWeightedCounterVersion},
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

	_, full, _, err := state.prepareModelInput(context.Background(), messages, nil, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if full.ProjectedToolResults != 0 {
		t.Fatalf("unlimited build projected %d results", full.ProjectedToolResults)
	}
	state.snapshot.Budget.MaxInputBytes = full.InputBytes - 1
	built, info, _, err := state.prepareModelInput(context.Background(), messages, nil, 1, 0, full.InputBytes-1)
	if err != nil {
		t.Fatal(err)
	}
	if info.ProjectedToolResults != 1 {
		t.Fatalf("projected %d, want 1", info.ProjectedToolResults)
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
		case strings.HasPrefix(message.Content, "[Tool result stub:"):
			markers++
		case message.Content == content:
			intact++
		default:
			t.Fatalf("unexpected tool content: %q", message.Content)
		}
	}
	if markers != 1 || intact != 4 {
		t.Fatalf("markers=%d intact=%d, want 1 and 4", markers, intact)
	}
	if messages[2].Content != content {
		t.Fatal("projection mutated the Eino state messages")
	}
}

func TestUsageAnchorTriggersProactiveCompression(t *testing.T) {
	state := &execution{
		run: &durable.Run{Spec: durable.Spec{Query: "question"}},
		snapshot: &contextbuilder.Snapshot{
			Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4},
			Budget: contextbuilder.Budget{
				MaxInputTokens: 1000,
				CounterVersion: contextbuilder.CJKWeightedCounterVersion,
			},
			Materials: contextbuilder.FrozenMaterials{History: []contextbuilder.MessageGroup{{
				Source: contextbuilder.Source{Kind: contextbuilder.SourceHistory, ID: "previous"},
				Messages: []contextbuilder.Message{
					{Role: contextbuilder.RoleUser, Content: "older question"},
					{Role: contextbuilder.RoleAssistant, Content: strings.Repeat("abc ", 450)},
				},
			}}},
		},
		state: State{Turn: 2, UsageAnchor: &UsageAnchor{Turn: 0, PromptTokens: 920, EstimatedInputTokens: 300}},
	}
	input := []*schema.Message{schema.UserMessage("question")}
	_, without, _, err := state.prepareModelInput(context.Background(), input, nil, 0, 0, 0)
	if err != nil || without.UsedUsageAnchor || len(without.OmittedSources) != 0 {
		t.Fatalf("stale anchor affected projection: %+v error=%v", without, err)
	}
	state.state.UsageAnchor.Turn = 1
	_, with, _, err := state.prepareModelInput(context.Background(), input, nil, 0, 0, 0)
	if err != nil || !with.UsedUsageAnchor || len(with.OmittedSources) != 1 ||
		with.InputTokens >= without.InputTokens {
		t.Fatalf("fresh anchor did not reduce projection: %+v error=%v", with, err)
	}
}

func TestLadderPreservesEinoMetadataAndTruncatesOutboundArguments(t *testing.T) {
	state := &execution{run: &durable.Run{Spec: durable.Spec{Query: "question"}}}
	result := strings.Repeat("content ", 1000)
	args := fmt.Sprintf(`{"file":%q}`, strings.Repeat("argument ", 400))
	input := []*schema.Message{schema.UserMessage("question")}
	for i, name := range []string{"lookup", readStepResultTool} {
		id := fmt.Sprintf("call-%d", i)
		assistant := schema.AssistantMessage("searching", []schema.ToolCall{{
			ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: args},
			Extra: map[string]any{"signature": "signed"},
		}})
		assistant.ReasoningContent = "private reasoning"
		assistant.Name = "provider"
		assistant.ResponseMeta = &schema.ResponseMeta{FinishReason: "tool_calls"}
		input = append(input, assistant, schema.ToolMessage(result, id))
	}
	out, info, _, err := state.prepareModelInput(context.Background(), input, nil, 1, 0, 17100)
	if err != nil || info.LadderPreviewed != 1 || info.DroppedRounds != 0 {
		t.Fatalf("ladder projection info=%+v error=%v", info, err)
	}
	if out[1].ToolCalls[0].Function.Arguments == args || out[1].ToolCalls[0].Extra["signature"] != "signed" ||
		out[1].Name != input[1].Name || out[1].ReasoningContent != input[1].ReasoningContent ||
		out[1].ResponseMeta != input[1].ResponseMeta || out[4].Content != result ||
		input[1].ToolCalls[0].Function.Arguments != args || input[2].Content != result {
		t.Fatal("outbound projection lost model metadata, changed exempt output, or mutated original messages")
	}
}
