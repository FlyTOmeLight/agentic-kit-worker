package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
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
	messages, err := state.buildContext([]*schema.Message{schema.UserMessage("question")}, nil)
	if err != nil || len(messages) != 1 || messages[0].Content != "question" {
		t.Fatalf("empty active tools must replace frozen tools: messages=%+v error=%v", messages, err)
	}
	if len(state.snapshot.Materials.Tools) != 1 {
		t.Fatal("frozen snapshot mutated")
	}
}
