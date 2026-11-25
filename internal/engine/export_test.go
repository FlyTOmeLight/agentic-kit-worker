package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func ModelBoundaryForTest(
	t *testing.T,
	store *durable.Store,
	run *durable.Run,
	base model.BaseChatModel,
) model.BaseChatModel {
	t.Helper()
	execution := &execution{store: store, run: run}
	if err := json.Unmarshal(run.State, &execution.state); err != nil {
		t.Fatal(err)
	}
	execution.state.Version = 3
	execution.bindings = map[string]Binding{"lookup": {Policy: durable.ReadOnly}}
	return &boundaryModel{BaseChatModel: base, execution: execution}
}

func SetDecisionCheckpointForTest(
	t *testing.T,
	store *durable.Store,
	run *durable.Run,
) (State, error) {
	t.Helper()
	execution := &execution{store: store, run: run}
	if err := json.Unmarshal(run.State, &execution.state); err != nil {
		t.Fatal(err)
	}
	execution.state.Version = 3
	execution.bindings = map[string]Binding{"lookup": {Policy: durable.ReadOnly}}
	boundary := &boundaryModel{BaseChatModel: &staticToolModel{}, execution: execution}
	if _, err := boundary.Generate(
		context.Background(),
		[]*schema.Message{schema.UserMessage("do work")},
	); err != nil {
		t.Fatal(err)
	}
	setCtx, cancel := context.WithCancel(context.Background())
	cancel()
	return execution.state, execution.Set(setCtx, run.ID, []byte("checkpoint"))
}

type staticToolModel struct{}

func (m *staticToolModel) Generate(
	context.Context,
	[]*schema.Message,
	...model.Option,
) (*schema.Message, error) {
	message := &schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{{
			ID:   "turn_0_tool_0",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "lookup",
				Arguments: `{"question":"which version?"}`,
			},
		}},
	}
	message.ResponseMeta = &schema.ResponseMeta{Usage: &schema.TokenUsage{TotalTokens: 7}}
	return message, nil
}

func (m *staticToolModel) Stream(
	ctx context.Context,
	input []*schema.Message,
	opts ...model.Option,
) (*schema.StreamReader[*schema.Message], error) {
	message, err := m.Generate(ctx, input, opts...)
	return schema.StreamReaderFromArray([]*schema.Message{message}), err
}
