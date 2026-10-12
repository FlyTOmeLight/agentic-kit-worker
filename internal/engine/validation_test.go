package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

func TestInvalidArgumentsReturnToModelWithoutMutation(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "edit_file", durable.Unsafe)
	validate, err := engine.CompileArguments(&schema.ToolInfo{Name: "edit_file", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"path": {Type: schema.String, Required: true}})})
	if err != nil {
		t.Fatal(err)
	}
	modelCalls, mutations := 0, 0
	e := engine.Engine{Store: s, Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
		llm := &fakeModel{generate: func(_ context.Context, in []*schema.Message) (*schema.Message, error) {
			modelCalls++
			if modelCalls == 1 {
				return toolReply("edit_file"), nil
			}
			if modelCalls == 2 {
				if !strings.Contains(in[len(in)-1].Content, "INVALID_TOOL_INPUT") {
					t.Fatalf("missing correction feedback: %s", in[len(in)-1].Content)
				}
				msg := toolReply("edit_file")
				msg.ToolCalls[0].Function.Arguments = `{"path":"calc.py"}`
				return msg, nil
			}
			return schema.AssistantMessage("done", nil), nil
		}}
		return llm, map[string]engine.Binding{"edit_file": {Policy: durable.Unsafe, Validate: validate, Tool: &fakeTool{name: "edit_file", invoke: func(context.Context, string) (string, error) { mutations++; return "edited", nil }}}}, nil
	}}
	claimed, err := s.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "owner", r.ID)
	if err != nil || got.Status != durable.Completed || modelCalls != 3 || mutations != 1 {
		t.Fatalf("status=%s model=%d mutations=%d err=%v", got.Status, modelCalls, mutations, err)
	}
}
