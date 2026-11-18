package adapters

import (
	"context"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

type modelWithoutCallbacks struct {
	model.ToolCallingChatModel
}

func (base *modelWithoutCallbacks) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return base, nil
}

type callbackSwitchModel struct {
	model.ToolCallingChatModel
	enabled bool
	bound   model.ToolCallingChatModel
}

func (base *callbackSwitchModel) IsCallbacksEnabled() bool {
	return base.enabled
}

func (base *callbackSwitchModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return base.bound, nil
}

func TestUsageModelForwardsCallbacks(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		base  model.ToolCallingChatModel
		bound model.ToolCallingChatModel
	}{
		{name: "absent", base: &modelWithoutCallbacks{}, bound: &modelWithoutCallbacks{}},
		{name: "disabled", base: &callbackSwitchModel{}, bound: &callbackSwitchModel{}},
		{name: "enabled", base: &callbackSwitchModel{enabled: true}, bound: &callbackSwitchModel{enabled: true}},
		{name: "bound disables", base: &callbackSwitchModel{enabled: true}, bound: &callbackSwitchModel{}},
		{name: "bound enables", base: &callbackSwitchModel{}, bound: &callbackSwitchModel{enabled: true}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if base, ok := testCase.base.(*callbackSwitchModel); ok {
				base.bound = testCase.bound
			}
			wrapped := &usageModel{ToolCallingChatModel: testCase.base}
			if got, want := components.IsCallbacksEnabled(
				wrapped,
			), components.IsCallbacksEnabled(
				testCase.base,
			); got != want {
				t.Fatalf("callbacks before WithTools=%t, want %t", got, want)
			}
			bound, err := wrapped.WithTools([]*schema.ToolInfo{{Name: "lookup"}})
			if err != nil {
				t.Fatal(err)
			}
			if got, want := components.IsCallbacksEnabled(
				bound,
			), components.IsCallbacksEnabled(
				testCase.bound,
			); got != want {
				t.Fatalf("callbacks after WithTools=%t, want %t", got, want)
			}
		})
	}
}

func TestBuildModelPreservesCallbacks(t *testing.T) {
	base, err := buildModel(context.Background(), durable.ModelSpec{Name: "model", BaseURL: "http://localhost"})
	if err != nil {
		t.Fatal(err)
	}
	if !components.IsCallbacksEnabled(base) {
		t.Fatal("OpenAI callbacks disabled before WithTools")
	}
	bound, err := base.WithTools([]*schema.ToolInfo{{Name: "lookup"}})
	if err != nil {
		t.Fatal(err)
	}
	if !components.IsCallbacksEnabled(bound) {
		t.Fatal("OpenAI callbacks disabled after WithTools")
	}
}
