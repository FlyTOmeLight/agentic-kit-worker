// Package engine persists the ReAct loop at model/tool boundaries. Eino remains the model/tool interface;
// an ADK clarification checkpoint alone does not capture arbitrary process failures.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

type Binding struct {
	Tool   tool.InvokableTool
	Policy durable.Policy
}

// Build must resolve the immutable run configuration, never the latest editable agent configuration.
type Build func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]Binding, error)
type Engine struct {
	Store *durable.Store
	Build Build
}
type State struct {
	Version     int               `json:"version"`
	Messages    []*schema.Message `json:"messages"`
	Turn        int               `json:"turn"`
	Pending     []schema.ToolCall `json:"pending,omitempty"`
	NextTool    int               `json:"nextTool"`
	WaitingStep string            `json:"waitingStep,omitempty"`
}

func encode(s State) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return b
}
func decode(run *durable.Run) (State, error) {
	var state State
	if err := json.Unmarshal(run.State, &state); err != nil {
		return state, err
	}
	if state.Version == 0 {
		state = State{Version: 1, Messages: []*schema.Message{schema.SystemMessage(run.Spec.SystemPrompt)}}
		for _, msg := range run.Spec.History {
			state.Messages = append(state.Messages, &schema.Message{Role: schema.RoleType(msg.Role), Content: msg.Content})
		}
		state.Messages = append(state.Messages, schema.UserMessage(run.Spec.Query))
	} else if state.Version != 1 {
		return state, fmt.Errorf("unsupported checkpoint version %d", state.Version)
	}
	return state, nil
}

type operationKey struct{}

// OperationID is stable across worker attempts. Idempotent adapters MUST forward it to the downstream service.
func OperationID(ctx context.Context) string { s, _ := ctx.Value(operationKey{}).(string); return s }
func (e *Engine) Execute(ctx context.Context, run *durable.Run) error {
	state, err := decode(run)
	if err != nil {
		return err
	}
	lease := run.Lease()
	llm, bindings, err := e.Build(ctx, run.Spec)
	if err != nil {
		return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", err.Error(), "RunFailed")
	}
	infos := make([]*schema.ToolInfo, 0, len(run.Spec.Tools))
	for _, spec := range run.Spec.Tools {
		binding, ok := bindings[spec.Name]
		if !ok {
			return fmt.Errorf("tool not bound: %s", spec.Name)
		}
		if binding.Policy != spec.Policy {
			return fmt.Errorf("tool policy mismatch: %s", spec.Name)
		}
		info, err := binding.Tool.Info(ctx)
		if err != nil {
			return err
		}
		infos = append(infos, info)
	}
	if len(infos) > 0 {
		llm, err = llm.WithTools(infos)
		if err != nil {
			return err
		}
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		// Complete persisted model decisions before asking the model to make new ones.
		for state.NextTool < len(state.Pending) {
			call := state.Pending[state.NextTool]
			binding, ok := bindings[call.Function.Name]
			if !ok {
				return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", "model requested unknown tool: "+call.Function.Name, "RunFailed")
			}
			input := call.Function.Name + "\x00" + call.Function.Arguments
			result, cached, err := e.Store.BeginStep(ctx, lease, call.ID, input, binding.Policy)
			if errors.Is(err, durable.ErrUncertain) {
				state.WaitingStep = call.ID
				return e.Store.Save(ctx, lease, encode(state), durable.NeedsReview, "", "tool outcome unknown: "+call.Function.Name, "RunNeedsReview")
			}
			if err != nil {
				return err
			}
			if !cached && call.Function.Name == "ask_for_clarification" {
				state.WaitingStep = call.ID
				return e.Store.Save(ctx, lease, encode(state), durable.WaitingClarification, "", call.Function.Arguments, "ClarificationRequested")
			}
			if !cached {
				toolCtx := context.WithValue(ctx, operationKey{}, run.ID+":"+call.ID)
				result, err = binding.Tool.InvokableRun(toolCtx, call.Function.Arguments)
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if binding.Policy == durable.Unsafe {
						state.WaitingStep = call.ID
						return e.Store.Save(ctx, lease, encode(state), durable.NeedsReview, "", err.Error(), "RunNeedsReview")
					}
					return e.Store.Retry(ctx, lease, encode(state), err.Error(), time.Second*time.Duration(run.Attempt))
				}
			}
			state.Messages = append(state.Messages, schema.ToolMessage(result, call.ID))
			state.NextTool++
			state.WaitingStep = ""
			if cached {
				err = e.Store.Save(ctx, lease, encode(state), durable.Running, "", "", "ToolResultRestored")
			} else {
				err = e.Store.FinishStep(ctx, lease, call.ID, result, encode(state))
			}
			if err != nil {
				return err
			}
		}
		if state.Turn >= run.Spec.MaxTurns {
			return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", "maximum model turns exceeded", "RunFailed")
		}
		state.Pending = nil
		state.NextTool = 0
		msg, err := llm.Generate(ctx, state.Messages)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return e.Store.Retry(ctx, lease, encode(state), err.Error(), time.Second*time.Duration(run.Attempt))
		}
		if msg == nil {
			return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", "model returned no message", "RunFailed")
		}
		if msg.ResponseMeta != nil && (msg.ResponseMeta.FinishReason == "length" || msg.ResponseMeta.FinishReason == "content_filter") {
			return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", "incomplete model response: "+msg.ResponseMeta.FinishReason, "RunFailed")
		}
		state.Turn++
		// Stable local IDs avoid missing/duplicate provider tool IDs and survive every recovery attempt.
		for i := range msg.ToolCalls {
			msg.ToolCalls[i].ID = fmt.Sprintf("turn_%d_tool_%d", state.Turn, i)
			if !json.Valid([]byte(msg.ToolCalls[i].Function.Arguments)) {
				return e.Store.Save(ctx, lease, encode(state), durable.Failed, "", "invalid tool arguments", "RunFailed")
			}
		}
		state.Messages = append(state.Messages, msg)
		state.Pending = msg.ToolCalls
		if len(msg.ToolCalls) == 0 {
			return e.Store.Save(ctx, lease, encode(state), durable.Completed, msg.Content, "", "RunCompleted")
		}
		// Persist the model's chosen tool arguments BEFORE any external side effect.
		decision := durable.Message{Role: "assistant", Content: msg.Content}
		for _, call := range msg.ToolCalls {
			decision.ToolCalls = append(decision.ToolCalls, durable.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments})
		}
		if err = e.Store.SaveDecision(ctx, lease, encode(state), decision); err != nil {
			return err
		}
	}
}
