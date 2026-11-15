package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
)

func (s *execution) WrapModel(
	_ context.Context,
	m model.BaseChatModel,
	_ *adk.ModelContext,
) (model.BaseChatModel, error) {
	return &boundaryModel{BaseChatModel: m, execution: s}, nil
}

type boundaryModel struct {
	model.BaseChatModel
	execution *execution
}

func (m *boundaryModel) Generate(
	ctx context.Context,
	input []*schema.Message,
	opts ...model.Option,
) (*schema.Message, error) {
	s := m.execution
	if s.state.Final != nil {
		return schema.AssistantMessage(*s.state.Final, nil), nil
	}
	if s.state.Turn >= s.run.Spec.MaxTurns {
		return nil, fmt.Errorf("model turn limit exceeded: %d", s.run.Spec.MaxTurns)
	}
	_, err := s.store.TakePendingSteer(ctx, s.run.Lease(), func(steer string) json.RawMessage {
		s.state.Steers = append(s.state.Steers, steer)
		return s.encode()
	})
	if err != nil {
		return nil, err
	}
	if (s.summarizer != nil && s.compactionMaxBytes > 0) || s.run.CompactedSummary != "" {
		input, err = s.projectContext(ctx, input)
		if err != nil {
			return nil, err
		}
	}
	for _, item := range s.state.Steers {
		input = append(input, schema.UserMessage(item))
	}
	if err := s.store.CommitBoundary(ctx, s.run.Lease(), durable.Boundary{
		Kind:  durable.BoundaryModelStarted,
		State: s.encode(),
	}); err != nil {
		return nil, err
	}
	msg, err := m.BaseChatModel.Generate(ctx, input, opts...)
	if err != nil {
		s.retry = true
		return nil, err
	}
	if msg == nil {
		return nil, errors.New("model returned no message")
	}
	if msg.ResponseMeta != nil &&
		(msg.ResponseMeta.FinishReason == "length" || msg.ResponseMeta.FinishReason == "content_filter") {
		return nil, fmt.Errorf("incomplete model response: %s", msg.ResponseMeta.FinishReason)
	}
	s.state.Turn++
	s.state.ReturnDirectlyStep = ""

	if len(msg.ToolCalls) > 0 {
		var batchError error
		if len(msg.ToolCalls) > 1 {
			for _, call := range msg.ToolCalls {
				if s.bindings[call.Function.Name].EndRun {
					batchError = errors.New(
						"finish must be the only tool in a batch; no operation in this batch was executed",
					)
				}
			}
		}
		decision := &durable.Message{Role: "assistant", Content: msg.Content}
		for i := range msg.ToolCalls {
			call := &msg.ToolCalls[i]
			call.ID = fmt.Sprintf("turn_%d_tool_%d", s.state.Turn, i)
			if !json.Valid([]byte(call.Function.Arguments)) {
				return nil, errors.New("invalid tool arguments")
			}
			if _, ok := s.bindings[call.Function.Name]; !ok {
				return nil, fmt.Errorf("model requested unknown tool: %s", call.Function.Name)
			}
			binding := s.bindings[call.Function.Name]
			inputError := batchError
			if inputError == nil && binding.EndRun && binding.Validate != nil {
				inputError = binding.Validate(call.Function.Arguments)
			}
			if inputError == nil && s.state.ReturnDirectlyStep == "" &&
				(binding.EndRun || binding.ReturnDirectly) {
				s.state.ReturnDirectlyStep = call.ID
			}
			if inputError != nil {
				call.Function.Name = invalidInputTool
				call.Function.Arguments = toolInputFeedback(inputError)
			}
			decision.ToolCalls = append(
				decision.ToolCalls,
				durable.ToolCall{
					ID:        call.ID,
					Name:      call.Function.Name,
					Arguments: call.Function.Arguments,
				},
			)
		}
		s.decision = decision
	}
	return msg, nil
}

func (s *execution) wrapTool(next compose.InvokableToolEndpoint) compose.InvokableToolEndpoint {
	return func(ctx context.Context, input *compose.ToolInput) (*compose.ToolOutput, error) {
		// Every call in this response interrupts before side effects. Runner stores
		// its native checkpoint and the public model decision in one transaction.
		if s.decision != nil || s.pending != nil {
			return nil, tool.Interrupt(ctx, "save execution boundary")
		}
		// Eino's sequential ToolsNode visits later calls even if an earlier one failed.
		if s.toolError != nil {
			return nil, s.toolError
		}
		binding := s.bindings[input.Name]
		decision, err := s.store.BeginStep(ctx, s.run.Lease(), durable.StepIntent{
			ID:     input.CallID,
			Input:  input.Name + "\x00" + input.Arguments,
			Policy: binding.Policy,
		})
		if err != nil {
			s.toolError = err
			return nil, err
		}
		switch decision.Outcome {
		case durable.StepUncertain:
			return s.pause(
				ctx,
				input.CallID,
				durable.InteractionReview,
				"tool outcome unknown: "+input.Name,
			)
		case durable.StepReplay:
			s.recordReturnDirectly(input.CallID, decision.Result)
			return &compose.ToolOutput{Result: decision.Result}, nil
		case durable.StepExternalWait:
			return s.awaitExternal(
				ctx,
				input.CallID,
				input.Name+": "+input.Arguments,
			)
		}
		if input.Name == "ask_for_clarification" {
			return s.pause(ctx, input.CallID, durable.InteractionClarification, input.Arguments)
		}
		if binding.Validate != nil {
			if validationErr := binding.Validate(input.Arguments); validationErr != nil {
				feedback := toolInputFeedback(validationErr)
				if err = s.store.FinishStep(
					ctx,
					s.run.Lease(),
					input.CallID,
					feedback,
					s.encode(),
				); err != nil {
					return nil, err
				}
				return &compose.ToolOutput{Result: feedback}, nil
			}
		}
		ctx = context.WithValue(ctx, operationKey{}, s.run.ID+":"+input.CallID)
		ctx = sandboxAdapter.WithOperationID(ctx, s.run.ID+":"+input.CallID)
		output, err := next(ctx, input)
		if err != nil {
			if ctx.Err() != nil {
				s.toolError = ctx.Err()
				return nil, ctx.Err()
			}
			var correctable interface{ Correctable() bool }
			if errors.As(err, &correctable) && correctable.Correctable() {
				feedback := toolInputFeedback(err)
				if saveErr := s.store.FinishStep(
					ctx,
					s.run.Lease(),
					input.CallID,
					feedback,
					s.encode(),
				); saveErr != nil {
					return nil, saveErr
				}
				return &compose.ToolOutput{Result: feedback}, nil
			}
			if binding.Policy == durable.Unsafe {
				return s.pause(ctx, input.CallID, durable.InteractionReview, err.Error())
			}
			s.retry = true
			s.toolError = err
			return nil, err
		}
		// After a crash, BeginStep supplies this committed result to Eino without
		// invoking the external tool again, even if the checkpoint precedes the batch.
		previousFinal := s.state.Final
		s.recordReturnDirectly(input.CallID, output.Result)
		if err = s.store.FinishStep(ctx, s.run.Lease(), input.CallID, output.Result, s.encode()); err != nil {
			s.state.Final = previousFinal
			s.toolError = err
			return nil, err
		}
		return output, nil
	}
}

func (s *execution) recordReturnDirectly(callID, result string) {
	if callID == s.state.ReturnDirectlyStep && !toolHasError(result) {
		final := result
		s.state.Final = &final
	}
}

func (s *execution) pause(
	ctx context.Context,
	step string,
	kind durable.InteractionKind,
	detail string,
) (*compose.ToolOutput, error) {
	boundaryKind := durable.BoundaryAwaitReview
	if kind == durable.InteractionClarification {
		boundaryKind = durable.BoundaryAwaitClarification
	}
	s.pending = &durable.Boundary{Kind: boundaryKind, Detail: detail, WaitingStep: step}
	return nil, tool.Interrupt(ctx, "save execution boundary")
}

// awaitExternal suspends the run until the external system callback arrives.
// The tool itself is not invoked; the step record committed by BeginStep is
// the anchor for SupplyExternalResult.
func (s *execution) awaitExternal(
	ctx context.Context,
	step string,
	detail string,
) (*compose.ToolOutput, error) {
	s.pending = &durable.Boundary{
		Kind:        durable.BoundaryAwaitExternal,
		Detail:      detail,
		WaitingStep: step,
		ExternalRef: step,
	}
	return nil, tool.Interrupt(ctx, "save execution boundary")
}

// environmentBackend is the release contract of an environment-owning
// backend such as the sandbox adapter.
type environmentBackend interface {
	Released() bool
	Detach()
	Close(context.Context) error
}

func finishEnvironments(ctx context.Context, bindings map[string]Binding, release bool) {
	for _, binding := range bindings {
		backend, ok := binding.Tool.(environmentBackend)
		if !ok || backend.Released() {
			continue
		}
		if !release {
			backend.Detach()
			continue
		}
		releaseCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_ = backend.Close(releaseCtx)
		cancel()
	}
}
