package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
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
	if err := s.store.Save(ctx, s.run.Lease(), s.encode(), durable.Running, "", "", "ModelStarted"); err != nil {
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
	s.state.WaitingStep = ""
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
		if s.decision != nil || s.pauseStatus != "" {
			return nil, tool.Interrupt(ctx, "save execution boundary")
		}
		// Eino's sequential ToolsNode visits later calls even if an earlier one failed.
		if s.toolError != nil {
			return nil, s.toolError
		}
		binding := s.bindings[input.Name]
		result, cached, err := s.store.BeginStep(
			ctx,
			s.run.Lease(),
			input.CallID,
			input.Name+"\x00"+input.Arguments,
			binding.Policy,
		)
		if errors.Is(err, durable.ErrUncertain) {
			return s.pause(ctx, input.CallID, durable.NeedsReview, "tool outcome unknown: "+input.Name)
		}
		if err != nil {
			s.toolError = err
			return nil, err
		}
		if cached {
			s.state.WaitingStep = ""
			s.recordReturnDirectly(input.CallID, result)
			return &compose.ToolOutput{Result: result}, nil
		}
		if input.Name == "ask_for_clarification" {
			return s.pause(ctx, input.CallID, durable.WaitingClarification, input.Arguments)
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
				return s.pause(ctx, input.CallID, durable.NeedsReview, err.Error())
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
	status durable.Status,
	detail string,
) (*compose.ToolOutput, error) {
	s.state.WaitingStep = step
	s.pauseStatus, s.detail = status, detail
	return nil, tool.Interrupt(ctx, "save execution boundary")
}
