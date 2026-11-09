package engine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/schema"
)

func decodeSnapshot(run *durable.Run) (*contextbuilder.Snapshot, error) {
	if len(run.ContextSnapshot.Payload) == 0 {
		return nil, nil
	}
	snapshot, err := contextbuilder.DecodeSnapshot(contextbuilder.Envelope{
		Format:        run.ContextSnapshot.Format,
		PolicyVersion: run.ContextSnapshot.PolicyVersion,
		Digest:        run.ContextSnapshot.Digest,
		Payload:       run.ContextSnapshot.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("decode context snapshot: %w", err)
	}
	if snapshot.Materials.SystemInstruction == "" && run.Spec.SystemPrompt != "" {
		snapshot.Materials.SystemInstruction = run.Spec.SystemPrompt
	}
	if len(snapshot.Materials.History) == 0 && len(run.Spec.History) > 0 {
		var groups []contextbuilder.MessageGroup
		var currentGroup []contextbuilder.Message
		for _, msg := range run.Spec.History {
			currentGroup = append(currentGroup, contextbuilder.Message{
				Role:    contextbuilder.Role(msg.Role),
				Content: msg.Content,
			})
			if msg.Role == "assistant" {
				groups = append(groups, contextbuilder.MessageGroup{
					Source: contextbuilder.Source{
						Kind: contextbuilder.SourceHistory,
						ID:   fmt.Sprintf("hist-%d", len(groups)),
					},
					Messages: currentGroup,
				})
				currentGroup = nil
			}
		}
		if len(currentGroup) > 0 {
			groups = append(groups, contextbuilder.MessageGroup{
				Source: contextbuilder.Source{
					Kind: contextbuilder.SourceHistory,
					ID:   fmt.Sprintf("hist-%d", len(groups)),
				},
				Messages: currentGroup,
			})
		}
		snapshot.Materials.History = groups
	}
	return &snapshot, nil
}

func toContextMessage(m *schema.Message) (contextbuilder.Message, error) {
	if m == nil {
		return contextbuilder.Message{}, errors.New("nil message")
	}
	msg := contextbuilder.Message{
		Content: m.Content,
	}
	switch m.Role {
	case schema.System:
		msg.Role = contextbuilder.RoleSystem
	case schema.User:
		msg.Role = contextbuilder.RoleUser
	case schema.Assistant:
		msg.Role = contextbuilder.RoleAssistant
		for _, call := range m.ToolCalls {
			msg.ToolCalls = append(msg.ToolCalls, contextbuilder.ToolCall{
				ID:        call.ID,
				Name:      call.Function.Name,
				Arguments: call.Function.Arguments,
			})
		}
	case schema.Tool:
		msg.Role = contextbuilder.RoleTool
		msg.ToolCallID = m.ToolCallID
	default:
		return contextbuilder.Message{}, fmt.Errorf("unsupported message role: %s", m.Role)
	}
	return msg, nil
}

func toSchemaMessage(m contextbuilder.Message) *schema.Message {
	switch m.Role {
	case contextbuilder.RoleSystem:
		return schema.SystemMessage(m.Content)
	case contextbuilder.RoleUser:
		return schema.UserMessage(m.Content)
	case contextbuilder.RoleAssistant:
		var calls []schema.ToolCall
		for _, call := range m.ToolCalls {
			calls = append(calls, schema.ToolCall{
				ID:   call.ID,
				Type: "function",
				Function: schema.FunctionCall{
					Name:      call.Name,
					Arguments: call.Arguments,
				},
			})
		}
		return &schema.Message{
			Role:      schema.Assistant,
			Content:   m.Content,
			ToolCalls: calls,
		}
	case contextbuilder.RoleTool:
		return &schema.Message{
			Role:       schema.Tool,
			Content:    m.Content,
			ToolCallID: m.ToolCallID,
		}
	default:
		return &schema.Message{
			Role:    schema.RoleType(m.Role),
			Content: m.Content,
		}
	}
}

func toSchemaMessages(messages []contextbuilder.Message) []*schema.Message {
	result := make([]*schema.Message, len(messages))
	for i, m := range messages {
		result[i] = toSchemaMessage(m)
	}
	return result
}

// buildContext selects materials and verifies input limits for the current
// turn. When over budget, it omits low-priority memory and oldest history
// turns. If required inputs exceed the budget, it returns a BudgetExceededError.
func (s *execution) buildContext(
	stateMessages []*schema.Message,
	toolInfos []*schema.ToolInfo,
) ([]*schema.Message, *durable.ContextInfo, error) {
	if s.snapshot == nil {
		return stateMessages, nil, nil
	}

	// Locate the current turn in stateMessages. The current turn starts with the
	// last User message (or the first User message if only one exists).
	userIndex := -1
	for i := len(stateMessages) - 1; i >= 0; i-- {
		if stateMessages[i] != nil && stateMessages[i].Role == schema.User {
			userIndex = i
			break
		}
	}

	var currentTurn []contextbuilder.Message
	if userIndex >= 0 {
		for _, m := range stateMessages[userIndex:] {
			cmsg, err := toContextMessage(m)
			if err != nil {
				return nil, nil, err
			}
			currentTurn = append(currentTurn, cmsg)
		}
	} else {
		// Fallback for initial run before user message entered state
		currentTurn = []contextbuilder.Message{{
			Role:    contextbuilder.RoleUser,
			Content: s.run.Spec.Query,
		}}
	}

	materials := s.snapshot.Materials
	materials.Tools = make([]contextbuilder.ToolDefinition, 0, len(toolInfos))
	for _, info := range toolInfos {
		if info == nil {
			return nil, nil, errors.New("nil tool definition")
		}
		parameters, err := info.ParamsOneOf.ToJSONSchema()
		if err != nil {
			return nil, nil, err
		}
		encoded, err := json.Marshal(parameters)
		if err != nil {
			return nil, nil, err
		}
		materials.Tools = append(materials.Tools, contextbuilder.ToolDefinition{
			Name: info.Name, Description: info.Desc, Parameters: encoded,
		})
	}
	counter, err := contextbuilder.ResolveCounter(s.snapshot.Budget.CounterVersion)
	if err != nil {
		return nil, nil, err
	}
	result, err := contextbuilder.Build(contextbuilder.BuildInput{
		FrozenMaterials: materials,
		CurrentTurn:     currentTurn,
		Policy:          s.snapshot.Policy,
		Budget:          s.snapshot.Budget,
		Counter:         counter,
	})
	if err != nil {
		return nil, nil, err
	}

	// Map contextbuilder sources to the execution-layer ContextInfo type.
	var omitted []durable.Source
	for _, o := range result.OmittedSources {
		omitted = append(omitted, durable.Source{
			Kind:     string(o.Source.Kind),
			ID:       o.Source.ID,
			Revision: o.Source.Revision,
			Version:  o.Source.Version,
		})
	}
	var collapsed []durable.Source
	for _, c := range result.CollapsedSources {
		collapsed = append(collapsed, durable.Source{
			Kind:     string(c.Kind),
			ID:       c.ID,
			Revision: c.Revision,
			Version:  c.Version,
		})
	}
	info := &durable.ContextInfo{
		InputTokens:          result.Usage.InputTokens,
		InputBytes:           result.Usage.InputBytes,
		TokenLimit:           result.Usage.TokenLimit,
		ByteLimit:            result.Usage.ByteLimit,
		OutputReserve:        result.Usage.OutputReserve,
		OmittedSources:       omitted,
		CollapsedSources:     collapsed,
		ProjectedToolResults: result.ProjectedToolResults,
	}
	return toSchemaMessages(result.Messages), info, nil
}
