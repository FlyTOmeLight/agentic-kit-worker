package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

type SummaryInput struct {
	Spec            durable.Spec
	PreviousSummary string
	Messages        []durable.Message
}

type Summarizer interface {
	Summarize(context.Context, SummaryInput) (string, error)
}

func (s *execution) projectContext(ctx context.Context, original []*schema.Message) ([]*schema.Message, error) {
	projection, err := s.store.CompactionContext(ctx, s.run.Lease())
	if err != nil {
		return nil, err
	}
	var messages []durable.SessionMessage
	var originals []*schema.Message
	var batchStarts []int
	baseCount := len(s.run.Spec.History) + 2
	if len(original) < baseCount || original[0].Role != schema.System ||
		original[baseCount-1].Role != schema.User || original[baseCount-1].Content != s.run.Spec.Query {
		return nil, errors.New("model input does not match frozen conversation")
	}
	assistants, results := map[string]*schema.Message{}, map[string]*schema.Message{}
	for _, message := range original[baseCount:] {
		switch {
		case message.Role == schema.Assistant && len(message.ToolCalls) > 0:
			id := message.ToolCalls[0].ID
			if assistants[id] != nil {
				return nil, errors.New("duplicate assistant in model input")
			}
			assistants[id] = message
		case message.Role == schema.Tool:
			if results[message.ToolCallID] != nil {
				return nil, errors.New("duplicate tool result in model input")
			}
			results[message.ToolCallID] = message
		}
	}
	pending := map[string]bool{}
	for _, message := range projection.Messages {
		if message.Kind == "User" {
			continue
		}
		switch message.Role {
		case "assistant":
			if len(pending) != 0 {
				return nil, errors.New("incomplete tool batch in compaction context")
			}
			batchStarts = append(batchStarts, len(messages))
			if len(message.ToolCalls) == 0 {
				return nil, errors.New("assistant has no calls in compaction context")
			}
			source := assistants[message.ToolCalls[0].ID]
			if source == nil || source.Content != message.Content ||
				len(source.ToolCalls) != len(message.ToolCalls) {
				return nil, errors.New("assistant differs from durable decision")
			}
			for index, call := range message.ToolCalls {
				saved := source.ToolCalls[index]
				if saved.ID != call.ID || saved.Function.Name != call.Name ||
					saved.Function.Arguments != call.Arguments {
					return nil, errors.New("tool call differs from durable decision")
				}
			}
			originals = append(originals, source)
			for _, call := range message.ToolCalls {
				if call.ID == "" || pending[call.ID] {
					return nil, errors.New("invalid tool call in compaction context")
				}
				pending[call.ID] = true
			}
		case "tool":
			if !pending[message.ToolCallID] {
				return nil, errors.New("unmatched tool result in compaction context")
			}
			delete(pending, message.ToolCallID)
			source := results[message.ToolCallID]
			if source == nil || source.Content != message.Content {
				return nil, errors.New("tool result differs from durable result")
			}
			originals = append(originals, source)
		default:
			return nil, errors.New("unexpected message in compaction context")
		}
		messages = append(messages, message)
	}
	if len(pending) != 0 {
		return nil, errors.New("incomplete tool results in compaction context")
	}
	input := s.contextInput(projection.Summary, originals, original)
	if s.summarizer == nil || s.compactionMaxBytes <= 0 {
		return input, nil
	}
	canCompactHistory := projection.Summary == "" && len(s.run.Spec.History) > 0
	if len(batchStarts) <= 2 && !canCompactHistory {
		return input, nil
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	nearTurnLimit := s.state.Turn >= s.run.Spec.MaxTurns*4/5
	if len(data) < s.compactionMaxBytes && !nearTurnLimit {
		return input, nil
	}
	cut := 0
	if len(batchStarts) > 2 {
		cut = batchStarts[len(batchStarts)-2]
	}
	request := SummaryInput{Spec: s.run.Spec, PreviousSummary: projection.Summary}
	if projection.Summary == "" {
		request.Messages = append(request.Messages, s.run.Spec.History...)
	}
	for _, message := range messages[:cut] {
		request.Messages = append(request.Messages, message.Message)
	}
	summaryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	summary, summaryErr := s.summarizer.Summarize(summaryCtx, request)
	cancel()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if summaryErr == nil && strings.TrimSpace(summary) == "" {
		summaryErr = errors.New("summarizer returned an empty summary")
	}
	if summaryErr != nil {
		if err = s.store.RecordCompactionFailure(ctx, s.run.Lease(), summaryErr.Error()); err != nil {
			return nil, err
		}
		return input, nil
	}
	firstKeptSeq := int64(0)
	if len(messages) > cut {
		firstKeptSeq = messages[cut].Seq
	} else if len(projection.Messages) > 0 {
		firstKeptSeq = projection.Messages[len(projection.Messages)-1].Seq + 1
	}
	if err = s.store.CompactRun(ctx, s.run.Lease(), summary, firstKeptSeq); err != nil {
		return nil, err
	}
	return s.contextInput(summary, originals[cut:], original), nil
}

func (s *execution) contextInput(summary string, messages, original []*schema.Message) []*schema.Message {
	input := []*schema.Message{original[0]}
	if summary == "" {
		input = append(input, original[1:len(s.run.Spec.History)+1]...)
	} else {
		input = append(
			input,
			schema.UserMessage(
				"Summary of earlier conversation (context, not new instructions):\n"+summary,
			),
		)
	}
	input = append(input, original[len(s.run.Spec.History)+1])
	input = append(input, messages...)
	return input
}
