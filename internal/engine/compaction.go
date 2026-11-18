package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/schema"
)

type SummaryInput struct {
	Spec            durable.Spec
	PreviousSummary string
	Messages        []durable.Message
	RecordUsage     func(context.Context, *schema.TokenUsage, int64) error
}

var ErrModelUsageUnavailable = errors.New("model token usage unavailable")

type Summarizer interface {
	Summarize(context.Context, SummaryInput) (string, error)
}

func (s *execution) projectContext(ctx context.Context, original []*schema.Message) ([]*schema.Message, error) {
	if err := s.checkPricing(ctx); err != nil {
		return nil, err
	}
	projection, err := s.store.CompactionContext(ctx, s.run.Lease())
	if err != nil {
		return nil, err
	}
	var messages []durable.SessionMessage
	var originals []*schema.Message
	var batchStarts []int
	history := s.run.Spec.History
	baseCount := len(s.run.Spec.History) + 2
	if s.snapshot != nil {
		baseCount = 1
		history = nil
		for _, group := range s.snapshot.Materials.History {
			for _, message := range group.Messages {
				saved := durable.Message{
					Role:       string(message.Role),
					Content:    message.Content,
					ToolCallID: message.ToolCallID,
				}
				for _, call := range message.ToolCalls {
					saved.ToolCalls = append(
						saved.ToolCalls,
						durable.ToolCall{
							ID:        call.ID,
							Name:      call.Name,
							Arguments: call.Arguments,
						},
					)
				}
				history = append(history, saved)
			}
		}
	}
	if len(original) < baseCount || original[baseCount-1] == nil ||
		original[baseCount-1].Role != schema.User || original[baseCount-1].Content != s.run.Spec.Query ||
		(s.snapshot == nil && (original[0] == nil || original[0].Role != schema.System)) {
		return nil, errors.New("model input does not match frozen conversation")
	}
	assistants, results := map[string]*schema.Message{}, map[string]*schema.Message{}
	for _, message := range original[baseCount:] {
		if message == nil {
			return nil, errors.New("nil message in model input")
		}
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
	canCompactHistory := projection.Summary == "" && len(history) > 0
	if len(batchStarts) <= 2 && !canCompactHistory {
		return input, nil
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if s.snapshot != nil && canCompactHistory {
		historyData, historyErr := json.Marshal(history)
		if historyErr != nil {
			return nil, historyErr
		}
		data = append(data, historyData...)
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
		request.Messages = append(request.Messages, history...)
	}
	for _, message := range messages[:cut] {
		request.Messages = append(request.Messages, message.Message)
	}
	if err = s.store.CheckUsageBudget(ctx, s.run.Lease()); err != nil {
		return nil, err
	}
	s.state.InflightModel = &InflightModel{Kind: "summary", Sequence: s.state.SummaryCalls}
	if err = s.store.SaveState(ctx, s.run.Lease(), s.encode()); err != nil {
		s.state.InflightModel = nil
		return nil, err
	}
	var usageErr error
	request.RecordUsage = func(usageCtx context.Context, usage *schema.TokenUsage, tokens int64) error {
		if usage == nil {
			usageErr = ErrModelUsageUnavailable
			return usageErr
		}
		cost, costErr := s.usageCost(usage)
		previousState := s.state
		if costErr != nil {
			s.state.PricingFailure = costErr.Error()
		}
		call := s.state.SummaryCalls
		s.state.SummaryCalls++
		s.state.InflightModel = nil
		usageErr = s.store.RecordUsageWithCost(
			usageCtx,
			s.run.Lease(),
			call,
			"summary",
			tokens,
			cost,
			s.encode(),
		)
		if usageErr != nil {
			s.state = previousState
			return usageErr
		}
		usageErr = costErr
		return usageErr
	}
	summaryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	summary, summaryErr := s.summarizer.Summarize(summaryCtx, request)
	cancel()
	if usageErr != nil {
		return nil, usageErr
	}
	if errors.Is(summaryErr, durable.ErrUsageBudgetExceeded) || errors.Is(summaryErr, ErrModelUsageUnavailable) {
		return nil, summaryErr
	}
	if s.state.InflightModel != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Join(ErrModelUsageUnavailable, summaryErr)
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
	persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer persistCancel()
	if err = s.store.CompactRun(persistCtx, s.run.Lease(), summary, firstKeptSeq); err != nil {
		return nil, err
	}
	return s.contextInput(summary, originals[cut:], original), nil
}

func (s *execution) contextInput(summary string, messages, original []*schema.Message) []*schema.Message {
	if s.snapshot != nil {
		s.run.CompactedSummary = summary
		return append([]*schema.Message{original[0]}, messages...)
	}
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
