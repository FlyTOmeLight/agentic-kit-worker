package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/schema"
)

const summaryFailureLimit = 3

type SummaryInput struct {
	Spec            durable.Spec
	PreviousSummary string
	Messages        []durable.Message
	MaxOutputTokens int
	RecordUsage     func(context.Context, *schema.TokenUsage, int64) error
}

var ErrModelUsageUnavailable = errors.New("model token usage unavailable")

type Summarizer interface {
	Summarize(context.Context, SummaryInput) (string, error)
}

func (s *execution) compactContext(
	ctx context.Context, snapshot *contextbuilder.Snapshot, projection durable.ContextProjection,
	counter contextbuilder.Counter, reactiveAttempt int, targetTokens, targetBytes int64,
) (durable.ContextProjection, error) {
	rows := projection.Messages
	if len(rows) == 0 {
		return projection, nil
	}
	summary := projection.Summary
	var head []durable.Message
	if summary == "" {
		summary = snapshot.Materials.BaseSummary
		for _, group := range snapshot.Materials.History {
			for _, m := range group.Messages {
				msg := durable.Message{
					Role:       string(m.Role),
					Content:    m.Content,
					ToolCallID: m.ToolCallID,
				}
				if len(m.Attachments) > 0 {
					if msg.Content != "" {
						msg.Content += "\n\n"
					}
					msg.Content += contextbuilder.AttachmentSummaryText(m.Attachments)
				}
				for _, call := range m.ToolCalls {
					msg.ToolCalls = append(
						msg.ToolCalls,
						durable.ToolCall{
							ID:        call.ID,
							Name:      call.Name,
							Arguments: call.Arguments,
						},
					)
				}
				head = append(head, msg)
			}
		}
	}

	cut := compactionCut(rows, counter, reactiveAttempt, targetTokens, targetBytes)
	firstKept := rows[len(rows)-1].Seq + 1
	if cut < len(rows) {
		firstKept = rows[cut].Seq
	}
	var pins []compactedPin
	var latestSkillPin *compactedSkillPin
	toolNames := make(map[string]string)
	for _, row := range rows[:cut] {
		for _, call := range row.ToolCalls {
			toolNames[call.ID] = call.Name
		}
		if pin, ok := interactionPin(row); ok {
			pins = append(pins, pin)
		}
		if row.Role == "tool" && toolNames[row.ToolCallID] == loadSkillToolName {
			if skillPin, ok := buildSkillPin(row.Seq, row.Content); ok {
				latestSkillPin = skillPin
			}
		}
		if row.Kind != "User" {
			head = append(head, row.Message)
		} else if len(row.Attachments) > 0 {
			head = append(head, durable.Message{
				Role:    row.Role,
				Content: contextbuilder.AttachmentSummaryText(contextAttachments(row.Attachments)),
			})
		}
	}
	if len(head) == 0 && (summary == "" || projection.Summary != "") {
		return projection, nil
	}
	summary, err := s.summarizeChunks(ctx, snapshot.Budget, counter, summary, head)
	if err != nil {
		if ctx.Err() != nil {
			return projection, ctx.Err()
		}
		if s.state.InflightModel != nil || errors.Is(err, durable.ErrUsageBudgetExceeded) ||
			errors.Is(err, ErrModelPricingUnavailable) || errors.Is(err, durable.ErrLeaseLost) {
			return projection, err
		}
		var soft *summaryFailure
		if !errors.As(err, &soft) {
			return projection, err
		}
		s.state.SummaryConsecutiveFailures++
		if err := s.store.SaveState(ctx, s.run.Lease(), s.encode()); err != nil {
			return projection, err
		}
		return projection, s.store.RecordCompactionFailure(ctx, s.run.Lease(), soft.Error())
	}
	s.state.SummaryConsecutiveFailures = 0
	// Save pins before advancing the boundary. A worker lost between these
	// writes will render the original rows and ignore pins at or after the cut.
	for _, pin := range pins {
		alreadySaved := false
		for _, saved := range s.state.CompactedInteractionPins {
			if saved.Seq == pin.Seq {
				alreadySaved = true
				break
			}
		}
		if !alreadySaved {
			s.state.CompactedInteractionPins = append(s.state.CompactedInteractionPins, pin)
		}
	}
	if latestSkillPin != nil &&
		(s.state.CompactedSkillPin == nil || latestSkillPin.Seq > s.state.CompactedSkillPin.Seq) {
		s.state.CompactedSkillPin = latestSkillPin
	}
	persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer persistCancel()
	if err := s.store.SaveState(persistCtx, s.run.Lease(), s.encode()); err != nil {
		return projection, err
	}
	if err := s.store.CompactRun(persistCtx, s.run.Lease(), summary, firstKept); err != nil {
		return projection, err
	}
	s.run.CompactedSummary, s.run.FirstKeptSeq = summary, firstKept
	return durable.ContextProjection{Summary: summary, FirstKeptSeq: firstKept, Messages: rows[cut:]}, nil
}

// Cut only at assistant boundaries with no pending tool results. Always retain
// the newest tool batch. The first query is carried separately as a pinned block.
func compactionCut(
	rows []durable.SessionMessage,
	counter contextbuilder.Counter,
	attempt int,
	tokens, bytes int64,
) int {
	var starts []int
	pending := 0
	for i, row := range rows {
		switch row.Role {
		case "assistant":
			if pending == 0 {
				starts = append(starts, i)
			}
			pending += len(row.ToolCalls)
		case "tool":
			pending--
		}
	}
	if len(starts) == 0 {
		return len(rows)
	}
	divisor := int64(2)
	if attempt >= 2 {
		divisor = 1 << attempt
	}
	budget := summaryMeasure{tokens: tokens, bytes: bytes}.divide(divisor)
	var size summaryMeasure
	cut := starts[len(starts)-1]
	end := len(rows)
	for i := len(starts) - 1; i >= 0; i-- {
		start := starts[i]
		for _, row := range rows[start:end] {
			data, _ := json.Marshal(row.Message)
			size.add(measureSummary(counter, string(data)))
		}
		if !size.fits(budget) {
			break
		}
		cut, end = start, start
	}
	return cut
}

type summaryMeasure struct {
	tokens int64
	bytes  int64
}

func measureSummary(counter contextbuilder.Counter, text string) summaryMeasure {
	return summaryMeasure{tokens: counter.Count(text), bytes: int64(len(text))}
}

func (m *summaryMeasure) add(n summaryMeasure) {
	m.tokens += n.tokens
	m.bytes += n.bytes
}

func (m summaryMeasure) fits(limit summaryMeasure) bool {
	return (limit.tokens == 0 || m.tokens <= limit.tokens) && (limit.bytes == 0 || m.bytes <= limit.bytes)
}

func (m summaryMeasure) divide(n int64) summaryMeasure {
	if m.tokens > 0 {
		m.tokens = max(1, m.tokens/n)
	}
	if m.bytes > 0 {
		m.bytes = max(1, m.bytes/n)
	}
	return m
}

type summaryFailure struct{ error }

func (s *execution) summarizeChunks(
	ctx context.Context, budget contextbuilder.Budget, counter contextbuilder.Counter,
	previous string, messages []durable.Message,
) (string, error) {
	limit := summaryMeasure{tokens: budget.InputTokenLimit(), bytes: budget.InputByteLimit()}
	if limit.tokens == 0 && limit.bytes == 0 {
		limit.bytes = 128 * 1024
	}
	maxOutput := 2048
	if budget.OutputReserve > 0 {
		maxOutput = int(min(int64(maxOutput), budget.OutputReserve))
	}
	request := SummaryInput{Spec: s.run.Spec, MaxOutputTokens: maxOutput}
	request.Spec.Query = capRunes(request.Spec.Query, 8000)
	records := make([]summaryRecord, len(messages))
	for i, message := range messages {
		records[i].message = message
	}
	// Leave one fifth of the window for the summary instructions and
	// message framing, with a minimum allowance for the fixed prompt.
	chunkLimit := limit
	if chunkLimit.tokens > 0 {
		chunkLimit.tokens -= max(256, chunkLimit.tokens/5)
	}
	if chunkLimit.bytes > 0 {
		chunkLimit.bytes -= max(1024, chunkLimit.bytes/5)
	}
	if (limit.tokens > 0 && chunkLimit.tokens <= 0) || (limit.bytes > 0 && chunkLimit.bytes <= 0) {
		return "", &summaryFailure{
			error: errors.New("summary input window cannot fit the summary instructions"),
		}
	}
	request.PreviousSummary = previous
	if len(records) == 0 {
		// A frozen base summary can itself need compression.
		if _, _, err := summaryChunk(counter, chunkLimit, request, nil); err != nil {
			return "", &summaryFailure{error: err}
		}
		result, _, err := s.summarizeChunk(ctx, request, 1)
		return result, err
	}
	attempt := 1
	for len(records) > 0 {
		chunk, rest, err := summaryChunk(counter, chunkLimit, request, records)
		if err != nil {
			return "", &summaryFailure{error: err}
		}
		request.Messages = chunk
		next, rejected, err := s.summarizeChunk(ctx, request, attempt)
		if rejected && attempt < 3 {
			attempt++
			chunkLimit = chunkLimit.divide(2)
			continue
		}
		if err != nil {
			return "", err
		}
		request.PreviousSummary, records = next, rest
		attempt = 1
	}
	return request.PreviousSummary, nil
}

type summaryRecord struct {
	message  durable.Message
	fragment string
}

func summaryChunk(
	counter contextbuilder.Counter, limit summaryMeasure, request SummaryInput, records []summaryRecord,
) (chunk []durable.Message, rest []summaryRecord, err error) {
	base, _ := json.Marshal(struct {
		Task            string `json:"task"`
		PreviousSummary string `json:"previousSummary"`
		Messages        []int  `json:"messages"`
	}{Task: request.Spec.Query, PreviousSummary: request.PreviousSummary, Messages: []int{}})
	size := measureSummary(counter, string(base))
	if !size.fits(limit) {
		return nil, nil, errors.New("task and previous summary exceed the summary input window")
	}
	for i, record := range records {
		message := record.message
		data, _ := json.Marshal(message)
		size.add(measureSummary(counter, string(data)+","))
		if !size.fits(limit) {
			if i > 0 {
				return chunk, records[i:], nil
			}
			fragment := record.fragment
			if fragment == "" {
				fragment = string(data)
			}
			part := durable.Message{Role: message.Role, ToolCallID: message.ToolCallID}
			head, tail, err := splitSummaryMessage(counter, limit, base, part, fragment)
			if err != nil {
				return nil, nil, err
			}
			rest := records[1:]
			if tail != "" {
				part.Content = summaryFragmentPrefix + tail
				rest = append([]summaryRecord{{message: part, fragment: tail}}, rest...)
			}
			return []durable.Message{head}, rest, nil
		}
		chunk = append(chunk, message)
	}
	return chunk, nil, nil
}

const summaryFragmentPrefix = "[Stored message fragment]\n"

func splitSummaryMessage(
	counter contextbuilder.Counter, limit summaryMeasure, base []byte,
	part durable.Message, data string,
) (durable.Message, string, error) {
	// Carry all data from oversized records across successive chunks.
	// Fragments are evidence, not provider tool calls.
	size := measureSummary(counter, string(base))
	end := sort.Search(len(data)+1, func(n int) bool {
		part.Content = summaryFragmentPrefix + data[:n]
		encoded, _ := json.Marshal(part)
		combined := size
		combined.add(measureSummary(counter, string(encoded)+","))
		return !combined.fits(limit)
	}) - 1
	for end > 0 && end < len(data) && !utf8.RuneStart(data[end]) {
		end--
	}
	if end <= 0 {
		return durable.Message{}, "", errors.New("summary input has no room for stored message fragments")
	}
	part.Content = summaryFragmentPrefix + data[:end]
	return part, data[end:], nil
}

func (s *execution) summarizeChunk(
	ctx context.Context,
	request SummaryInput,
	overflowAttempt int,
) (string, bool, error) {
	if err := s.checkPricing(ctx); err != nil {
		return "", false, err
	}
	if err := s.store.CheckUsageBudget(ctx, s.run.Lease()); err != nil {
		return "", false, err
	}
	s.state.InflightModel = &InflightModel{Kind: "summary", Sequence: s.state.SummaryCalls}
	if err := s.store.SaveState(ctx, s.run.Lease(), s.encode()); err != nil {
		s.state.InflightModel = nil
		return "", false, err
	}
	var usageErr error
	request.RecordUsage = func(usageCtx context.Context, usage *schema.TokenUsage, tokens int64) error {
		if usage == nil || s.state.InflightModel == nil {
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
	summaryCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	summary, err := s.summarizer.Summarize(summaryCtx, request)
	cancel()
	if usageErr != nil {
		return "", false, usageErr
	}
	if s.state.InflightModel != nil {
		if ctx.Err() != nil {
			return "", false, ctx.Err()
		}
		if !isContextOverflowError(err) {
			return "", false, errors.Join(ErrModelUsageUnavailable, err)
		}
		marker := s.state.InflightModel
		s.state.InflightModel = nil
		if saveErr := s.store.RecordModelOverflow(
			ctx,
			s.run.Lease(),
			overflowAttempt,
			err.Error(),
			s.encode(),
		); saveErr != nil {
			s.state.InflightModel = marker
			return "", false, saveErr
		}
		return "", true, &summaryFailure{error: err}
	}
	if errors.Is(err, durable.ErrUsageBudgetExceeded) || errors.Is(err, ErrModelPricingUnavailable) ||
		errors.Is(err, durable.ErrLeaseLost) {
		return "", false, err
	}
	if err == nil && strings.TrimSpace(summary) == "" {
		err = errors.New("summarizer returned an empty summary")
	}
	if err != nil {
		return "", false, &summaryFailure{error: err}
	}
	return summary, false, nil
}
