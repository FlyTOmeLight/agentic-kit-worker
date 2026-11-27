package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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
	fillLegacyHistory(&snapshot, run.Spec.History)
	return &snapshot, nil
}

func fillLegacyHistory(snapshot *contextbuilder.Snapshot, history []durable.Message) {
	if len(snapshot.Materials.History) > 0 {
		return
	}
	var group []contextbuilder.Message
	pending := 0
	addGroup := func() {
		if len(group) > 0 {
			snapshot.Materials.History = append(snapshot.Materials.History, contextbuilder.MessageGroup{
				Source: contextbuilder.Source{
					Kind: contextbuilder.SourceHistory,
					ID:   fmt.Sprintf("hist-%d", len(snapshot.Materials.History)),
				},
				Messages: group,
			})
			group = nil
		}
	}
	for _, msg := range history {
		m := contextbuilder.Message{
			Role:        contextbuilder.Role(msg.Role),
			Content:     msg.Content,
			ToolCallID:  msg.ToolCallID,
			Attachments: contextAttachments(msg.Attachments),
		}
		for _, call := range msg.ToolCalls {
			m.ToolCalls = append(
				m.ToolCalls,
				contextbuilder.ToolCall{ID: call.ID, Name: call.Name, Arguments: call.Arguments},
			)
		}
		if m.Role == contextbuilder.RoleUser && len(group) > 0 && pending == 0 {
			addGroup()
		}
		group = append(group, m)
		switch m.Role {
		case contextbuilder.RoleAssistant:
			pending = len(m.ToolCalls)
			if pending == 0 {
				addGroup()
			}
		case contextbuilder.RoleTool:
			if pending > 0 {
				pending--
			}
		}
	}
	addGroup()
}

func (s *execution) modelSnapshot() *contextbuilder.Snapshot {
	if s.snapshot != nil {
		return s.snapshot
	}
	spec := s.run.Spec
	budget := contextbuilder.Budget{
		MaxInputBytes: 128 * 1024, MaxToolResultBytes: 16 * 1024,
		CounterVersion: contextbuilder.CJKWeightedCounterVersion,
	}
	if spec.Model.ContextWindow > 0 && spec.Model.MaxTokens != nil {
		budget.ContextWindow = spec.Model.ContextWindow
		budget.OutputReserve = int64(*spec.Model.MaxTokens)
		budget.MaxInputBytes = 0
		budget.ProtocolOverheadTokens = 256
		budget.MaxToolResultBytes = max(128, min(budget.MaxToolResultBytes, budget.InputTokenLimit()/2))
	}
	snapshot := &contextbuilder.Snapshot{
		Materials: contextbuilder.FrozenMaterials{
			SystemInstruction: spec.SystemPrompt,
			FileIndex:         appendAttachmentFiles(nil, spec.Attachments),
		},
		Policy: contextbuilder.Policy{Version: contextbuilder.PolicyVersionV4}, Budget: budget,
	}
	fillLegacyHistory(snapshot, spec.History)
	return snapshot
}

func contextAttachments(attachments []durable.Attachment) []contextbuilder.Attachment {
	var out []contextbuilder.Attachment
	for _, att := range attachments {
		out = append(out, contextbuilder.Attachment{
			ID: att.ID, Kind: string(att.Kind), URI: att.URI, Name: att.Name, MediaType: att.MediaType,
			SizeBytes: att.SizeBytes, ArtifactURI: att.ArtifactURI,
		})
	}
	return out
}

func appendAttachmentFiles(
	files []contextbuilder.FileIndexEntry,
	attachments []durable.Attachment,
) []contextbuilder.FileIndexEntry {
	for _, att := range attachments {
		files = append(files, contextbuilder.FileIndexEntry{
			URI: att.URI, Name: att.Name, MediaType: att.MediaType, SizeBytes: att.SizeBytes,
			Source: "attachment", Action: "uploaded",
		})
	}
	return files
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
		msg := &schema.Message{Role: schema.User}
		setProjectedContent(msg, m)
		return msg
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

func setProjectedContent(msg *schema.Message, projected contextbuilder.Message) {
	msg.Content = projected.Content
	msg.UserInputMultiContent = nil
	if len(projected.Attachments) == 0 {
		return
	}

	// OpenAI represents content as either a string or an array of parts.
	msg.Content = ""
	parts := make([]schema.MessageInputPart, 0, 1+len(projected.Attachments))
	parts = append(parts, schema.MessageInputPart{Type: schema.ChatMessagePartTypeText, Text: projected.Content})
	for _, att := range projected.Attachments {
		uri := att.URI
		common := schema.MessagePartCommon{URL: &uri}
		switch att.Kind {
		case "image":
			parts = append(parts, schema.MessageInputPart{
				Type:  schema.ChatMessagePartTypeImageURL,
				Image: &schema.MessageInputImage{MessagePartCommon: common},
			})
		case "video":
			parts = append(parts, schema.MessageInputPart{
				Type:  schema.ChatMessagePartTypeVideoURL,
				Video: &schema.MessageInputVideo{MessagePartCommon: common},
			})
		}
	}
	msg.UserInputMultiContent = parts
}

// projectedTurn keeps the original Eino messages available for metadata and
// uses durable sequence numbers only to select which messages reach the model.
type projectedTurn struct {
	messages  []contextbuilder.Message
	originals map[string]*schema.Message
	human     map[string]compactedPin
}

type compactedPin struct {
	Seq   int64                      `json:"seq"`
	Block contextbuilder.PinnedBlock `json:"block"`
}

func interactionPin(row durable.SessionMessage) (compactedPin, bool) {
	switch row.Kind {
	case "ClarificationAnswer", "ReviewResult":
		return compactedPin{Seq: row.Seq, Block: contextbuilder.PinnedBlock{
			Label: row.Kind, Content: capRunes(row.Content, 4000),
		}}, true
	default:
		return compactedPin{}, false
	}
}

func capRunes(content string, limit int) string {
	for i := range content {
		if limit == 0 {
			return content[:i]
		}
		limit--
	}
	return content
}

func projectTurn(
	original []*schema.Message,
	query string,
	projection durable.ContextProjection,
	attachments []contextbuilder.Attachment,
) (projectedTurn, error) {
	out := projectedTurn{
		originals: make(map[string]*schema.Message),
		human:     make(map[string]compactedPin),
	}
	keptCalls := make(map[string]bool)
	keptResults := make(map[string]bool)
	for _, row := range projection.Messages {
		for _, call := range row.ToolCalls {
			keptCalls[call.ID] = true
		}
		if row.ToolCallID != "" {
			keptResults[row.ToolCallID] = true
			if pin, ok := interactionPin(row); ok {
				out.human[row.ToolCallID] = pin
			}
		}
	}
	names := make(map[string]string)
	for i, source := range original {
		message, err := toContextMessage(source)
		if err != nil {
			return out, err
		}
		if projection.FirstKeptSeq > 0 {
			switch {
			case i == 0 && message.Role == contextbuilder.RoleUser && message.Content == query:
				continue
			case len(message.ToolCalls) > 0 && !keptCalls[message.ToolCalls[0].ID]:
				continue
			case message.Role == contextbuilder.RoleTool && !keptResults[message.ToolCallID]:
				continue
			}
		}
		if i == 0 && source.Role == schema.User && projection.FirstKeptSeq == 0 {
			message.Attachments = attachments
		}
		for _, call := range message.ToolCalls {
			names[call.ID] = call.Name
		}
		if message.Role == contextbuilder.RoleTool {
			message.ToolName = names[message.ToolCallID]
			message.ToolFailed = toolHasError(message.Content)
			_, human := out.human[message.ToolCallID]
			message.Exempt = human || message.ToolName == "ask_for_clarification" ||
				message.ToolName == readStepResultTool
			out.originals["tool:"+message.ToolCallID] = source
		} else {
			// The builder carries ToolName through for all roles. On non-tool
			// messages it is unused, so use it to track the exact source.
			message.ToolName = "current:" + strconv.Itoa(i)
			out.originals[message.ToolName] = source
		}
		out.messages = append(out.messages, message)
	}
	if len(out.messages) == 0 {
		message := contextbuilder.Message{Role: contextbuilder.RoleUser, Content: query}
		if projection.FirstKeptSeq > 0 {
			message.Content = capRunes(query, 8000)
		}
		out.messages = append(out.messages, message)
	}
	return out, nil
}

func (turn projectedTurn) render(messages []contextbuilder.Message) []*schema.Message {
	out := make([]*schema.Message, 0, len(messages))
	for _, message := range messages {
		key := message.ToolName
		if message.Role == contextbuilder.RoleTool {
			key = "tool:" + message.ToolCallID
		}
		if source := turn.originals[key]; source != nil {
			copy := *source
			setProjectedContent(&copy, message)
			if len(message.ToolCalls) > 0 {
				copy.ToolCalls = append([]schema.ToolCall(nil), source.ToolCalls...)
				for i, call := range message.ToolCalls {
					copy.ToolCalls[i].Function.Arguments = call.Arguments
				}
			}
			out = append(out, &copy)
		} else {
			out = append(out, toSchemaMessage(message))
		}
	}
	return out
}

// Legacy Eino checkpoints may still include the old injected system/history
// prefix. New checkpoints start at the current run's user query.
func (s *execution) currentRunMessages(input []*schema.Message) []*schema.Message {
	if s.snapshot == nil && len(input) > len(s.run.Spec.History)+1 && input[0] != nil &&
		input[0].Role == schema.System {
		return input[len(s.run.Spec.History)+1:]
	}
	return input
}

func (s *execution) prepareModelInput(
	ctx context.Context, stateMessages []*schema.Message, tools []*schema.ToolInfo,
	reactiveAttempt int, targetTokens, targetBytes int64,
) ([]*schema.Message, *durable.ContextInfo, int64, error) {
	snapshot := s.modelSnapshot()
	budget := snapshot.Budget
	if budget.CounterVersion == "" {
		budget.CounterVersion = contextbuilder.CJKWeightedCounterVersion
	}
	counter, err := contextbuilder.ResolveCounter(budget.CounterVersion)
	if err != nil {
		return nil, nil, 0, err
	}
	projection := durable.ContextProjection{}
	if s.store != nil {
		projection, err = s.store.CompactionContext(ctx, s.run.Lease())
		if err != nil {
			return nil, nil, 0, err
		}
	}
	in := contextbuilder.BuildInput{
		FrozenMaterials: snapshot.Materials, Policy: snapshot.Policy, Budget: budget, Counter: counter,
		DirectModalities: s.run.Spec.Model.InputModalities, Steers: s.state.Steers,
	}
	in.FrozenMaterials.Tools = make([]contextbuilder.ToolDefinition, 0, len(tools))
	for _, tool := range tools {
		if tool == nil {
			return nil, nil, 0, errors.New("nil tool definition")
		}
		parameters, err := tool.ParamsOneOf.ToJSONSchema()
		if err != nil {
			return nil, nil, 0, err
		}
		encoded, err := json.Marshal(parameters)
		if err != nil {
			return nil, nil, 0, err
		}
		in.FrozenMaterials.Tools = append(in.FrozenMaterials.Tools, contextbuilder.ToolDefinition{
			Name: tool.Name, Description: tool.Desc, Parameters: encoded,
		})
	}

	original := s.currentRunMessages(stateMessages)
	query := s.run.Spec.Query
	attachments := contextAttachments(s.run.Spec.Attachments)
	prepareTurn := func() (projectedTurn, error) {
		turn, err := projectTurn(original, query, projection, attachments)
		if err != nil {
			return turn, err
		}
		in.CurrentTurn = turn.messages
		in.FileIndex = s.state.ArtifactFiles
		// Build indexes attachments on the original first message. When that
		// message is gone, retain its files without passing media to the model.
		if s.snapshot != nil && len(attachments) > 0 && len(turn.messages[0].Attachments) == 0 {
			files := append([]contextbuilder.FileIndexEntry(nil), in.FileIndex...)
			in.FileIndex = appendAttachmentFiles(files, s.run.Spec.Attachments)
		}
		in.CompactedSummary = projection.Summary
		in.PinnedBlocks = nil
		if projection.FirstKeptSeq > 0 && len(turn.messages) > 0 &&
			(turn.messages[0].Role != contextbuilder.RoleUser || turn.messages[0].Content != capRunes(query, 8000)) {
			in.PinnedBlocks = append(
				in.PinnedBlocks,
				contextbuilder.PinnedBlock{Label: "query", Content: capRunes(query, 8000)},
			)
		}
		for _, pin := range s.state.CompactedInteractionPins {
			if pin.Seq < projection.FirstKeptSeq {
				in.PinnedBlocks = append(in.PinnedBlocks, pin.Block)
			}
		}
		return turn, nil
	}
	turn, err := prepareTurn()
	if err != nil {
		return nil, nil, 0, err
	}

	// Measure before budget-driven reductions so earlier omission cannot
	// hide pressure from the proactive threshold or the usage anchor.
	baseline := in
	baseline.Budget.ContextWindow = 0
	baseline.Budget.OutputReserve = 0
	baseline.Budget.MaxInputTokens = 0
	baseline.Budget.MaxInputBytes = 0
	measured, err := contextbuilder.Build(baseline)
	if err != nil {
		return nil, nil, 0, err
	}
	estimated := measured.Usage.InputTokens
	usedAnchor := false
	anchored := estimated
	if anchor := s.state.UsageAnchor; anchor != nil && anchor.Turn == s.state.Turn-1 &&
		anchor.PromptTokens > 0 && estimated >= anchor.EstimatedInputTokens {
		anchored = anchor.PromptTokens + (estimated - anchor.EstimatedInputTokens)
		usedAnchor = true
	}
	tokenLimit, byteLimit := budget.InputTokenLimit(), budget.InputByteLimit()
	byteTrigger := byteLimit
	if budget.ContextWindow == 0 && s.compactionMaxBytes > 0 &&
		(byteTrigger == 0 || int64(s.compactionMaxBytes) < byteTrigger) {
		byteTrigger = int64(s.compactionMaxBytes)
	}
	triggered := (tokenLimit > 0 && max(estimated, anchored) >= max(1, tokenLimit*9/10)) ||
		(byteTrigger > 0 && measured.Usage.InputBytes >= max(1, byteTrigger*9/10))
	if reactiveAttempt == 0 && triggered {
		if tokenLimit > 0 {
			targetTokens = max(1, tokenLimit/2)
		}
		if byteTrigger > 0 {
			targetBytes = max(1, byteTrigger/2)
		}
	}

	in.TargetInputTokens, in.TargetInputBytes = targetTokens, targetBytes
	result := measured
	result.Usage.TokenLimit, result.Usage.ByteLimit = tokenLimit, byteLimit
	result.Usage.OutputReserve = budget.OutputReserve
	if targetTokens > 0 || targetBytes > 0 || reactiveAttempt > 0 ||
		(budget.ContextWindow > 0 && budget.OutputReserve >= budget.ContextWindow) {
		result, err = contextbuilder.Build(in)
	}
	if needsCompression(result, err) && s.summarizer != nil && s.store != nil &&
		s.state.SummaryConsecutiveFailures < summaryFailureLimit && reactiveAttempt <= 3 {
		var compactErr error
		projection, compactErr = s.compactContext(
			ctx,
			snapshot,
			projection,
			counter,
			reactiveAttempt,
			targetTokens,
			targetBytes,
		)
		if compactErr != nil {
			return nil, nil, 0, compactErr
		}
		turn, err = prepareTurn()
		if err != nil {
			return nil, nil, 0, err
		}
		result, err = contextbuilder.Build(in)
	}
	if needsCompression(result, err) {
		in.AllowDropCurrentRounds = true
		in.CurrentTurn = append([]contextbuilder.Message(nil), in.CurrentTurn...)
		for i := range in.CurrentTurn {
			message := &in.CurrentTurn[i]
			if pin, ok := turn.human[message.ToolCallID]; ok {
				in.PinnedBlocks = append(in.PinnedBlocks, pin.Block)
				message.Content = "[See pinned " + pin.Block.Label + "]"
			}
			if i == 0 && message.Role == contextbuilder.RoleUser && message.Content == query {
				message.Content = capRunes(query, 8000)
			}
		}
		result, err = contextbuilder.Build(in)
	}
	if err != nil {
		return nil, nil, 0, err
	}
	info := &durable.ContextInfo{
		InputTokens:          result.Usage.InputTokens,
		InputBytes:           result.Usage.InputBytes,
		TokenLimit:           result.Usage.TokenLimit,
		ByteLimit:            result.Usage.ByteLimit,
		OutputReserve:        result.Usage.OutputReserve,
		LadderPreviewed:      result.LadderPreviewed,
		LadderStubbed:        result.LadderStubbed,
		DroppedRounds:        result.DroppedRounds,
		ProjectedToolResults: result.ProjectedToolResults,
		UsedUsageAnchor:      usedAnchor,
		ReactiveAttempt:      reactiveAttempt,
		SummaryBreakerOpen:   s.state.SummaryConsecutiveFailures >= summaryFailureLimit,
	}
	source := func(src contextbuilder.Source) durable.Source {
		return durable.Source{Kind: string(src.Kind), ID: src.ID, Revision: src.Revision, Version: src.Version}
	}
	for _, omitted := range result.OmittedSources {
		info.OmittedSources = append(info.OmittedSources, source(omitted.Source))
	}
	for _, collapsed := range result.CollapsedSources {
		info.CollapsedSources = append(info.CollapsedSources, source(collapsed))
	}
	s.lastEstimatedInputTokens = result.Usage.InputTokens
	s.lastUsedUsageAnchor = usedAnchor
	s.lastReactiveAttempt = reactiveAttempt
	return turn.render(result.Messages), info, result.Usage.InputTokens, nil
}

func needsCompression(result contextbuilder.BuildResult, err error) bool {
	return (err == nil && !result.TargetMet) || errors.Is(err, contextbuilder.ErrBudgetExceeded)
}
