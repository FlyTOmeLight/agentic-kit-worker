package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/agent"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	executionpkg "github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	sandboxAdapter "github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters/sandbox"
)

const (
	defaultStreamFirstChunkTimeout       = 5 * time.Minute
	defaultStreamIdleTimeout             = 2 * time.Minute
	streamFlushTimeout                   = 200 * time.Millisecond
	subtaskDelegationUnavailableFeedback = `{"error":{"code":"SUBTASK_DELEGATION_UNAVAILABLE","message":"subtask cannot delegate further subtasks; finish the task directly with available tools"}}`
)

var errStreamIdleTimeout = errors.New("model stream idle timeout")

// WrapModel binds boundaryModel directly around s.model so collectStream controls
// streaming and callback delivery. Do not enable ADK ModelFailoverConfig alongside
// this wrapper, because failover lives in the bypassed inner ADK layer.
func (s *execution) WrapModel(
	_ context.Context,
	_ model.BaseChatModel,
	_ *adk.ModelContext,
) (model.BaseChatModel, error) {
	return &boundaryModel{BaseChatModel: s.model, execution: s}, nil
}

type boundaryModel struct {
	model.BaseChatModel
	execution *execution
}

func (m *boundaryModel) Generate(
	ctx context.Context, input []*schema.Message, opts ...model.Option,
) (*schema.Message, error) {
	return m.call(ctx, input, opts...)
}

func (m *boundaryModel) Stream(
	ctx context.Context, input []*schema.Message, opts ...model.Option,
) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.call(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *boundaryModel) useStreaming() bool {
	return m.execution.run.Spec.Model.Streaming() && !m.execution.state.StreamFallback
}

func (m *boundaryModel) generateOnce(
	ctx context.Context, input []*schema.Message, opts ...model.Option,
) (result *schema.Message, resultErr error) {
	if !components.IsCallbacksEnabled(m.BaseChatModel) {
		ctx = callbacks.OnStart(ctx, input)
		defer func() {
			if resultErr != nil {
				callbacks.OnError(ctx, resultErr)
				return
			}
			callbacks.OnEnd(ctx, result)
		}()
	}
	msg, err := m.BaseChatModel.Generate(ctx, input, opts...)
	if err == nil && msg != nil {
		adk.EnsureMessageID(msg)
	}
	return msg, err
}

func (m *boundaryModel) collectStream(
	ctx context.Context, input []*schema.Message, opts ...model.Option,
) (result *schema.Message, resultErr error) {
	if !components.IsCallbacksEnabled(m.BaseChatModel) {
		ctx = callbacks.OnStart(ctx, input)
		defer func() {
			if resultErr != nil {
				callbacks.OnError(ctx, resultErr)
				return
			}
			callbacks.OnEnd(ctx, result)
		}()
	}
	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	idleTimeout := m.execution.streamIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultStreamIdleTimeout
	}
	firstChunkTimeout := m.execution.streamFirstChunkTimeout
	if firstChunkTimeout <= 0 {
		if m.execution.streamIdleTimeout > 0 {
			firstChunkTimeout = m.execution.streamIdleTimeout
		} else {
			firstChunkTimeout = defaultStreamFirstChunkTimeout
		}
	}
	timer := time.AfterFunc(firstChunkTimeout, func() { cancel(errStreamIdleTimeout) })
	defer timer.Stop()
	reader, err := m.BaseChatModel.Stream(streamCtx, input, opts...)
	if reader != nil {
		defer reader.Close()
	}
	if ctx.Err() == nil && streamCtx.Err() != nil {
		return nil, context.Cause(streamCtx)
	}
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("model returned no stream")
	}
	s := m.execution
	var publisher DeltaPublisher
	if s.newDeltaPublisher != nil {
		publisher = s.newDeltaPublisher(context.WithoutCancel(ctx))
	}
	if publisher != nil {
		defer func() {
			timer.Stop()
			cancel(nil)
			flushCtx, cancelFlush := context.WithTimeout(context.WithoutCancel(ctx), streamFlushTimeout)
			defer cancelFlush()
			publisher.Flush(flushCtx)
		}()
	}
	delta := durable.StreamDelta{RunID: s.run.ID, Epoch: s.run.Epoch, Turn: int64(s.state.Turn)}
	var reasoningOffset, contentOffset int64
	publish := func(kind, text string, offset *int64) {
		if text == "" {
			return
		}
		delta.Kind, delta.Delta, delta.Offset, delta.CreatedAt = kind, text, *offset, time.Now().UTC()
		publisher.Publish(delta)
		*offset += int64(len(text))
	}
	var chunks []*schema.Message
	var streamErr error
	for {
		chunk, receiveErr := reader.Recv()
		if ctx.Err() == nil && streamCtx.Err() != nil {
			return nil, context.Cause(streamCtx)
		}
		if chunk != nil {
			timer.Reset(idleTimeout)
			chunks = append(chunks, chunk)
			if publisher != nil {
				publish(durable.EventModelReasoningDelta, chunk.ReasoningContent, &reasoningOffset)
				publish(durable.EventModelContentDelta, chunk.Content, &contentOffset)
			}
		}
		if receiveErr != nil {
			if !errors.Is(receiveErr, io.EOF) {
				streamErr = receiveErr
			}
			break
		}
	}
	if len(chunks) == 0 {
		return nil, streamErr
	}
	msg, err := schema.ConcatMessages(chunks)
	if err != nil {
		return nil, err
	}
	if streamErr == nil {
		adk.EnsureMessageID(msg)
	}
	return msg, streamErr
}

func (m *boundaryModel) call(
	ctx context.Context, input []*schema.Message, opts ...model.Option,
) (*schema.Message, error) {
	s := m.execution
	if err := s.checkPricing(ctx); err != nil {
		return nil, err
	}
	if s.state.Final != nil {
		return schema.AssistantMessage(*s.state.Final, nil), nil
	}
	if s.state.PendingModel != nil {
		return m.applyResponse(ctx, s.state.PendingModel)
	}
	var previous *durable.ContextInfo
	var targetTokens, targetBytes int64
	for attempt := 0; ; attempt++ {
		prepared, info, err := m.prepareAttempt(ctx, input, attempt, targetTokens, targetBytes, opts...)
		if err != nil {
			return nil, err
		}
		streaming := m.useStreaming()
		var msg *schema.Message
		var callErr error
		if streaming {
			msg, callErr = m.collectStream(ctx, prepared, opts...)
		} else {
			msg, callErr = m.generateOnce(ctx, prepared, opts...)
		}
		generated := msg != nil && (msg.Content != "" || msg.ReasoningContent != "" || len(msg.ToolCalls) > 0)
		if streaming && callErr != nil && !generated && !hasModelUsage(msg) &&
			isStreamOptionsUnsupportedError(callErr) {
			s.state.StreamFallback = true
			msg, callErr = m.generateOnce(ctx, prepared, opts...)
			generated = msg != nil &&
				(msg.Content != "" || msg.ReasoningContent != "" || len(msg.ToolCalls) > 0)
		}
		if generated || hasModelUsage(msg) || !isContextOverflowError(callErr) {
			// Match Eino's bypassed callback wrapper, which drops partial messages
			// returned alongside an error when the underlying model disables callbacks.
			if callErr != nil && !components.IsCallbacksEnabled(m.BaseChatModel) {
				msg = nil
			}
			return m.complete(ctx, msg, callErr)
		}
		marker := s.state.InflightModel
		s.state.InflightModel = nil
		if err := s.store.RecordModelOverflow(
			ctx,
			s.run.Lease(),
			attempt+1,
			callErr.Error(),
			s.encode(),
		); err != nil {
			s.state.InflightModel = marker
			return nil, err
		}

		tokenFloor := max(1, info.TokenLimit/10)
		byteFloor := max(1, info.ByteLimit/10)
		if info.TokenLimit == 0 && info.ByteLimit == 0 {
			// An unconfigured upstream window can still reject input. Use
			// the first rejected byte size as the reference for its floor.
			byteFloor = max(1, info.InputBytes/10)
			if previous != nil {
				byteFloor = max(1, previous.InputBytes/10)
			}
		}
		atFloor := false
		targetTokens, targetBytes = 0, 0
		if info.TokenLimit > 0 {
			atFloor = info.InputTokens <= tokenFloor
			targetTokens = max(tokenFloor, info.InputTokens*7/10)
		}
		if info.ByteLimit > 0 || info.TokenLimit == 0 {
			atFloor = atFloor || info.InputBytes <= byteFloor
			targetBytes = max(byteFloor, info.InputBytes*7/10)
		}
		noShrink := previous != nil && info.InputTokens >= previous.InputTokens &&
			info.InputBytes >= previous.InputBytes
		if attempt >= maxReactiveOverflowAttempts || atFloor || noShrink {
			s.retry = false
			return nil, fmt.Errorf("%w: %v", ErrContextCompressionExhausted, callErr)
		}
		previous = info
	}
}

func hasModelUsage(msg *schema.Message) bool {
	return msg != nil && msg.ResponseMeta != nil && msg.ResponseMeta.Usage != nil
}

func (m *boundaryModel) prepareAttempt(
	ctx context.Context, input []*schema.Message, attempt int, targetTokens, targetBytes int64,
	opts ...model.Option,
) ([]*schema.Message, *durable.ContextInfo, error) {
	s := m.execution
	if s.state.Turn >= s.run.Spec.MaxTurns {
		return nil, nil, fmt.Errorf("model turn limit exceeded: %d", s.run.Spec.MaxTurns)
	}
	if s.store != nil {
		if err := s.store.CheckUsageBudget(ctx, s.run.Lease()); err != nil {
			return nil, nil, err
		}
		_, err := s.store.TakePendingSteer(ctx, s.run.Lease(), func(steer string) json.RawMessage {
			s.state.Steers = append(s.state.Steers, steer)
			return s.encode()
		})
		if err != nil {
			return nil, nil, err
		}
	}
	options := model.GetCommonOptions(&model.Options{}, opts...)
	input, info, _, err := s.prepareModelInput(ctx, input, options.Tools, attempt, targetTokens, targetBytes)
	if err != nil {
		return nil, nil, err
	}
	if err := s.store.CheckUsageBudget(ctx, s.run.Lease()); err != nil {
		return nil, nil, err
	}
	turn := int64(s.state.Turn)
	s.state.InflightModel = &InflightModel{Kind: "model", Sequence: turn}
	if err := s.store.CommitBoundary(ctx, s.run.Lease(), durable.Boundary{
		Kind:    durable.BoundaryModelStarted,
		State:   s.encode(),
		Context: info,
		Turn:    &turn,
	}); err != nil {
		s.state.InflightModel = nil
		return nil, nil, err
	}
	return input, info, nil
}

func (m *boundaryModel) complete(ctx context.Context, msg *schema.Message, callErr error) (*schema.Message, error) {
	s := m.execution
	if msg == nil {
		if callErr != nil {
			s.retry = true
			return nil, callErr
		}
		return nil, ErrModelUsageUnavailable
	}
	if !hasModelUsage(msg) {
		return nil, errors.Join(ErrModelUsageUnavailable, callErr)
	}
	usage := msg.ResponseMeta.Usage
	tokens := int64(usage.TotalTokens)
	if tokens == 0 {
		tokens = int64(usage.PromptTokens) + int64(usage.CompletionTokens)
	}
	cost, costErr := s.usageCost(usage)
	previousState := s.state
	if costErr != nil {
		s.state.PricingFailure = costErr.Error()
	}
	callTurn := s.state.Turn
	s.state.Turn++
	s.state.ReturnDirectlyStep = ""
	s.state.InflightModel = nil
	if callErr == nil {
		s.state.PendingModel = msg
		s.state.UsageAnchor = &UsageAnchor{
			Turn: callTurn, PromptTokens: int64(usage.PromptTokens),
			EstimatedInputTokens: s.lastEstimatedInputTokens,
		}
	}
	if err := s.store.RecordUsageWithCost(
		ctx,
		s.run.Lease(),
		int64(callTurn),
		"model",
		tokens,
		cost,
		s.encode(),
	); err != nil {
		s.state = previousState
		return nil, err
	}
	if costErr != nil {
		return nil, errors.Join(costErr, callErr)
	}
	if callErr != nil {
		s.retry = true
		return nil, callErr
	}
	return m.applyResponse(ctx, msg)
}

func (m *boundaryModel) applyResponse(ctx context.Context, msg *schema.Message) (*schema.Message, error) {
	s := m.execution
	if msg.ResponseMeta != nil &&
		(msg.ResponseMeta.FinishReason == "length" || msg.ResponseMeta.FinishReason == "content_filter") {
		return nil, fmt.Errorf("incomplete model response: %s", msg.ResponseMeta.FinishReason)
	}
	if err := s.store.CheckUsageBudget(ctx, s.run.Lease()); err != nil {
		return nil, err
	}

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
			mcpTool := len(s.run.Spec.MCPServers) > 0 &&
				strings.HasPrefix(call.Function.Name, agent.MCPToolPrefix)
			if mcpTool && strings.TrimSpace(call.Function.Arguments) == "" {
				call.Function.Arguments = "{}"
			}
			binding, known := s.bindings[call.Function.Name]
			subtaskDelegateCall := s.hasTools && !known && s.run.ParentRunID != "" &&
				call.Function.Name == durable.DelegateSubAgentToolName
			if !known && !mcpTool && !subtaskDelegateCall {
				return nil, fmt.Errorf("model requested unknown tool: %s", call.Function.Name)
			}
			inputError := batchError
			switch {
			case subtaskDelegateCall:
				if inputError == nil {
					inputError = errors.New("subtask cannot delegate further subtasks")
				}
			case binding.IsDelegate:
				if !json.Valid([]byte(call.Function.Arguments)) {
					if inputError == nil {
						inputError = errors.New("invalid JSON arguments for delegate_subagent")
					}
				} else if inputError == nil && binding.Validate != nil {
					inputError = binding.Validate(call.Function.Arguments)
				}
			default:
				if !json.Valid([]byte(call.Function.Arguments)) {
					return nil, errors.New("invalid tool arguments")
				}
				if inputError == nil && binding.EndRun && binding.Validate != nil {
					inputError = binding.Validate(call.Function.Arguments)
				}
			}
			if inputError == nil && s.state.ReturnDirectlyStep == "" &&
				(binding.EndRun || binding.ReturnDirectly) {
				s.state.ReturnDirectlyStep = call.ID
			}
			if inputError != nil {
				call.Function.Name = invalidInputTool
				if subtaskDelegateCall {
					call.Function.Arguments = subtaskDelegationUnavailableFeedback
				} else {
					call.Function.Arguments = toolInputFeedback(inputError)
				}
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
		intent := durable.StepIntent{
			ID:     input.CallID,
			Input:  input.Name + "\x00" + input.Arguments,
			Policy: binding.Policy,
		}
		decision, err := s.store.BeginStep(ctx, s.run.Lease(), intent)
		if errors.Is(err, durable.ErrConflict) && binding.Tool == nil &&
			strings.HasPrefix(input.Name, agent.MCPToolPrefix) {
			serverName, remoteName, found := strings.Cut(
				strings.TrimPrefix(input.Name, agent.MCPToolPrefix),
				"__",
			)
			for _, server := range s.run.Spec.MCPServers {
				if found && server.Name == serverName && server.PolicyFor(remoteName) != intent.Policy {
					intent.Policy = server.PolicyFor(remoteName)
					decision, err = s.store.BeginStep(ctx, s.run.Lease(), intent)
					break
				}
			}
		}
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
			if s.run.ParentRunID != "" {
				const feedback = `{"error":{"code":"CLARIFICATION_UNAVAILABLE","message":"subtask cannot request user clarification directly; finish with available context or explain what information is missing for the parent agent"}}`
				return s.finishToolFeedback(ctx, input.CallID, feedback)
			}
			return s.pause(ctx, input.CallID, durable.InteractionClarification, input.Arguments)
		}
		if binding.Validate != nil {
			if validationErr := binding.Validate(input.Arguments); validationErr != nil {
				return s.finishToolFeedback(ctx, input.CallID, toolInputFeedback(validationErr))
			}
		}
		if binding.IsDelegate {
			request, err := parseSubAgentRequest(input.Arguments)
			if err != nil {
				return s.finishToolFeedback(ctx, input.CallID, toolInputFeedback(err))
			}
			request.StepID = input.CallID
			detail := "awaiting delegated subtask"
			if request.SubAgent != "" {
				detail = "awaiting subagent " + request.SubAgent
			}
			s.pendingSpawn = &request
			s.pending = &durable.Boundary{
				Kind: durable.BoundaryAwaitExternal, Detail: detail, WaitingStep: input.CallID,
			}
			return nil, tool.Interrupt(ctx, "save execution boundary")
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
				return s.finishToolFeedback(ctx, input.CallID, toolInputFeedback(err))
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
		previousFiles := s.state.ArtifactFiles
		s.recordReturnDirectly(input.CallID, output.Result)
		var artifacts []durable.FileArtifact
		if binding.FileAction != "" && !toolHasError(output.Result) {
			artifacts = fileArtifacts(s.run.Spec.Filesystem, binding.FileAction, input.Arguments)
			if len(artifacts) > 0 {
				s.state.ArtifactFiles = indexArtifact(previousFiles, artifacts[0])
			}
		}
		if err = s.store.FinishStepWithArtifacts(
			ctx, s.run.Lease(), input.CallID, output.Result, artifacts, s.encode(),
		); err != nil {
			s.state.Final = previousFinal
			s.state.ArtifactFiles = previousFiles
			s.toolError = err
			return nil, err
		}
		return output, nil
	}
}

func fileArtifacts(
	filesystem *durable.FilesystemBinding, action, arguments string,
) []durable.FileArtifact {
	var input struct {
		Path string `json:"file_path"`
	}
	if filesystem == nil || json.Unmarshal([]byte(arguments), &input) != nil || input.Path == "" {
		return nil
	}
	workspace := path.Clean(filesystem.Workspace)
	if !path.IsAbs(workspace) {
		return nil
	}
	cleanPath := path.Clean(input.Path)
	if !path.IsAbs(cleanPath) {
		cleanPath = path.Join(workspace, cleanPath)
	}
	if cleanPath != workspace && !strings.HasPrefix(cleanPath, strings.TrimSuffix(workspace, "/")+"/") {
		return nil
	}
	artifacts := []durable.FileArtifact{
		{Path: cleanPath, Name: path.Base(cleanPath), Action: action, Provider: filesystem.Provider},
	}
	if executionpkg.ValidateFileArtifacts(artifacts) != nil {
		// Preserve the completed tool result even when its path cannot be indexed.
		return nil
	}
	return artifacts
}

func indexArtifact(
	previous []contextbuilder.FileIndexEntry,
	artifact durable.FileArtifact,
) []contextbuilder.FileIndexEntry {
	files := make([]contextbuilder.FileIndexEntry, 1, min(len(previous)+1, contextbuilder.MaxFileIndexEntries))
	files[0] = contextbuilder.FileIndexEntry{
		URI: artifact.Path, Name: artifact.Name, Source: "workspace", Action: artifact.Action, Accessible: true,
	}
	for _, file := range previous {
		if len(files) == contextbuilder.MaxFileIndexEntries {
			break
		}
		if file.URI != artifact.Path {
			files = append(files, file)
		}
	}
	return files
}

func (s *execution) finishToolFeedback(ctx context.Context, stepID, feedback string) (*compose.ToolOutput, error) {
	if err := s.store.FinishStep(ctx, s.run.Lease(), stepID, feedback, s.encode()); err != nil {
		return nil, err
	}
	return &compose.ToolOutput{Result: feedback}, nil
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

type sessionCloser interface {
	CloseSession(context.Context) error
}

func finishSessions(ctx context.Context, bindings map[string]Binding) {
	var closing sync.WaitGroup
	for _, binding := range bindings {
		if session, ok := binding.Tool.(sessionCloser); ok {
			closing.Go(func() { _ = session.CloseSession(ctx) })
		}
	}
	closing.Wait()
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
