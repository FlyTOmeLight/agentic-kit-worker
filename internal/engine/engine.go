// Package engine connects Eino ADK execution to the platform's task leases and storage.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/agent"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type Binding struct {
	EndRun         bool
	ReturnDirectly bool
	IsDelegate     bool
	Validate       func(string) error
	Tool           tool.InvokableTool
	Policy         durable.Policy
	FileAction     string
}

// Build resolves only the immutable configuration accepted for this run. The
// store is passed so an environment backend can record its durable intent
// under the caller's claim before touching the network.
type Build func(context.Context, *durable.Store, durable.Spec) (model.ToolCallingChatModel, map[string]Binding, error)

// RunContext is the caller's claim on the run, handed to Build so an
// environment backend can bind its lifecycle to the same lease.
type RunContext struct {
	RunID       string
	WorkerID    string
	Epoch       int64
	OwnerID     string
	TenantID    string
	GoalID      string
	ParentRunID string
}

type runContextKey struct{}

// WithRunContext attaches the claim to the build context.
func WithRunContext(ctx context.Context, rc RunContext) context.Context {
	return context.WithValue(ctx, runContextKey{}, rc)
}

// BuildRunContext returns the claim attached by Execute, if any.
func BuildRunContext(ctx context.Context) (RunContext, bool) {
	v, ok := ctx.Value(runContextKey{}).(RunContext)
	return v, ok
}

type Engine struct {
	Store              *durable.Store
	Build              Build
	Summarizer         Summarizer
	CompactionMaxBytes int
	StreamIdleTimeout  time.Duration
	NewDeltaPublisher  func(context.Context) DeltaPublisher
}

// DeltaPublisher accepts best-effort deltas from a single model stream.
// Publish must be nonblocking. Flush ends publishing and waits within ctx.
type DeltaPublisher interface {
	Publish(durable.StreamDelta)
	Flush(context.Context)
}

type operationKey struct{}

type executionKey struct{}

type Execution struct {
	RunID string
	Epoch int64
}

func ExecutionContext(ctx context.Context) Execution {
	v, _ := ctx.Value(executionKey{}).(Execution)
	return v
}

// OperationID remains stable across worker attempts and Eino checkpoint resumes.
func OperationID(ctx context.Context) string {
	v, _ := ctx.Value(operationKey{}).(string)
	return v
}

func (e *Engine) Execute(ctx context.Context, run *durable.Run) error {
	return e.execute(ctx, run, nil)
}

func (e *Engine) execute(ctx context.Context, run *durable.Run, beforeRelease func() bool) error {
	var bindings map[string]Binding
	defer func() {
		release := beforeRelease == nil
		defer func() { finishEnvironments(context.WithoutCancel(ctx), bindings, release) }()
		defer func() { finishSessions(context.WithoutCancel(ctx), bindings) }()
		if beforeRelease != nil {
			release = beforeRelease()
		}
	}()
	ctx = context.WithValue(ctx, executionKey{}, Execution{RunID: run.ID, Epoch: run.Epoch})
	tenantID := ""
	if run.Scope != nil {
		tenantID = run.Scope.TenantID
	}
	ctx = WithRunContext(ctx, RunContext{
		RunID:       run.ID,
		WorkerID:    run.WorkerID,
		Epoch:       run.Epoch,
		OwnerID:     run.OwnerID,
		TenantID:    tenantID,
		GoalID:      run.GoalID,
		ParentRunID: run.ParentRunID,
	})
	s := &execution{
		store: e.Store, run: run,
		summarizer: e.Summarizer, compactionMaxBytes: e.CompactionMaxBytes,
		streamIdleTimeout: e.StreamIdleTimeout, newDeltaPublisher: e.NewDeltaPublisher,
	}
	snapshot, err := decodeSnapshot(run)
	if err != nil {
		return s.fail(ctx, err)
	}
	s.snapshot = snapshot
	if err := json.Unmarshal(run.State, &s.state); err != nil {
		return err
	}
	if s.state.Version != 0 && s.state.Version != 3 {
		return e.Store.CommitBoundary(ctx, run.Lease(), durable.Boundary{
			Kind:   durable.BoundaryFail,
			State:  run.State,
			Detail: "unsupported execution state; drain legacy runs before upgrading the worker",
		})
	}
	s.state.Version = 3
	if s.state.InflightModel != nil {
		marker := s.state.InflightModel
		return s.block(ctx, fmt.Sprintf(
			"model dispatch #%d (%s) has unknown consumption; automatic replay is disabled",
			marker.Sequence, marker.Kind,
		))
	}
	if err := s.checkPricing(ctx); err != nil {
		return s.fail(ctx, err)
	}
	llm, bindings, err := e.Build(ctx, e.Store, run.Spec)
	if err != nil {
		var retryable interface{ Retryable() bool }
		if errors.As(err, &retryable) && retryable.Retryable() {
			return e.Store.Retry(
				ctx,
				run.Lease(),
				s.encode(),
				err.Error(),
				time.Second*time.Duration(run.Attempt),
			)
		}
		return s.fail(ctx, err)
	}
	s.model = llm
	var tools []tool.BaseTool
	hasReturnDirectly := false
	for _, spec := range run.Spec.Tools {
		binding, ok := bindings[spec.Name]
		if !ok || binding.Policy != spec.Policy {
			return s.fail(ctx, fmt.Errorf("tool binding mismatch: %s", spec.Name))
		}
		binding.ReturnDirectly = spec.ReturnDirectly
		bindings[spec.Name] = binding
		tools = append(tools, binding.Tool)
		if spec.ReturnDirectly || binding.EndRun {
			hasReturnDirectly = true
		}
	}
	dynamicTools := len(run.Spec.Toolsets) > 0 || len(run.Spec.MCPServers) > 0
	if dynamicTools {
		configured := map[string]bool{}
		for _, t := range run.Spec.Tools {
			configured[t.Name] = true
		}
		names := make([]string, 0, len(bindings))
		for name := range bindings {
			if !configured[name] && bindings[name].Tool != nil {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			tools = append(tools, bindings[name].Tool)
		}
	}
	if bindings == nil {
		bindings = make(map[string]Binding)
	}
	if _, exists := bindings[durable.DelegateSubAgentToolName]; !exists && canDelegate(run) {
		binding, err := delegateSubAgentBinding(s)
		if err != nil {
			return s.fail(ctx, err)
		}
		bindings[durable.DelegateSubAgentToolName] = binding
		tools = append(tools, binding.Tool)
		dynamicTools = true
	}
	if _, exists := bindings[readStepResultTool]; !exists &&
		(len(run.Spec.Tools) > 0 || dynamicTools) {
		binding, err := readStepResultBinding(s)
		if err != nil {
			return s.fail(ctx, err)
		}
		bindings[readStepResultTool] = binding
		tools = append(tools, binding.Tool)
	}
	if dynamicTools && len(tools) > 128 {
		return s.fail(ctx, fmt.Errorf("tool count %d exceeds limit of 128", len(tools)))
	}
	bindings[invalidInputTool] = Binding{Policy: durable.ReadOnly}
	s.bindings = bindings
	s.hasTools = len(tools) > 0
	maxIterations := run.Spec.MaxTurns
	if hasReturnDirectly {
		// A successful terminal tool uses one internal synthetic response, without
		// spending another user-configured model turn.
		maxIterations++
	}
	chatAgent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "agent",
		Instruction:   run.Spec.SystemPrompt,
		Model:         llm,
		MaxIterations: maxIterations,
		// Published prompts are literal text, not templates.
		GenModelInput: func(_ context.Context, _ string, input *adk.AgentInput) ([]*schema.Message, error) {
			return input.Messages, nil
		},
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{
			Tools: tools, ExecuteSequentially: true,
			UnknownToolsHandler: func(ctx context.Context, name, args string) (string, error) {
				feedback := args
				switch {
				case name == invalidInputTool:
				case strings.HasPrefix(name, agent.MCPToolPrefix):
					bindings[name] = Binding{Policy: durable.ReadOnly}
					message, _ := json.Marshal(
						fmt.Sprintf("MCP tool %q is no longer available", name),
					)
					feedback = `{"error":{"code":"TOOL_UNAVAILABLE","message":` + string(
						message,
					) + `}}`
				default:
					return "", fmt.Errorf("unknown tool: %s", name)
				}
				out, err := s.wrapTool(
					func(context.Context, *compose.ToolInput) (*compose.ToolOutput, error) {
						return &compose.ToolOutput{Result: feedback}, nil
					},
				)(
					ctx,
					&compose.ToolInput{
						Name:      name,
						Arguments: args,
						CallID:    compose.GetToolCallID(ctx),
					},
				)
				if err != nil {
					return "", err
				}
				return out.Result, nil
			},
			ToolCallMiddlewares: []compose.ToolMiddleware{{Invokable: s.wrapTool}},
		}},
		Handlers: []adk.ChatModelAgentMiddleware{s},
	})
	if err != nil {
		return s.fail(ctx, err)
	}
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: chatAgent, CheckPointStore: s})
	var iter *adk.AsyncIterator[*adk.AgentEvent]
	if len(s.state.Checkpoint) > 0 {
		iter, err = runner.Resume(ctx, run.ID)
	} else {
		iter = runner.Run(
			ctx,
			[]*schema.Message{schema.UserMessage(run.Spec.Query)},
			adk.WithCheckPointID(run.ID),
		)
	}
	// Eino owns the ReAct loop. This loop consumes events and resumes automatic
	// tool-boundary checkpoints after their database transaction has committed.
	for err == nil {
		var interrupted bool
		var final *schema.Message
		for event, ok := iter.Next(); ok; event, ok = iter.Next() {
			if event.Err != nil && err == nil {
				err = event.Err
			}
			if event.Action != nil && event.Action.Interrupted != nil {
				interrupted = true
			}
			if event.Output != nil && event.Output.MessageOutput != nil {
				msg := event.Output.MessageOutput.Message
				if msg != nil && msg.Role == schema.Assistant && len(msg.ToolCalls) == 0 {
					final = msg
				}
			}
		}
		// Drain the iterator before returning: no ADK goroutine may outlive this lease.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			break
		}
		if s.pending != nil {
			return nil
		} // Set atomically saved pause and checkpoint.
		if interrupted {
			iter, err = runner.Resume(ctx, run.ID)
			continue
		}
		if s.state.Final != nil {
			final = schema.AssistantMessage(*s.state.Final, nil)
		}
		if final == nil {
			return s.fail(ctx, errors.New("Eino runner ended without a final response"))
		}
		return e.Store.CommitBoundary(ctx, run.Lease(), durable.Boundary{
			Kind:   durable.BoundaryComplete,
			State:  s.encode(),
			Output: final.Content,
		})
	}
	if errors.Is(err, durable.ErrLeaseLost) || ctx.Err() != nil {
		return err
	}
	if marker := s.state.InflightModel; marker != nil {
		return s.block(ctx, fmt.Sprintf(
			"model dispatch #%d (%s) has unknown consumption; automatic replay is disabled: %v",
			marker.Sequence, marker.Kind, err,
		))
	}
	if s.retry {
		return e.Store.Retry(ctx, run.Lease(), s.encode(), err.Error(), time.Second*time.Duration(run.Attempt))
	}
	return s.fail(ctx, err)
}
