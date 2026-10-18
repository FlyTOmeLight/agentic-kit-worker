// Package engine connects Eino ADK execution to the platform's task leases and storage.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

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
	Validate       func(string) error
	Tool           tool.InvokableTool
	Policy         durable.Policy
}

// Build resolves only the immutable configuration accepted for this run.
type Build func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]Binding, error)

type Engine struct {
	Store *durable.Store
	Build Build
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
	ctx = context.WithValue(ctx, executionKey{}, Execution{RunID: run.ID, Epoch: run.Epoch})
	s := &execution{store: e.Store, run: run}
	if err := json.Unmarshal(run.State, &s.state); err != nil {
		return err
	}
	if s.state.Version != 0 && s.state.Version != 3 {
		return e.Store.Save(
			ctx,
			run.Lease(),
			run.State,
			durable.Failed,
			"",
			"unsupported execution state; drain legacy runs before upgrading the worker",
			"RunFailed",
		)
	}
	s.state.Version = 3
	llm, bindings, err := e.Build(ctx, run.Spec)
	if err != nil {
		return s.fail(ctx, err)
	}
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
	if len(run.Spec.Toolsets) > 0 {
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
	bindings[invalidInputTool] = Binding{Policy: durable.ReadOnly}
	s.bindings = bindings
	maxIterations := run.Spec.MaxTurns
	if hasReturnDirectly {
		// A successful terminal tool uses one internal synthetic response, without
		// spending another user-configured model turn.
		maxIterations++
	}
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:          "agent",
		Instruction:   run.Spec.SystemPrompt,
		Model:         llm,
		MaxIterations: maxIterations,
		// Published prompts are literal text, not templates.
		GenModelInput: func(_ context.Context, instruction string, input *adk.AgentInput) ([]*schema.Message, error) {
			return append([]*schema.Message{schema.SystemMessage(instruction)}, input.Messages...), nil
		},
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{
			Tools: tools, ExecuteSequentially: true,
			UnknownToolsHandler: func(ctx context.Context, name, args string) (string, error) {
				if name != invalidInputTool {
					return "", fmt.Errorf("unknown tool: %s", name)
				}
				out, err := s.wrapTool(
					func(_ context.Context, input *compose.ToolInput) (*compose.ToolOutput, error) {
						return &compose.ToolOutput{Result: input.Arguments}, nil
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
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent, CheckPointStore: s})
	var iter *adk.AsyncIterator[*adk.AgentEvent]
	if len(s.state.Checkpoint) > 0 {
		iter, err = runner.Resume(ctx, run.ID)
	} else {
		var messages []*schema.Message
		for _, msg := range run.Spec.History {
			messages = append(
				messages,
				&schema.Message{Role: schema.RoleType(msg.Role), Content: msg.Content},
			)
		}
		messages = append(messages, schema.UserMessage(run.Spec.Query))
		iter = runner.Run(ctx, messages, adk.WithCheckPointID(run.ID))
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
		if s.pauseStatus != "" {
			// A later clarification or review owns the next model turn; clear any
			// provisional direct-tool result so it cannot bypass the human answer.
			s.state.Final = nil
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
		return e.Store.Save(ctx, run.Lease(), s.encode(), durable.Completed, final.Content, "", "RunCompleted")
	}
	if errors.Is(err, durable.ErrLeaseLost) || ctx.Err() != nil {
		return err
	}
	if s.retry {
		return e.Store.Retry(ctx, run.Lease(), s.encode(), err.Error(), time.Second*time.Duration(run.Attempt))
	}
	return s.fail(ctx, err)
}
