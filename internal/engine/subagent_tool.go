package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

const defaultSubAgentTTL = 10 * time.Minute

type delegateSubAgentTool struct {
	info *schema.ToolInfo
}

func canDelegate(run *durable.Run) bool {
	return run.ParentRunID == "" && (len(run.Spec.SubAgents) > 0 || run.Spec.SelfDelegate)
}

func delegateSubAgentBinding(s *execution) (Binding, error) {
	if !canDelegate(s.run) {
		return Binding{}, errors.New("subagent delegation is unavailable for this run")
	}
	info, err := delegateSubAgentToolInfo(s.run.Spec)
	if err != nil {
		return Binding{}, err
	}
	validate, err := CompileArguments(info)
	if err != nil {
		return Binding{}, err
	}
	return Binding{
		IsDelegate: true,
		Policy:     durable.Idempotent,
		Tool:       &delegateSubAgentTool{info: info},
		Validate: func(raw string) error {
			if err := validate(raw); err != nil {
				return err
			}
			_, err := parseSubAgentRequest(raw)
			return err
		},
	}, nil
}

func delegateSubAgentToolInfo(spec durable.Spec) (*schema.ToolInfo, error) {
	var parameters jsonschema.Schema
	if err := json.Unmarshal([]byte(`{
		"type":"object",
		"properties":{
			"task":{"type":"string","minLength":1,"description":"The task and context the child needs to work independently."},
			"timeout_seconds":{"type":"integer","minimum":1,"description":"Optional child deadline in seconds. Defaults to 600."}
		},
		"required":["task"],
		"additionalProperties":false
	}`), &parameters); err != nil {
		return nil, err
	}
	description := "Delegate a subtask and wait for its result."
	if len(spec.SubAgents) > 0 {
		names := make([]any, 0, len(spec.SubAgents))
		available := make([]string, 0, len(spec.SubAgents))
		for _, sub := range spec.SubAgents {
			names = append(names, sub.Name)
			available = append(available, sub.Name+": "+sub.Description)
		}
		parameters.Properties.Set("subagent", &jsonschema.Schema{
			Type: "string", Enum: names, Description: "The name of a configured subagent.",
		})
		description += " Available subagents: " + strings.Join(available, "; ") + "."
		if !spec.SelfDelegate {
			parameters.Required = append(parameters.Required, "subagent")
		}
	}
	if spec.SelfDelegate {
		if len(spec.SubAgents) == 0 {
			description = "Delegate a subtask to a fresh execution of this agent and wait for its result."
		} else {
			description += " Omit subagent to delegate to a fresh execution of this agent."
		}
		parameters.Properties.Set("instruction", &jsonschema.Schema{
			Type:        "string",
			MaxLength:   new(uint64(4096)),
			Description: "Optional role and execution guidance for a self-delegated task. Use only without subagent.",
		})
	}
	return &schema.ToolInfo{
		Name:        durable.DelegateSubAgentToolName,
		Desc:        description,
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&parameters),
	}, nil
}

func (t *delegateSubAgentTool) Info(context.Context) (*schema.ToolInfo, error) {
	return t.info, nil
}

func (*delegateSubAgentTool) InvokableRun(context.Context, string, ...tool.Option) (string, error) {
	return "", errors.New("subagent delegation requires a durable checkpoint")
}

func parseSubAgentRequest(raw string) (durable.ChildSpawnRequest, error) {
	var args struct {
		SubAgent       string `json:"subagent"`
		Instruction    string `json:"instruction"`
		Task           string `json:"task"`
		TimeoutSeconds *int64 `json:"timeout_seconds"`
	}
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return durable.ChildSpawnRequest{}, err
	}
	switch {
	case strings.TrimSpace(args.Task) == "":
		return durable.ChildSpawnRequest{}, errors.New("task must not be blank")
	case args.SubAgent != "" && args.Instruction != "":
		return durable.ChildSpawnRequest{}, errors.New("instruction is only allowed when subagent is omitted")
	case len(args.Instruction) > 4096:
		return durable.ChildSpawnRequest{}, errors.New("instruction must not exceed 4096 bytes")
	}
	ttl := defaultSubAgentTTL
	if args.TimeoutSeconds != nil {
		const maxSeconds = int64((1<<63 - 1) / time.Second)
		if *args.TimeoutSeconds < 1 || *args.TimeoutSeconds > maxSeconds {
			return durable.ChildSpawnRequest{}, fmt.Errorf(
				"timeout_seconds must be between 1 and %d",
				maxSeconds,
			)
		}
		ttl = time.Duration(*args.TimeoutSeconds) * time.Second
	}
	return durable.ChildSpawnRequest{
		SubAgent: args.SubAgent, Instruction: args.Instruction, Query: args.Task, TTL: ttl,
	}, nil
}
