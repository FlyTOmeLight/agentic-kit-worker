package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/schema"
	validator "github.com/santhosh-tekuri/jsonschema/v6"
)

// CompileArguments uses the published tool schema; valid JSON alone is insufficient.
func CompileArguments(info *schema.ToolInfo) (func(string) error, error) {
	if info.ParamsOneOf == nil {
		return nil, nil
	}
	params, err := info.ParamsOneOf.ToJSONSchema()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	doc, err := validator.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		return nil, err
	}
	c := validator.NewCompiler()
	if err = c.AddResource("https://agentic.invalid/tool.json", doc); err != nil {
		return nil, err
	}
	s, err := c.Compile("https://agentic.invalid/tool.json")
	if err != nil {
		return nil, err
	}
	return func(args string) error {
		value, err := validator.UnmarshalJSON(strings.NewReader(args))
		if err != nil {
			return err
		}
		return s.Validate(value)
	}, nil
}

func toolInputFeedback(err error) string {
	raw, _ := json.Marshal(
		map[string]any{
			"error":   map[string]string{"code": "INVALID_TOOL_INPUT", "message": fmt.Sprint(err)},
			"changed": false,
		},
	)
	return string(raw)
}

func toolHasError(result string) bool {
	var response struct {
		Error json.RawMessage `json:"error"`
	}
	return json.Unmarshal([]byte(result), &response) == nil && len(response.Error) > 0 &&
		string(response.Error) != "null"
}

const invalidInputTool = "__invalid_tool_input"
