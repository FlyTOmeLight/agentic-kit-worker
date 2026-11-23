package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

const (
	readStepResultTool = "read_step_result"
	defaultReadLimit   = 8192
	maxReadLimit       = 16384
)

type stepResultReader struct {
	execution *execution
}

func readStepResultBinding(s *execution) (Binding, error) {
	reader := &stepResultReader{execution: s}
	info, err := stepResultToolInfo()
	if err != nil {
		return Binding{}, err
	}
	params, err := info.ParamsOneOf.ToJSONSchema()
	if err != nil {
		return Binding{}, err
	}
	// The published schema tells the model the page ceiling. The handler
	// clamps oversized requests even when the model ignores that ceiling.
	limit, _ := params.Properties.Get("limit")
	limit.Maximum = ""
	validate, err := CompileArguments(info)
	return Binding{Policy: durable.ReadOnly, Tool: reader, Validate: validate}, err
}

func (*stepResultReader) Info(context.Context) (*schema.ToolInfo, error) {
	return stepResultToolInfo()
}

func stepResultToolInfo() (*schema.ToolInfo, error) {
	var parameters jsonschema.Schema
	if err := json.Unmarshal([]byte(`{
		"type":"object",
		"properties":{
			"step_id":{"type":"string","minLength":1,"description":"A completed step ID from this run."},
			"offset":{"type":"integer","minimum":0,"default":0,"description":"Start at this byte offset."},
			"limit":{"type":"integer","default":8192,"maximum":16384,"description":"Maximum bytes to return."}
		},
		"required":["step_id"]
	}`), &parameters); err != nil {
		return nil, err
	}
	return &schema.ToolInfo{
		Name:        readStepResultTool,
		Desc:        "Read the original result of a completed tool step in this run. Use next_offset to continue reading.",
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&parameters),
	}, nil
}

func (t *stepResultReader) InvokableRun(ctx context.Context, raw string, _ ...tool.Option) (string, error) {
	var args struct {
		StepID string `json:"step_id"`
		Offset int64  `json:"offset"`
		Limit  int64  `json:"limit"`
	}
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return toolInputFeedback(err), nil
	}
	if strings.TrimSpace(args.StepID) == "" || args.Offset < 0 {
		return toolInputFeedback(errors.New("step_id is required and offset must be non-negative")), nil
	}
	full, err := t.execution.store.ReadStepResult(ctx, t.execution.run.Lease(), args.StepID)
	if errors.Is(err, durable.ErrNotFound) {
		return toolInputFeedback(errors.New("step_id does not identify a completed step in this run")), nil
	}
	if err != nil {
		return "", err
	}
	limit := args.Limit
	if limit == 0 {
		limit = defaultReadLimit
	}
	limit = max(1, min(limit, maxReadLimit))
	offset := int(min(args.Offset, int64(len(full))))
	for offset < len(full) && !utf8.RuneStart(full[offset]) {
		offset++
	}
	end := offset + int(min(limit, int64(len(full)-offset)))
	for end < len(full) && end > offset && !utf8.RuneStart(full[end]) {
		end--
	}
	if end == offset && offset < len(full) {
		return toolInputFeedback(errors.New("limit is too small for the next UTF-8 character")), nil
	}
	result, err := json.Marshal(struct {
		StepID    string `json:"step_id"`
		Offset    int    `json:"offset"`
		Next      int    `json:"next_offset"`
		Total     int    `json:"total_bytes"`
		Truncated bool   `json:"truncated"`
		Content   string `json:"content"`
	}{StepID: args.StepID, Offset: offset, Next: end, Total: len(full), Truncated: end < len(full), Content: full[offset:end]})
	return string(result), err
}
