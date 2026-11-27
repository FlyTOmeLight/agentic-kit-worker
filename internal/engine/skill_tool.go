package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
	serrors "github.com/FlyTOmeLight/server-go/errors"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

const (
	loadSkillToolName   = durable.LoadSkillToolName
	skillEntryPath      = "SKILL.md"
	maxSkillFileBytes   = 128 << 10
	maxSkillExemptBytes = 16 << 10
	maxSkillPinBytes    = 16 << 10
)

// SkillReader fetches one file from a run's frozen skill manifest using the
// worker's active lease.
type SkillReader interface {
	ReadSkillFile(ctx context.Context, lease durable.Lease, skill, path string) (*durable.SkillFileContent, error)
}

// HTTPSkillReader adapts the API Go SDK execution client to SkillReader.
type HTTPSkillReader struct {
	Client sdk.ExecutionAPI
}

func (r HTTPSkillReader) ReadSkillFile(
	ctx context.Context,
	lease durable.Lease,
	skill, path string,
) (*durable.SkillFileContent, error) {
	if r.Client == nil {
		return nil, errors.New("control plane skill reader is not configured")
	}
	got, err := r.Client.ReadSkillFile(ctx, &sdk.ReadSkillFileRequest{
		ID:       lease.RunID,
		WorkerID: lease.WorkerID,
		Epoch:    lease.Epoch,
		Skill:    skill,
		Path:     path,
	})
	if err != nil {
		var apiErr *serrors.Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict && apiErr.Code == "LeaseLost" {
			return nil, durable.ErrLeaseLost
		}
		return nil, err
	}
	return &durable.SkillFileContent{
		Skill:      got.Skill,
		Path:       got.Path,
		VersionURI: got.VersionURI,
		MediaType:  got.MediaType,
		SizeBytes:  got.SizeBytes,
		Content:    got.Content,
	}, nil
}

type loadedSkillOutput struct {
	Skill       string   `json:"skill"`
	Path        string   `json:"path"`
	VersionURI  string   `json:"versionUri"`
	MediaType   string   `json:"mediaType,omitempty"`
	SizeBytes   int64    `json:"sizeBytes"`
	HelperFiles []string `json:"helperFiles,omitempty"`
	Content     string   `json:"content"`
}

type compactedSkillPin struct {
	Seq        int64  `json:"seq"`
	Skill      string `json:"skill"`
	VersionURI string `json:"versionUri,omitempty"`
	Content    string `json:"content"`
	ReloadHint string `json:"reloadHint"`
}

type loadSkillTool struct {
	execution *execution
}

func loadSkillBinding(s *execution) (Binding, error) {
	loader := &loadSkillTool{execution: s}
	info, err := loadSkillToolInfo()
	if err != nil {
		return Binding{}, err
	}
	validate, err := CompileArguments(info)
	if err != nil {
		return Binding{}, err
	}
	return Binding{Policy: durable.ReadOnly, Tool: loader, Validate: validate}, nil
}

func (*loadSkillTool) Info(context.Context) (*schema.ToolInfo, error) {
	return loadSkillToolInfo()
}

func loadSkillToolInfo() (*schema.ToolInfo, error) {
	var parameters jsonschema.Schema
	if err := json.Unmarshal([]byte(`{
		"type":"object",
		"properties":{
			"name":{"type":"string","minLength":1,"description":"Skill name from the skills directory."},
			"path":{"type":"string","description":"Optional relative file path inside the skill package. Omit to read SKILL.md."}
		},
		"required":["name"],
		"additionalProperties":false
	}`), &parameters); err != nil {
		return nil, err
	}
	return &schema.ToolInfo{
		Name:        loadSkillToolName,
		Desc:        "Load instructions (SKILL.md) or a helper file from a skill listed in this run's skills directory.",
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(&parameters),
	}, nil
}

func (t *loadSkillTool) InvokableRun(ctx context.Context, raw string, _ ...tool.Option) (string, error) {
	var args struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return toolInputFeedback(err), nil
	}
	name := strings.TrimSpace(args.Name)
	if name == "" {
		return toolInputFeedback(errors.New("name is required")), nil
	}
	skill, ok := findRunSkill(t.execution.run.Spec.Skills, name)
	if !ok {
		return skillToolError("SKILL_NOT_FOUND", fmt.Sprintf("skill %q is not mounted on this run", name)), nil
	}
	entryPath := skill.EntryPath
	if entryPath == "" {
		entryPath = skillEntryPath
	}
	targetPath := strings.TrimSpace(args.Path)
	if targetPath == "" {
		targetPath = entryPath
	}
	file, ok := findSkillFile(skill.Files, targetPath)
	if !ok {
		return skillToolError(
			"SKILL_FILE_NOT_FOUND",
			fmt.Sprintf("file %q is not in skill %q", targetPath, skill.Name),
		), nil
	}
	if file.SizeBytes > maxSkillFileBytes {
		return skillToolError(
			"SKILL_FILE_TOO_LARGE",
			fmt.Sprintf(
				"file %q in skill %q exceeds the %d byte limit",
				targetPath,
				skill.Name,
				maxSkillFileBytes,
			),
		), nil
	}
	if t.execution.skillReader == nil {
		return skillToolError("SKILL_UNAVAILABLE", "skill loader is not configured on this worker"), nil
	}
	got, err := t.execution.skillReader.ReadSkillFile(ctx, t.execution.run.Lease(), skill.Name, targetPath)
	if err != nil {
		if errors.Is(err, durable.ErrLeaseLost) || ctx.Err() != nil {
			return "", err
		}
		var apiErr *serrors.Error
		if errors.As(err, &apiErr) {
			switch apiErr.Status {
			case http.StatusBadRequest:
				return skillToolError(
					"SKILL_FILE_UNSUPPORTED",
					fmt.Sprintf(
						"skill file %q in %q is not a supported UTF-8 text file or exceeds size limits; do not retry loading it",
						targetPath,
						skill.Name,
					),
				), nil
			case http.StatusNotFound:
				return skillToolError(
					"SKILL_FILE_NOT_FOUND",
					fmt.Sprintf(
						"skill file %q in %q was not found; do not retry loading it",
						targetPath,
						skill.Name,
					),
				), nil
			}
		}
		return skillToolError(
			"SKILL_UNAVAILABLE",
			fmt.Sprintf("skill file %q in %q is temporarily unavailable", targetPath, skill.Name),
		), nil
	}
	if int64(len(got.Content)) > maxSkillFileBytes {
		return skillToolError(
			"SKILL_FILE_TOO_LARGE",
			fmt.Sprintf(
				"file %q in skill %q exceeds the %d byte limit",
				targetPath,
				skill.Name,
				maxSkillFileBytes,
			),
		), nil
	}
	out := loadedSkillOutput{
		Skill:      skill.Name,
		Path:       targetPath,
		VersionURI: skill.VersionURI,
		MediaType:  got.MediaType,
		SizeBytes:  int64(len(got.Content)),
		Content:    got.Content,
	}
	if got.VersionURI != "" {
		out.VersionURI = got.VersionURI
	}
	if targetPath == entryPath {
		for _, f := range skill.Files {
			if f.Path != entryPath {
				out.HelperFiles = append(out.HelperFiles, f.Path)
			}
		}
	}
	encoded, err := json.Marshal(out)
	return string(encoded), err
}

func findRunSkill(skills []durable.SkillSnapshot, name string) (durable.SkillSnapshot, bool) {
	for _, skill := range skills {
		if skill.Name == name {
			return skill, true
		}
	}
	return durable.SkillSnapshot{}, false
}

func findSkillFile(files []durable.SkillFileSnapshot, path string) (durable.SkillFileSnapshot, bool) {
	for _, file := range files {
		if file.Path == path {
			return file, true
		}
	}
	return durable.SkillFileSnapshot{}, false
}

func skillToolError(code, message string) string {
	raw, _ := json.Marshal(map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
	return string(raw)
}

func skillExemptLimit(budget contextbuilder.Budget) int64 {
	limit := int64(maxSkillExemptBytes)
	if budget.MaxToolResultBytes > 0 && budget.MaxToolResultBytes < limit {
		limit = budget.MaxToolResultBytes
	}
	return limit
}

func skillReloadHint(skill, versionURI string) string {
	if versionURI != "" {
		return fmt.Sprintf(
			"[Loaded skill %q (%s) was compacted; call load_skill(name=%q) to reload full instructions]",
			skill,
			versionURI,
			skill,
		)
	}
	return fmt.Sprintf(
		"[Loaded skill %q was compacted; call load_skill(name=%q) to reload full instructions]",
		skill,
		skill,
	)
}

func parseLoadedSkillEntry(content string) (skill, versionURI, body string, ok bool) {
	if toolHasError(content) {
		return "", "", "", false
	}
	var out loadedSkillOutput
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		return "", "", "", false
	}
	if out.Skill == "" || out.Path != skillEntryPath || out.VersionURI == "" || out.Content == "" {
		return "", "", "", false
	}
	return out.Skill, out.VersionURI, out.Content, true
}

func buildSkillPin(seq int64, content string) (*compactedSkillPin, bool) {
	skill, versionURI, body, ok := parseLoadedSkillEntry(content)
	if !ok {
		return nil, false
	}
	hint := skillReloadHint(skill, versionURI)
	pinned := fmt.Sprintf("Skill: %s (%s)\n\n%s", skill, versionURI, body)
	if len(pinned) > maxSkillPinBytes {
		pinned = hint
	}
	return &compactedSkillPin{
		Seq:        seq,
		Skill:      skill,
		VersionURI: versionURI,
		Content:    pinned,
		ReloadHint: hint,
	}, true
}
