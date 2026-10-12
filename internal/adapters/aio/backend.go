// Package aio adapts AIO Sandbox's public SDK to Eino filesystem tools.
package aio

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	api "github.com/agent-infra/sandbox-sdk-go"
	"github.com/agent-infra/sandbox-sdk-go/client"
	"github.com/agent-infra/sandbox-sdk-go/core"
	"github.com/agent-infra/sandbox-sdk-go/option"
	"github.com/cloudwego/eino/adk/filesystem"
)

type Backend struct {
	client *client.Client
	root   string
}

func New(endpoint, token, root string) (*Backend, error) {
	return NewWithClient(endpoint, token, root, http.DefaultClient)
}

func NewWithClient(endpoint, token, root string, httpClient *http.Client) (*Backend, error) {
	if endpoint == "" || token == "" || !path.IsAbs(root) {
		return nil, errors.New("AIO endpoint, credential and absolute workspace are required")
	}
	return &Backend{client: client.NewClient(option.WithBaseURL(endpoint), option.WithHTTPHeader(http.Header{"Authorization": []string{"Bearer " + token}}), option.WithMaxAttempts(1), option.WithHTTPClient(httpClient)), root: path.Clean(root)}, nil
}

// InputError means a request was rejected or no mutation was made. Transport
// errors retain their identity and cannot be treated as a known-safe failure.
type InputError struct{ Message string }

func (e *InputError) Error() string     { return e.Message }
func (e *InputError) Correctable() bool { return true }
func invalid(message string) error      { return &InputError{message} }
func checked(ok *bool, message *string, err error) error {
	if err != nil {
		var apiErr *core.APIError
		if errors.As(err, &apiErr) && (apiErr.StatusCode == 400 || apiErr.StatusCode == 422) {
			return invalid("AIO rejected tool input: " + apiErr.Error())
		}
		return err
	}
	if ok == nil || !*ok {
		return fmt.Errorf("AIO operation rejected: %s", value(message))
	}
	return nil
}
func value(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func ptr[T any](v T) *T { return &v }
func (b *Backend) resolve(raw string) (string, error) {
	if raw == "" {
		raw = "."
	}
	if !path.IsAbs(raw) {
		raw = path.Join(b.root, raw)
	}
	p := path.Clean(raw)
	if p != b.root && !strings.HasPrefix(p, b.root+"/") {
		return "", invalid("path must be inside the bound workspace")
	}
	return p, nil
}
func (b *Backend) Read(ctx context.Context, r *filesystem.ReadRequest) (*filesystem.FileContent, error) {
	p, err := b.resolve(r.FilePath)
	if err != nil {
		return nil, err
	}
	if r.FilePath == "" || r.Offset < 0 || r.Limit < 0 {
		return nil, invalid("file path and valid line range are required")
	}
	start := r.Offset - 1
	if start < 0 {
		start = 0
	}
	input := &api.FileReadRequest{File: p, StartLine: &start}
	if r.Limit > 0 {
		input.EndLine = ptr(start + r.Limit)
	}
	out, err := b.client.File.ReadFile(ctx, input)
	if err != nil {
		return nil, checked(nil, nil, err)
	}
	if err = checked(out.Success, out.Message, nil); err != nil {
		return nil, err
	}
	if out.Data == nil {
		return nil, errors.New("AIO read response has no data")
	}
	return &filesystem.FileContent{Content: out.Data.Content}, nil
}
func (b *Backend) Write(ctx context.Context, r *filesystem.WriteRequest) error {
	p, err := b.resolve(r.FilePath)
	if err != nil {
		return err
	}
	if r.FilePath == "" {
		return invalid("file path is required")
	}
	out, err := b.client.File.WriteFile(ctx, &api.FileWriteRequest{File: p, Content: r.Content})
	if err != nil {
		return checked(nil, nil, err)
	}
	return checked(out.Success, out.Message, nil)
}
func (b *Backend) Edit(ctx context.Context, r *filesystem.EditRequest) error {
	if r.OldString == "" {
		return invalid("old string must be non-empty")
	}
	current, err := b.Read(ctx, &filesystem.ReadRequest{FilePath: r.FilePath})
	if err != nil {
		return err
	}
	n := strings.Count(current.Content, r.OldString)
	if n == 0 || (!r.ReplaceAll && n != 1) {
		return invalid(fmt.Sprintf("old string matched %d times; file was not changed", n))
	}
	// Eino serializes tools for this workspace. AIO's replace endpoint replaces
	// all occurrences; count verification preserves Eino's unique-match contract.
	p, err := b.resolve(r.FilePath)
	if err != nil {
		return err
	}
	out, err := b.client.File.ReplaceInFile(ctx, &api.FileReplaceRequest{File: p, OldStr: r.OldString, NewStr: r.NewString})
	if err != nil {
		return checked(nil, nil, err)
	}
	return checked(out.Success, out.Message, nil)
}
func (b *Backend) LsInfo(ctx context.Context, r *filesystem.LsInfoRequest) ([]filesystem.FileInfo, error) {
	p, err := b.resolve(r.Path)
	if err != nil {
		return nil, err
	}
	out, err := b.client.File.ListPath(ctx, &api.FileListRequest{Path: p, IncludeSize: ptr(true)})
	if err != nil {
		return nil, checked(nil, nil, err)
	}
	if err = checked(out.Success, out.Message, nil); err != nil {
		return nil, err
	}
	if out.Data == nil {
		return nil, errors.New("AIO list response has no data")
	}
	files := make([]filesystem.FileInfo, 0, len(out.Data.Files))
	for _, f := range out.Data.Files {
		if f == nil {
			continue
		}
		size := int64(0)
		if f.Size != nil {
			size = int64(*f.Size)
		}
		files = append(files, filesystem.FileInfo{Path: f.Path, IsDir: f.IsDirectory, Size: size, ModifiedAt: value(f.ModifiedTime)})
	}
	return files, nil
}
func (b *Backend) GlobInfo(ctx context.Context, r *filesystem.GlobInfoRequest) ([]filesystem.FileInfo, error) {
	p, err := b.resolve(r.Path)
	if err != nil {
		return nil, err
	}
	out, err := b.client.File.FindFiles(ctx, &api.FileFindRequest{Path: p, Glob: r.Pattern})
	if err != nil {
		return nil, checked(nil, nil, err)
	}
	if err = checked(out.Success, out.Message, nil); err != nil {
		return nil, err
	}
	if out.Data == nil {
		return nil, errors.New("AIO glob response has no data")
	}
	files := make([]filesystem.FileInfo, 0, len(out.Data.Files))
	for _, p := range out.Data.Files {
		files = append(files, filesystem.FileInfo{Path: p})
	}
	return files, nil
}
func (b *Backend) GrepRaw(ctx context.Context, r *filesystem.GrepRequest) ([]filesystem.GrepMatch, error) {
	p, err := b.resolve(r.Path)
	if err != nil {
		return nil, err
	}
	input := &api.FileGrepRequest{Path: p, Pattern: r.Pattern, CaseInsensitive: &r.CaseInsensitive, Multiline: &r.EnableMultiline, ContextBefore: &r.BeforeLines, ContextAfter: &r.AfterLines, MaxResults: ptr(200)}
	if r.Glob != "" {
		input.Include = []string{r.Glob}
	}
	if r.FileType != "" {
		input.Type = &r.FileType
	}
	out, err := b.client.File.GrepFiles(ctx, input)
	if err != nil {
		return nil, checked(nil, nil, err)
	}
	if err = checked(out.Success, out.Message, nil); err != nil {
		return nil, err
	}
	if out.Data == nil {
		return nil, errors.New("AIO grep response has no data")
	}
	matches := make([]filesystem.GrepMatch, 0, len(out.Data.Matches))
	for _, m := range out.Data.Matches {
		if m != nil {
			matches = append(matches, filesystem.GrepMatch{Path: m.File, Line: m.LineNumber, Content: m.LineContent})
		}
	}
	return matches, nil
}
func (b *Backend) Execute(ctx context.Context, r *filesystem.ExecuteRequest) (*filesystem.ExecuteResponse, error) {
	if r.RunInBackendGround {
		return nil, invalid("unmanaged background execution is disabled")
	}
	if strings.TrimSpace(r.Command) == "" {
		return nil, invalid("command is required")
	}
	out, err := b.client.Bash.Exec(ctx, &api.BashExecRequest{Command: r.Command, Env: map[string]*string{"PYTHONDONTWRITEBYTECODE": ptr("1")}, ExecDir: &b.root, AsyncMode: ptr(true), HardTimeout: ptr(float64(120)), MaxOutputLength: ptr(24000)})
	if err != nil {
		return nil, checked(nil, nil, err)
	}
	if err = checked(out.Success, out.Message, nil); err != nil {
		return nil, err
	}
	if out.Data == nil {
		return nil, errors.New("AIO execute response has no data")
	}
	cmd := out.Data
	stdout, stderr := value(cmd.Stdout), value(cmd.Stderr)
	truncated := false
	for cmd.Status == api.CommandStatus("running") {
		page, err := b.client.Bash.Output(ctx, &api.BashOutputRequest{SessionId: cmd.SessionId, CommandId: &cmd.CommandId, Offset: cmd.Offset, StderrOffset: cmd.StderrOffset, Wait: ptr(true), WaitTimeout: ptr(float64(1))})
		if err != nil {
			if ctx.Err() != nil {
				stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				_, stopErr := b.client.Bash.Kill(stopCtx, &api.BashKillRequest{SessionId: cmd.SessionId, Signal: ptr("SIGKILL")})
				cancel()
				if stopErr != nil {
					return nil, fmt.Errorf("AIO cancellation could not confirm stop: %w", stopErr)
				}
			}
			return nil, err
		}
		if err = checked(page.Success, page.Message, nil); err != nil {
			return nil, err
		}
		if page.Data == nil || page.Data.Command == nil {
			return nil, errors.New("AIO output response has no command status")
		}
		stdout += value(page.Data.Stdout)
		stderr += value(page.Data.Stderr)
		cmd.Status = page.Data.Command.Status
		cmd.ExitCode = page.Data.Command.ExitCode
		cmd.Offset = page.Data.Offset
		cmd.StderrOffset = page.Data.StderrOffset
		if len(stdout)+len(stderr) > 24000 {
			truncated = true
			if len(stdout) > 12000 {
				stdout = stdout[:12000]
			}
			if len(stderr) > 12000 {
				stderr = stderr[:12000]
			}
		}
	}
	if cmd.ExitCode == nil {
		return nil, errors.New("AIO command ended without exit code")
	}
	return &filesystem.ExecuteResponse{Output: stdout + stderr, ExitCode: cmd.ExitCode, Truncated: truncated}, nil
}

// Ready waits for the bound sandbox and its current execution ownership.
func (b *Backend) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		_, err := b.client.Sandbox.GetContext(ctx)
		if err == nil {
			return nil
		}
		var apiErr *core.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
