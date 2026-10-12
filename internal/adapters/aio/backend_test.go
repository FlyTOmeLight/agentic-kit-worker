package aio

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	fs "github.com/cloudwego/eino/adk/filesystem"
	middleware "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/components/tool"
)

func TestBoundWorkspace(t *testing.T) {
	b := &Backend{root: "/workspace/repo"}
	for _, name := range []string{"../../etc/passwd", "/workspace/another/file"} {
		if _, err := b.resolve(name); err == nil {
			t.Fatalf("unbound path accepted: %s", name)
		}
	}
	if got, err := b.resolve("calc.py"); err != nil || got != "/workspace/repo/calc.py" {
		t.Fatalf("relative path: %s, %v", got, err)
	}
}

func TestAIOPublicSDKAndEinoTools(t *testing.T) {
	endpoint := os.Getenv("TEST_AIO_URL")
	if endpoint == "" {
		t.Skip("TEST_AIO_URL is required for real AIO integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := New(endpoint, os.Getenv("TEST_AIO_TOKEN"), "/workspace/repo")
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	envResult, err := b.Execute(ctx, &fs.ExecuteRequest{Command: "python3 -c 'import sys; print(sys.dont_write_bytecode)'"})
	if err != nil || !strings.Contains(envResult.Output, "True") {
		t.Fatalf("Python cache isolation: %+v %v", envResult, err)
	}
	m, err := middleware.New(ctx, &middleware.MiddlewareConfig{Backend: b, Shell: b})
	if err != nil {
		t.Fatal(err)
	}
	_, run, err := m.BeforeAgent(ctx, &adk.ChatModelAgentContext{})
	if err != nil {
		t.Fatal(err)
	}
	tools := map[string]tool.InvokableTool{}
	for _, base := range run.Tools {
		info, err := base.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tools[info.Name] = base.(tool.InvokableTool)
	}
	call := func(name, args string) string {
		t.Helper()
		out, err := tools[name].InvokableRun(ctx, args)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return out
	}
	call("write_file", `{"file_path":"adapter-test.txt","content":"one\ntwo\n"}`)
	out := call("read_file", `{"file_path":"adapter-test.txt","offset":2,"limit":1}`)
	if !strings.Contains(out, "two") || strings.Contains(out, "one") {
		t.Fatalf("line range: %s", out)
	}
	call("edit_file", `{"file_path":"adapter-test.txt","old_string":"two","new_string":"three"}`)
	if out := call("grep", `{"pattern":"three","glob":"adapter-test.txt","output_mode":"content"}`); !strings.Contains(out, "three") {
		t.Fatalf("grep: %s", out)
	}
	if out := call("glob", `{"pattern":"adapter-test.txt"}`); !strings.Contains(out, "adapter-test") {
		t.Fatalf("glob: %s", out)
	}
	if _, err = b.LsInfo(ctx, &fs.LsInfoRequest{}); err != nil {
		t.Fatal(err)
	}
	r, err := b.Execute(ctx, &fs.ExecuteRequest{Command: "printf failure-evidence; exit 7"})
	if err != nil || r.ExitCode == nil || *r.ExitCode != 7 || r.Output != "failure-evidence" {
		t.Fatalf("nonzero command: %+v %v", r, err)
	}
	_, err = tools["edit_file"].InvokableRun(ctx, `{"old_string":"x","new_string":"y"}`)
	var invalid *InputError
	if !errors.As(err, &invalid) {
		t.Fatalf("missing path must be correctable: %v", err)
	}
	if err = b.Edit(ctx, &fs.EditRequest{FilePath: "adapter-test.txt", OldString: "not-present", NewString: "x"}); !errors.As(err, &invalid) {
		t.Fatalf("unmatched edit: %v", err)
	}
}
