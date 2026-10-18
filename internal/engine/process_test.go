package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestProcessKillResumesWithoutRepeatingTool(t *testing.T) {
	s, dsn := testDB(t)
	binary := buildAPI(t)
	workerBinary := buildWorker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var modelCalls, toolCalls atomic.Int32
	enteredSecondModel := make(chan struct{}, 1)
	releaseBlocked := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/tool" {
			toolCalls.Add(1)
			if r.Header.Get("Idempotency-Key") == "" {
				t.Error("tool operation ID missing")
			}
			fmt.Fprint(w, `{"receipt":"saved"}`)
			return
		}
		n := modelCalls.Add(1)
		if n == 1 {
			fmt.Fprint(
				w,
				`{"id":"cmpl-1","object":"chat.completion","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":"","tool_calls":[{"id":"remote-tool","type":"function","function":{"name":"submit","arguments":"{}"}}]}}]}`,
			)
			return
		}
		if n == 2 {
			enteredSecondModel <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-releaseBlocked:
			}
			return
		}
		fmt.Fprint(
			w,
			`{"id":"cmpl-2","object":"chat.completion","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"report completed"}}]}`,
		)
	}))
	defer upstream.Close()
	defer close(releaseBlocked)
	spec := durable.Spec{
		AgentID:       "agent",
		ConfigVersion: "v1",
		Query:         "do work",
		MaxTurns:      4,
		Model:         durable.ModelSpec{Name: "test-model", BaseURL: upstream.URL + "/v1"},
		Tools: []durable.ToolSpec{
			{
				Name:       "submit",
				URL:        upstream.URL + "/tool",
				Policy:     durable.Idempotent,
				Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
			},
		},
	}
	apiURL := startAPI(t, binary, dsn)
	session := configureConversation(t, apiURL, spec)
	submitted := submitMessage(t, ctx, apiURL, session.ID, "process-kill")
	child := startWorker(t, workerBinary, dsn, "127.0.0.1:0", "")
	select {
	case <-enteredSecondModel:
	case <-ctx.Done():
		t.Fatal("child did not reach saved-tool boundary")
	}
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.wait(t, 5*time.Second); err == nil {
		t.Fatal("child exited without being killed")
	}
	replacementCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w := engine.Worker{
			Engine:      &engine.Engine{Store: s, Build: adapters.Build},
			ID:          "replacement",
			Concurrency: 2,
			Lease:       600 * time.Millisecond,
			Poll:        20 * time.Millisecond,
		}
		_ = w.Run(replacementCtx)
	}()
	defer func() { stop(); <-done }()
	var result *durable.Run
	var err error
	for ctx.Err() == nil {
		result, err = s.Get(ctx, "owner", submitted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if result == nil || result.Status != durable.Completed || result.Output != "report completed" ||
		result.Epoch != 2 ||
		toolCalls.Load() != 1 {
		t.Fatalf("result=%+v tools=%d models=%d", result, toolCalls.Load(), modelCalls.Load())
	}
	messages, err := s.Messages(ctx, "owner", session.ID, 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 4 || messages[0].Kind != "User" || messages[1].Kind != "Assistant" ||
		messages[2].Kind != "Tool" ||
		messages[3].Kind != "Final" {
		t.Fatalf("messages after recovery: %+v", messages)
	}
	for i, message := range messages {
		if message.Seq != int64(i+1) || message.RunID != submitted.ID {
			t.Fatalf("message identity changed: %+v", message)
		}
	}
	// SSE is replayed from durable events after the worker has died, even with Redis unavailable.
	request, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/v1/runs/"+result.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "RunCompleted") || !strings.Contains(string(body), "report completed") {
		t.Fatalf("missing durable completion: %s", body)
	}
}

func submitMessage(t *testing.T, ctx context.Context, base, session, key string) durable.Run {
	t.Helper()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		base+"/v1/sessions/"+session+"/messages",
		strings.NewReader(`{"query":"do work"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("submit status %d: %s", resp.StatusCode, body)
	}
	var submitted durable.Run
	if err = json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}
	return submitted
}

func buildAPI(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "agentic-kit-api")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lookup := exec.CommandContext(
		ctx,
		"go",
		"list",
		"-m",
		"-f",
		"{{.Dir}}",
		"github.com/FlyTOmeLight/agentic-kit-api",
	)
	dir, err := lookup.Output()
	if err != nil {
		t.Fatalf("locate API module: %v", err)
	}
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/server")
	cmd.Dir = strings.TrimSpace(string(dir))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build API: %v\n%s", err, output)
	}
	return binary
}

func startAPI(t *testing.T, binary, dsn string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.toml")
	text := fmt.Sprintf(`AppName = "agentic-kit-api"
AppCode = 100
RunMode = "release"
[FakeAuth]
UserID = "owner"
[DB]
Driver = "PostgreSQL"
DSN = %q
AutoMigrate = false
MaxOpenConns = 20
MaxIdleConns = 5
ConnMaxIdleTime = 300
[HTTPServer]
Listen = %q
ReadTimeout = 10000
WriteTimeout = -1
IdleTimeout = 60000
[Redis]
Addrs = ["127.0.0.1:1"]
DB = 0
`, dsn, address)
	if err := os.WriteFile(configPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-config", configPath)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "DATABASE_URL",
			"LISTEN_ADDR",
			"REDIS_ADDR",
			"CONFIG_FILE",
			"SERVER_CONFIG_PATH",
			"GIN_MODE":
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "GIN_MODE=release")
	// Redis is intentionally unreachable; the HTTP replay contract must remain usable.
	logPath := filepath.Join(t.TempDir(), "api.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-done; _ = log.Close() })
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return "http://" + address
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = log.Sync()
	output, _ := os.ReadFile(logPath)
	t.Fatalf("API did not become ready: %s", output)
	return ""
}

func configureConversation(t *testing.T, base string, spec durable.Spec) durable.Session {
	t.Helper()
	call := func(path string, input any, output any, want int) {
		t.Helper()
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest("POST", base+path, strings.NewReader(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		client := http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
		if err = json.NewDecoder(resp.Body).Decode(output); err != nil {
			t.Fatal(err)
		}
	}
	var a durable.Agent
	call(
		"/v1/agents",
		map[string]any{
			"name": "test",
			"config": durable.AgentConfig{
				SystemPrompt: spec.SystemPrompt,
				Model:        spec.Model,
				Tools:        spec.Tools,
				MaxTurns:     spec.MaxTurns,
			},
		},
		&a,
		201,
	)
	var v durable.AgentVersion
	call("/v1/agents/"+a.ID+"/publish", map[string]any{"revision": a.Revision}, &v, 200)
	var session durable.Session
	call("/v1/sessions", map[string]any{"agentID": a.ID}, &session, 201)
	return session
}

func buildWorker(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "worker")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/worker")
	cmd.Dir = "../.."
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, output)
	}
	return binary
}

func writeWorkerConfig(t *testing.T, dsn, address string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	text := fmt.Sprintf(`AppName = "agentic-kit-worker"
AppCode = 101
RunMode = "release"
[HTTPServer]
Listen = %q
ReadTimeout = 10000
WriteTimeout = 10000
IdleTimeout = 60000
[DB]
Driver = "PostgreSQL"
DSN = %q
AutoMigrate = false
MaxOpenConns = 20
MaxIdleConns = 5
ConnMaxIdleTime = 300
[Worker]
Concurrency = 1
Lease = "600ms"
Poll = "20ms"
`, address, dsn)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type workerProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startWorker(t *testing.T, binary, dsn, address, logConfig string) *workerProcess {
	t.Helper()
	args := []string{"-config", writeWorkerConfig(t, dsn, address)}
	if logConfig != "" {
		args = append(args, "-log-config", logConfig)
	}
	cmd := exec.Command(binary, args...)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "CONFIG_FILE", "SERVER_CONFIG_PATH", "LOG_CONFIG_FILE", "GIN_MODE":
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "CONFIG_FILE=", "SERVER_CONFIG_PATH=")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	process := &workerProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		process.err = cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			t.Error("worker process did not exit during cleanup")
		}
	})
	return process
}

func (p *workerProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		t.Fatal("worker process did not exit")
		return nil
	}
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitWorkerReady(t *testing.T, process *workerProcess, address string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	for {
		resp, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-process.done:
			t.Fatalf("worker stopped before ready: %v", process.err)
		case <-ctx.Done():
			t.Fatal("worker did not become ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestWorkerHealthAndShutdown(t *testing.T) {
	_, dsn := testDB(t)
	binary := buildWorker(t)
	address := availableAddress(t)
	child := startWorker(t, binary, dsn, address, "")
	waitWorkerReady(t, child, address)
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := child.wait(t, 5*time.Second); err != nil {
		t.Fatalf("worker shutdown: %v", err)
	}
}

func TestWorkerLoggerConfiguration(t *testing.T) {
	_, dsn := testDB(t)
	binary := buildWorker(t)
	t.Run("file output and level filtering", func(t *testing.T) {
		address := availableAddress(t)
		dir := t.TempDir()
		infoPath := filepath.Join(dir, "info.log")
		warnPath := filepath.Join(dir, "warn.log")
		logConfig := filepath.Join(dir, "logger.toml")
		text := fmt.Sprintf(`Level = "Debug"
[[Outputs]]
Type = "File"
Level = "Info"
Path = %q
Rotation = false
[[Outputs]]
Type = "File"
Level = "Warn"
Path = %q
Rotation = false
`, infoPath, warnPath)
		if err := os.WriteFile(logConfig, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		child := startWorker(t, binary, dsn, address, logConfig)
		waitWorkerReady(t, child, address)
		if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if err := child.wait(t, 5*time.Second); err != nil {
			t.Fatalf("worker shutdown: %v", err)
		}
		infoLog, err := os.ReadFile(infoPath)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(infoLog), "route=/healthz") {
			t.Fatalf("file logger missed the real health request: %s", infoLog)
		}
		warnLog, err := os.ReadFile(warnPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(warnLog), "route=/healthz") {
			t.Fatalf("output level did not filter health logs: %s", warnLog)
		}
	})
	t.Run("invalid configuration", func(t *testing.T) {
		configPath := writeWorkerConfig(t, dsn, "127.0.0.1:0")
		logConfig := filepath.Join(t.TempDir(), "logger.toml")
		if err := os.WriteFile(
			logConfig,
			[]byte("Level = \"invalid\"\n[[Outputs]]\nType = \"Console\"\n"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "-config", configPath, "-log-config", logConfig)
		output, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatal("worker accepted an invalid logger level")
		}
		if !strings.Contains(string(output), "invalid") {
			t.Fatalf("invalid logger failure was not reported: %s", output)
		}
	})
}

func TestProcessShutdownCancelsInFlightHTTPTool(t *testing.T) {
	_, dsn := testDB(t)
	binary := buildAPI(t)
	workerBinary := buildWorker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	enteredTool := make(chan struct{}, 1)
	toolCanceled := make(chan struct{}, 1)
	releaseTool := make(chan struct{})
	var toolCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tool" {
			toolCalls.Add(1)
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Errorf("read tool request body: %v", err)
				return
			}
			select {
			case enteredTool <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				select {
				case toolCanceled <- struct{}{}:
				default:
				}
			case <-releaseTool:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(
			w,
			`{"id":"cmpl-1","object":"chat.completion","choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":"","tool_calls":[{"id":"remote-tool","type":"function","function":{"name":"submit","arguments":"{}"}}]}}]}`,
		)
	}))
	defer upstream.Close()
	defer close(releaseTool)
	spec := durable.Spec{
		AgentID:       "agent",
		ConfigVersion: "v1",
		Query:         "do work",
		MaxTurns:      4,
		Model:         durable.ModelSpec{Name: "test-model", BaseURL: upstream.URL + "/v1"},
		Tools: []durable.ToolSpec{
			{
				Name:       "submit",
				URL:        upstream.URL + "/tool",
				Policy:     durable.ReadOnly,
				Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
			},
		},
	}
	apiURL := startAPI(t, binary, dsn)
	session := configureConversation(t, apiURL, spec)
	submitMessage(t, ctx, apiURL, session.ID, "process-shutdown")
	child := startWorker(t, workerBinary, dsn, "127.0.0.1:0", "")
	select {
	case <-enteredTool:
	case <-ctx.Done():
		t.Fatal("worker did not enter the HTTP tool")
	}
	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := child.wait(t, 5*time.Second); err != nil {
		t.Fatalf("worker shutdown: %v", err)
	}
	cancelTimer := time.NewTimer(2 * time.Second)
	defer cancelTimer.Stop()
	select {
	case <-toolCanceled:
	case <-cancelTimer.C:
		t.Fatal("worker stopped without canceling the in-flight HTTP tool")
	}
	if toolCalls.Load() != 1 {
		t.Fatalf("tool calls=%d", toolCalls.Load())
	}
}
