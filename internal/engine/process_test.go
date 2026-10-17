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
	request, err := http.NewRequestWithContext(
		ctx,
		"POST",
		apiURL+"/v1/sessions/"+session.ID+"/messages",
		strings.NewReader(`{"query":"do work"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "process-kill")
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var submitted durable.Run
	if err = json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("submit status %d", resp.StatusCode)
	}
	child := exec.Command(workerBinary)
	child.Env = append(
		os.Environ(),
		"DATABASE_URL="+dsn,
		"LISTEN_ADDR=127.0.0.1:0",
		"WORKER_CONCURRENCY=1",
		"WORKER_LEASE=600ms",
		"WORKER_POLL=20ms",
		"CONFIG_FILE=",
		"SERVER_CONFIG_PATH=",
		"GIN_MODE=release",
	)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	select {
	case <-enteredSecondModel:
	case <-ctx.Done():
		t.Fatal("child did not reach saved-tool boundary")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = child.Wait(); err == nil {
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
	request, err = http.NewRequestWithContext(ctx, "GET", apiURL+"/v1/runs/"+result.ID+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(request)
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
	text := fmt.Sprintf(
		"AppName = \"agentic-kit-api\"\nAppCode = 100\nRunMode = \"release\"\n[FakeAuth]\nUserID = \"owner\"\n[DB]\nDriver = \"PostgreSQL\"\nDSN = %q\nMaxOpenConns = 20\nMaxIdleConns = 5\n[HTTPServer]\nListen = %q\nWriteTimeout = -1\n[Redis]\nAddrs = [\"127.0.0.1:1\"]\n",
		dsn,
		address,
	)
	if err := os.WriteFile(configPath, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-config", configPath)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "DATABASE_URL",
			"LISTEN_ADDR",
			"REDIS_URL",
			"CONFIG_FILE",
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

func TestWorkerHealthAndShutdown(t *testing.T) {
	_, dsn := testDB(t)
	binary := buildWorker(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	child := exec.Command(binary)
	child.Env = append(
		os.Environ(),
		"DATABASE_URL="+dsn,
		"LISTEN_ADDR="+address,
		"WORKER_POLL=20ms",
		"CONFIG_FILE=",
		"SERVER_CONFIG_PATH=",
		"GIN_MODE=release",
	)
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var childErr error
	go func() { childErr = child.Wait(); close(done) }()
	defer func() { _ = child.Process.Kill(); <-done }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	ready := false
	for ctx.Err() == nil {
		resp, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		select {
		case <-done:
			t.Fatalf("worker stopped before ready: %v", childErr)
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !ready {
		t.Fatal("worker did not become ready")
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if childErr != nil {
			t.Fatalf("worker shutdown: %v", childErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after SIGTERM")
	}
}
