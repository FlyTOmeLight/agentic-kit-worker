package engine_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/redis/go-redis/v9"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func TestWorkerLiveModelSSE(t *testing.T) {
	store, dsn := testDB(t)
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL required")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	cache := redis.NewClient(options)
	t.Cleanup(func() { _ = cache.Close() })
	if err := cache.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	redisConfig := fmt.Sprintf("\n[Redis]\nAddrs = [%q]\nUsername = %q\nPassword = %q\nDB = %d\n",
		options.Addr, options.Username, options.Password, options.DB)
	apiURL := startAPI(t, buildAPI(t), dsn, redisConfig)
	workerBinary := buildWorker(t)
	for _, scenario := range []struct {
		name, redis string
		live        bool
		stallWrites bool
	}{
		{name: "online", redis: redisConfig, live: true},
		{name: "offline", redis: "\n[Redis]\nAddrs = [\"127.0.0.1:1\"]\n"},
		{name: "slow write", stallWrites: true},
		{name: "unconfigured"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			workerRedis := scenario.redis
			var slowWrite <-chan struct{}
			if scenario.stallWrites {
				address, writing := startSlowRedisProxy(t, options.Addr)
				workerRedis = strings.Replace(
					redisConfig,
					fmt.Sprintf("%q", options.Addr),
					fmt.Sprintf("%q", address),
					1,
				)
				slowWrite = writing
			}
			releaseUsage := make(chan struct{})
			var modelCalls, toolCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/tool" {
					toolCalls.Add(1)
					assertSavedToolDecision(t, r.Context(), store, r.Header.Get("X-Agentic-Run-ID"))
					fmt.Fprint(w, `{"receipt":"saved"}`)
					return
				}
				if modelCalls.Add(1) == 1 && scenario.live {
					writeToolModelStream(t, w, r)
					return
				}
				checkStreamRequest(t, r)
				startModelStream(w)
				writeModelChunk(w, `{"role":"assistant","reasoning_content":"先"}`)
				writeModelChunk(w, `{"reasoning_content":"想"}`)
				writeModelChunk(w, `{"content":"答"}`)
				writeModelChunk(w, `{"content":"案"}`)
				if scenario.live {
					select {
					case <-releaseUsage:
					case <-r.Context().Done():
						return
					}
				}
				if scenario.stallWrites {
					select {
					case <-slowWrite:
					case <-r.Context().Done():
						return
					}
				}
				finishModelStream(w, "stop", modelStreamUsage)
			}))
			t.Cleanup(upstream.Close)
			spec := durable.Spec{
				AgentID: "agent", ConfigVersion: "v1", Query: "do work", MaxTurns: 4,
				Model: durable.ModelSpec{Name: "test-model", BaseURL: upstream.URL + "/v1"},
			}
			if scenario.live {
				spec.Tools = []durable.ToolSpec{{
					Name: "submit", URL: upstream.URL + "/tool", Policy: durable.ReadOnly,
					Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
				}}
			}
			session := configureConversation(t, apiURL, spec)
			run := submitMessage(t, ctx, apiURL, session.ID, scenario.name)
			liveKey := durable.LiveStreamKey(run.ID)
			t.Cleanup(func() {
				if err := cache.Del(context.Background(), liveKey, durable.StreamKey(run.ID)).
					Err(); err != nil {
					t.Error(err)
				}
			})
			workerAddress := availableAddress(t)
			worker := startWorker(t, workerBinary, dsn, workerAddress, "", workerRedis)
			waitWorkerReady(t, worker, workerAddress)
			request, err := http.NewRequestWithContext(
				ctx,
				http.MethodGet,
				apiURL+"/v1/runs/"+run.ID+"/events",
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			authorizeProcessRequest(request)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("SSE returned %s", response.Status)
			}
			frames := bufio.NewScanner(response.Body)
			reasoningKind, contentKind := durable.EventModelReasoningDelta, durable.EventModelContentDelta
			var reasoning, content string
			wantCalls, wantTools := int32(1), int32(0)
			if scenario.live {
				wantCalls, wantTools = 2, 1
				for content != "答案" {
					kind, id, data := scanRunFrame(t, frames)
					switch kind {
					case reasoningKind:
						if content != "" {
							t.Fatal("reasoning arrived after content")
						}
						reasoning += requireProcessDelta(t, id, kind, data, run.ID, reasoning)
					case contentKind:
						if reasoning != "先想" {
							t.Fatalf("content before full reasoning: %q", reasoning)
						}
						content += requireProcessDelta(t, id, kind, data, run.ID, content)
					}
				}
				current, err := store.Get(ctx, processTestUserID, run.ID)
				if err != nil || current.Status != durable.Running || current.TokensUsed != 3 {
					t.Fatalf(
						"live frames must arrive before final usage: run=%+v err=%v",
						current,
						err,
					)
				}
				close(releaseUsage)
			}
			for {
				kind, id, _ := scanRunFrame(t, frames)
				if kind == "RunCompleted" {
					if !strings.HasPrefix(id, run.ID+":") {
						t.Fatalf("invalid durable cursor: %q", id)
					}
					break
				}
				if kind == reasoningKind || kind == contentKind {
					t.Fatalf("unexpected model delta: %s", kind)
				}
			}
			finished, err := store.Get(ctx, processTestUserID, run.ID)
			if err != nil || finished.Status != durable.Completed || finished.Output != "答案" ||
				finished.TokensUsed != int64(
					wantCalls,
				)*3 || modelCalls.Load() != wantCalls || toolCalls.Load() != wantTools {
				t.Fatalf(
					"run=%+v err=%v model calls=%d tools=%d",
					finished,
					err,
					modelCalls.Load(),
					toolCalls.Load(),
				)
			}
			entries, err := cache.XRange(ctx, liveKey, "-", "+").Result()
			if err != nil || (len(entries) > 0) != scenario.live {
				t.Fatalf("Redis entries=%+v err=%v", entries, err)
			}
			if scenario.live {
				ttl, err := cache.TTL(ctx, liveKey).Result()
				if err != nil || ttl <= 0 || ttl > durable.LiveStreamTTL {
					t.Fatalf("live stream TTL=%s err=%v", ttl, err)
				}
				text := map[string]string{}
				for _, entry := range entries {
					raw, ok := entry.Values[durable.LiveStreamField].(string)
					delta, err := durable.DecodeStreamDelta([]byte(raw))
					if !ok || err != nil || delta.Turn != 1 || delta.Epoch != finished.Epoch ||
						delta.Offset != int64(len(text[delta.Kind])) {
						t.Fatalf("invalid Redis delta: %+v err=%v", delta, err)
					}
					text[delta.Kind] += delta.Delta
				}
				if text[reasoningKind] != reasoning || text[contentKind] != content {
					t.Fatalf("Redis text=%v SSE reasoning=%q content=%q", text, reasoning, content)
				}
			}
			if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := worker.wait(t, 5*time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func startSlowRedisProxy(t *testing.T, upstreamAddress string) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	writing := make(chan struct{})
	var signalWrite sync.Once
	var connections sync.WaitGroup
	connections.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Go(func() {
				defer client.Close()
				stopClient := context.AfterFunc(ctx, func() { _ = client.Close() })
				defer stopClient()
				upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", upstreamAddress)
				if err != nil {
					t.Error(err)
					return
				}
				defer upstream.Close()
				proxyDone := make(chan struct{})
				go func() {
					defer close(proxyDone)
					_, _ = io.Copy(client, upstream)
					_ = client.Close()
				}()
				defer func() {
					_ = client.Close()
					_ = upstream.Close()
					<-proxyDone
				}()
				reader := bufio.NewReader(client)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if strings.EqualFold(line, "eval\r\n") {
						signalWrite.Do(func() { close(writing) })
						_, _ = io.Copy(io.Discard, reader)
						return
					}
					if _, err := io.WriteString(upstream, line); err != nil {
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		connections.Wait()
	})
	return listener.Addr().String(), writing
}

func assertSavedToolDecision(t *testing.T, ctx context.Context, store *durable.Store, runID string) {
	t.Helper()
	run, err := store.Get(ctx, processTestUserID, runID)
	if err != nil {
		t.Error(err)
		return
	}
	var state engine.State
	if err := json.Unmarshal(run.State, &state); err != nil || len(state.Checkpoint) == 0 || run.TokensUsed != 3 {
		t.Errorf("tool started before saving checkpoint and usage: run=%+v err=%v", run, err)
	}
	events, err := store.Events(ctx, processTestUserID, runID, 0)
	if err != nil {
		t.Error(err)
		return
	}
	for _, event := range events {
		if event.Kind == "ModelCompleted" {
			return
		}
	}
	t.Error("tool started before ModelCompleted")
}

func scanRunFrame(t *testing.T, frames *bufio.Scanner) (kind, id, data string) {
	t.Helper()
	for frames.Scan() {
		line := frames.Text()
		if line == "" && kind != "" {
			return kind, id, data
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			kind = value
		case "id":
			id = value
		case "data":
			data = value
		}
	}
	t.Fatalf("missing SSE frame: %v", frames.Err())
	return "", "", ""
}

func requireProcessDelta(t *testing.T, id, kind, data, runID, previous string) string {
	t.Helper()
	delta, err := durable.DecodeStreamDelta([]byte(data))
	if err != nil || id != "" || delta.Kind != kind || delta.RunID != runID ||
		delta.Epoch < 1 || delta.Turn != 1 || delta.Offset != int64(len(previous)) || delta.CreatedAt.IsZero() {
		t.Fatalf("invalid SSE delta (id=%q): %+v, %v", id, delta, err)
	}
	return delta.Delta
}
