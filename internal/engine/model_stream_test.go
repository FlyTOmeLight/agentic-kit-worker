package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/adapters"
	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const modelStreamUsage = `{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}`

func checkStreamRequest(t *testing.T, request *http.Request) {
	t.Helper()
	var payload struct {
		Stream        bool `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil ||
		!payload.Stream || !payload.StreamOptions.IncludeUsage {
		t.Errorf("expected a stream request with usage: %+v, %v", payload, err)
	}
}

func startModelStream(response http.ResponseWriter) {
	response.Header().Set("Content-Type", "text/event-stream")
}

func writeModelChunk(response http.ResponseWriter, delta string) {
	fmt.Fprintf(response, "data: {\"id\":\"reply\",\"choices\":[{\"index\":0,\"delta\":%s}]}\n\n", delta)
	response.(http.Flusher).Flush()
}

func finishModelStream(response http.ResponseWriter, reason, usage string) {
	fmt.Fprintf(
		response,
		"data: {\"id\":\"reply\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":%q}]}\n\n",
		reason,
	)
	if usage != "" {
		fmt.Fprintf(response, "data: {\"id\":\"reply\",\"choices\":[],\"usage\":%s}\n\n", usage)
	}
	fmt.Fprint(response, "data: [DONE]\n\n")
}

func writeToolModelStream(t *testing.T, response http.ResponseWriter, request *http.Request) {
	t.Helper()
	checkStreamRequest(t, request)
	startModelStream(response)
	writeModelChunk(
		response,
		`{"role":"assistant","tool_calls":[{"index":0,"id":"remote-tool","type":"function","function":{"name":"submit","arguments":"{"}}]}`,
	)
	writeModelChunk(response, `{"tool_calls":[{"index":0,"function":{"arguments":"}"}}]}`)
	finishModelStream(response, "tool_calls", modelStreamUsage)
}

type capturedDeltas struct {
	deltas  []durable.StreamDelta
	onFlush func(context.Context)
}

func (p *capturedDeltas) Publish(delta durable.StreamDelta) {
	p.deltas = append(p.deltas, delta)
}

func (p *capturedDeltas) Flush(ctx context.Context) {
	if p.onFlush != nil {
		p.onFlush(ctx)
	}
}

func TestHTTPModelStreamResetsIdleTimeout(t *testing.T) {
	store, _ := testDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checkStreamRequest(t, r)
		startModelStream(w)
		writeModelChunk(w, `{"role":"assistant","reasoning_content":"思"}`)
		pulse := time.NewTicker(75 * time.Millisecond)
		defer pulse.Stop()
		for _, delta := range []string{`{"reasoning_content":"考"}`, `{"content":"答"}`, `{"content":"案"}`} {
			select {
			case <-pulse.C:
				writeModelChunk(w, delta)
			case <-r.Context().Done():
				return
			}
		}
		finishModelStream(w, "stop", modelStreamUsage)
	}))
	defer upstream.Close()
	run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
		spec.Model.BaseURL = upstream.URL
	})
	publisher := &capturedDeltas{}
	executor := &engine.Engine{
		Store: store, Build: adapters.Build, StreamIdleTimeout: 200 * time.Millisecond,
		NewDeltaPublisher: func(context.Context) engine.DeltaPublisher { return publisher },
	}
	if err := executor.Execute(ctx, claimForCompression(t, store)); err != nil {
		t.Fatal(err)
	}
	finished, state, _ := compressionResult(t, store, run)
	if finished.Status != durable.Completed || finished.Output != "答案" ||
		finished.TokensUsed != 3 || state.InflightModel != nil || len(publisher.deltas) != 4 {
		t.Fatalf("run=%+v state=%+v deltas=%+v", finished, state, publisher.deltas)
	}
	for index, delta := range publisher.deltas {
		kind := durable.EventModelReasoningDelta
		if index >= 2 {
			kind = durable.EventModelContentDelta
		}
		if delta.Kind != kind || delta.Offset != int64(index%2)*int64(len("思")) {
			t.Fatalf("unexpected delta %d: %+v", index, delta)
		}
	}
}

func TestHTTPModelStreamFailureBlocks(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		sendChunk  bool
		idle       bool
		disconnect bool
	}{
		{name: "idle before headers", idle: true},
		{name: "idle after reasoning", sendChunk: true, idle: true},
		{name: "missing usage", sendChunk: true},
		{name: "disconnect without usage", sendChunk: true, disconnect: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, _ := testDB(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			httpCanceled := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checkStreamRequest(t, r)
				startModelStream(w)
				if scenario.sendChunk {
					writeModelChunk(w, `{"role":"assistant","reasoning_content":"分析"}`)
				}
				if scenario.idle {
					select {
					case <-r.Context().Done():
						close(httpCanceled)
					case <-ctx.Done():
					}
					return
				}
				if scenario.disconnect {
					panic(http.ErrAbortHandler)
				}
				finishModelStream(w, "stop", "")
			}))
			defer upstream.Close()
			run := submit(t, store, "lookup", durable.ReadOnly, func(spec *durable.Spec) {
				spec.Model.BaseURL = upstream.URL
			})
			flushes := 0
			publisher := &capturedDeltas{onFlush: func(flushCtx context.Context) {
				flushes++
				current, err := store.Get(flushCtx, "owner", run.ID)
				if err != nil || current.Status != durable.Running || current.TokensUsed != 0 {
					t.Errorf("flush did not precede durable outcome: run=%+v err=%v", current, err)
				}
				if deadline, ok := flushCtx.Deadline(); !ok ||
					time.Until(deadline) > 200*time.Millisecond {
					t.Error("flush has no bounded deadline")
				}
			}}
			executor := &engine.Engine{
				Store: store, Build: adapters.Build, StreamIdleTimeout: 150 * time.Millisecond,
				NewDeltaPublisher: func(context.Context) engine.DeltaPublisher { return publisher },
			}
			if err := executor.Execute(ctx, claimForCompression(t, store)); err != nil {
				t.Fatal(err)
			}
			finished, state, events := compressionResult(t, store, run)
			expected := engine.ErrModelUsageUnavailable.Error()
			if scenario.idle {
				expected = "model stream idle timeout"
				select {
				case <-httpCanceled:
				case <-ctx.Done():
					t.Fatal("HTTP handler did not observe stream cancellation")
				}
			}
			if finished.Status != durable.Blocked || !strings.Contains(finished.Error, expected) ||
				finished.TokensUsed != 0 || state.InflightModel == nil || state.Turn != 0 ||
				events.counts["ModelOverflowRejected"] != 0 {
				t.Fatalf("run=%+v state=%+v events=%v", finished, state, events.counts)
			}
			if scenario.sendChunk {
				if flushes != 1 || len(publisher.deltas) != 1 ||
					publisher.deltas[0].Kind != durable.EventModelReasoningDelta ||
					publisher.deltas[0].Delta != "分析" {
					t.Fatalf("flushes=%d deltas=%+v", flushes, publisher.deltas)
				}
			} else if len(publisher.deltas) != 0 {
				t.Fatalf("unexpected deltas: %+v", publisher.deltas)
			}
		})
	}
}
