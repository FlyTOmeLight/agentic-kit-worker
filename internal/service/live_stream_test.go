package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

func TestLivePublisherBatchesBacklogAndFlushes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var writes []durable.StreamDelta
	publisher := startTestPublisher(t, func(ctx context.Context, delta durable.StreamDelta) error {
		writes = append(writes, delta)
		if len(writes) == 1 {
			close(writeStarted)
			select {
			case <-releaseWrite:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}, func(_ context.Context, format string, args ...any) {
		t.Errorf(format, args...)
	})
	reasoning, content := durable.EventModelReasoningDelta, durable.EventModelContentDelta
	at := time.Now().UTC()
	first := durable.StreamDelta{RunID: "run", Epoch: 2, Turn: 0, Kind: reasoning, Delta: "A", CreatedAt: at}
	publisher.Publish(first)
	select {
	case <-writeStarted:
	case <-ctx.Done():
		t.Fatal("no immediate write without backlog")
	}
	for _, delta := range []durable.StreamDelta{
		{RunID: "run", Epoch: 2, Kind: reasoning, Offset: 1, Delta: "想", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: reasoning, Offset: 4, Delta: "法", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: content, Offset: 0, Delta: "答", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: content, Offset: 3, Delta: "案", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: content, Offset: 9, Delta: "gap", CreatedAt: at},
		{RunID: "run", Epoch: 2, Turn: 1, Kind: content, Offset: 0, Delta: "next", CreatedAt: at},
		{RunID: "run", Epoch: 3, Turn: 1, Kind: content, Offset: 4, Delta: "epoch", CreatedAt: at},
		{RunID: "other", Epoch: 3, Turn: 1, Kind: content, Offset: 9, Delta: "run", CreatedAt: at},
	} {
		publisher.Publish(delta)
	}
	close(releaseWrite)
	publisher.Flush(ctx)
	select {
	case <-publisher.done:
	case <-ctx.Done():
		t.Fatal("flush returned before the queue drained")
	}
	want := []durable.StreamDelta{
		first,
		{RunID: "run", Epoch: 2, Kind: reasoning, Offset: 1, Delta: "想法", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: content, Offset: 0, Delta: "答案", CreatedAt: at},
		{RunID: "run", Epoch: 2, Kind: content, Offset: 9, Delta: "gap", CreatedAt: at},
		{RunID: "run", Epoch: 2, Turn: 1, Kind: content, Offset: 0, Delta: "next", CreatedAt: at},
		{RunID: "run", Epoch: 3, Turn: 1, Kind: content, Offset: 4, Delta: "epoch", CreatedAt: at},
		{RunID: "other", Epoch: 3, Turn: 1, Kind: content, Offset: 9, Delta: "run", CreatedAt: at},
	}
	if !reflect.DeepEqual(writes, want) {
		t.Fatalf("writes=%+v want=%+v", writes, want)
	}
}

func TestLivePublisherDropsFullQueueAndCancelsSlowWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	started := make(chan struct{})
	var logs []string
	publisher := startTestPublisher(t, func(ctx context.Context, _ durable.StreamDelta) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > liveWriteTimeout {
			t.Error("Redis write has no bounded deadline")
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, func(_ context.Context, format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	first := durable.StreamDelta{RunID: "run", Epoch: 1, Kind: durable.EventModelContentDelta, Delta: "x"}
	publisher.Publish(first)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("write did not start")
	}
	queued := make(chan struct{})
	go func() {
		defer close(queued)
		for offset := 1; offset <= liveQueueCapacity+1; offset++ {
			delta := first
			delta.Offset = int64(offset)
			publisher.Publish(delta)
		}
	}()
	select {
	case <-queued:
	case <-ctx.Done():
		t.Fatal("slow Redis blocked Publish")
	}
	flushCtx, stopFlush := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopFlush()
	publisher.Flush(flushCtx)
	if !errors.Is(flushCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("flush returned before the deadline with an in-flight write: %v", flushCtx.Err())
	}
	select {
	case <-publisher.done:
	case <-ctx.Done():
		t.Fatal("deadline left a running publisher")
	}
	if len(logs) != 2 || !strings.Contains(logs[0], "context canceled") ||
		!strings.Contains(logs[1], "dropped 1 deltas") {
		t.Fatalf("writes and queue failures must be logged: %v", logs)
	}
}

func startTestPublisher(
	t *testing.T,
	write func(context.Context, durable.StreamDelta) error,
	warn func(context.Context, string, ...any),
) *liveDeltaPublisher {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	publisher := &liveDeltaPublisher{
		ctx: ctx, cancel: cancel, queue: make(chan durable.StreamDelta, liveQueueCapacity),
		done: make(chan struct{}), write: write, warn: warn,
	}
	go publisher.drain()
	t.Cleanup(func() {
		cancel()
		select {
		case <-publisher.done:
		case <-time.After(time.Second):
			t.Error("publisher did not exit during cleanup")
		}
	})
	return publisher
}
