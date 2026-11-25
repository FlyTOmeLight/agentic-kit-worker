package service

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	frameworkredis "github.com/FlyTOmeLight/server-go/redis"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

const (
	liveQueueCapacity = 256
	liveWriteTimeout  = 2 * time.Second
)

type liveStreamRedis struct {
	config frameworkredis.Config
	client frameworkredis.Redis
	warn   func(context.Context, string, ...any)
	drains sync.WaitGroup
}

func (m *liveStreamRedis) Initialize(ctx context.Context) error {
	m.config.ContextTimeoutEnabled = true
	pingCtx, cancel := context.WithTimeout(ctx, liveWriteTimeout)
	defer cancel()
	client, err := frameworkredis.NewRedis(pingCtx, &m.config)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.warn(ctx, "live model Redis unavailable at startup: %v", err)
		client, err = frameworkredis.NewRedisLazy(&m.config)
		if err != nil {
			return err
		}
	}
	m.client = client
	return nil
}

func (m *liveStreamRedis) Close(context.Context) error {
	m.drains.Wait()
	if m.client == nil {
		return nil
	}
	return m.client.Close()
}

func (m *liveStreamRedis) newPublisher(ctx context.Context) engine.DeltaPublisher {
	ctx, cancel := context.WithCancel(ctx)
	publisher := &liveDeltaPublisher{
		ctx: ctx, cancel: cancel, queue: make(chan durable.StreamDelta, liveQueueCapacity),
		done: make(chan struct{}), write: m.writeDelta, warn: m.warn,
	}
	m.drains.Go(publisher.drain)
	return publisher
}

func (m *liveStreamRedis) writeDelta(ctx context.Context, delta durable.StreamDelta) error {
	encoded, err := durable.EncodeStreamDelta(delta)
	if err != nil {
		return err
	}
	return m.client.Eval(
		ctx, durable.PublishLiveDeltaLua, []string{durable.LiveStreamKey(delta.RunID)},
		durable.LiveStreamMaxLen, string(encoded), int64(durable.LiveStreamTTL/time.Second),
	).Err()
}

type liveDeltaPublisher struct {
	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan durable.StreamDelta
	done    chan struct{}
	dropped atomic.Uint64
	write   func(context.Context, durable.StreamDelta) error
	warn    func(context.Context, string, ...any)
}

func (p *liveDeltaPublisher) Publish(delta durable.StreamDelta) {
	select {
	case p.queue <- delta:
	default:
		p.dropped.Add(1)
	}
}

func (p *liveDeltaPublisher) Flush(ctx context.Context) {
	close(p.queue)
	defer p.cancel()
	select {
	case <-p.done:
	case <-ctx.Done():
	}
}

func (p *liveDeltaPublisher) drain() {
	defer close(p.done)
	defer func() {
		if dropped := p.dropped.Load(); dropped > 0 {
			p.warn(p.ctx, "live model delta queue full: dropped %d deltas", dropped)
		}
	}()
	backlog := make([]durable.StreamDelta, 0, liveQueueCapacity+1)
	for {
		var first durable.StreamDelta
		select {
		case <-p.ctx.Done():
			return
		case delta, ok := <-p.queue:
			if !ok {
				return
			}
			first = delta
		}
		backlog = append(backlog[:0], first)
		for range len(p.queue) {
			backlog = append(backlog, <-p.queue)
		}
		for start := 0; start < len(backlog) && p.ctx.Err() == nil; {
			delta, consumed := mergeLiveDeltas(backlog[start:])
			writeCtx, cancel := context.WithTimeout(p.ctx, liveWriteTimeout)
			err := p.write(writeCtx, delta)
			cancel()
			if err != nil {
				p.warn(p.ctx, "write live model delta for run %s: %v", delta.RunID, err)
			}
			start += consumed
		}
		if p.ctx.Err() != nil {
			return
		}
		clear(backlog)
	}
}

func mergeLiveDeltas(backlog []durable.StreamDelta) (durable.StreamDelta, int) {
	delta := backlog[0]
	var text strings.Builder
	count := 1
	end := delta.Offset + int64(len(delta.Delta))
	for _, next := range backlog[1:] {
		if next.RunID != delta.RunID || next.Epoch != delta.Epoch || next.Turn != delta.Turn ||
			next.Kind != delta.Kind || next.Offset != end {
			break
		}
		if count == 1 {
			text.WriteString(delta.Delta)
		}
		text.WriteString(next.Delta)
		end += int64(len(next.Delta))
		count++
	}
	if count > 1 {
		delta.Delta = text.String()
	}
	return delta, count
}
