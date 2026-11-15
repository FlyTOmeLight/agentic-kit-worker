package sandbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
)

type renewalClient struct {
	sdk.SandboxAPI
	renew func(context.Context, *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error)
}

func (client renewalClient) Renew(ctx context.Context, req *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
	return client.renew(ctx, req)
}

func TestRenewUsesAcceptedDeadline(t *testing.T) {
	initialDeadline := time.Now().Add(900 * time.Millisecond)
	acceptedDeadline := initialDeadline
	calls := 0
	stopError := errors.New("authority lost")
	failures := make(chan error, 9)
	backend := &Backend{
		cfg:       Config{AcquireKey: "run:env:1"},
		sandboxID: "sandbox",
		revision:  1,
		expiresAt: initialDeadline,
		renewDone: make(chan struct{}),
	}
	backend.client = renewalClient{
		renew: func(ctx context.Context, req *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
			calls++
			if !time.Now().Before(acceptedDeadline) {
				failures <- errors.New("renewal arrived after accepted deadline")
			}
			if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(acceptedDeadline) {
				failures <- fmt.Errorf("request deadline = %v, want %v", deadline, acceptedDeadline)
			}
			if req.ID != "sandbox" || req.Revision != int64(calls+1) ||
				req.RequestKey != fmt.Sprintf("run:env:1:renew:%d", calls+1) {
				failures <- fmt.Errorf("unexpected renewal: %+v", req)
			}
			if calls == 3 {
				return nil, stopError
			}
			acceptedDeadline = time.Now().Add(time.Duration(4-calls) * 300 * time.Millisecond)
			return &sdk.SandboxLease{Revision: req.Revision, HardExpiresAt: acceptedDeadline}, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go backend.renewLoop(ctx)
	select {
	case <-backend.renewDone:
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not finish within the short accepted leases")
	}
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if calls != 3 || backend.revision != 3 {
		t.Fatalf(
			"renew calls=%d revision=%d; want 3 attempts and revision 3 after failure",
			calls,
			backend.revision,
		)
	}
}

func TestRenewStopsAtExpiry(t *testing.T) {
	backend := &Backend{
		sandboxID: "sandbox",
		expiresAt: time.Now().Add(-time.Second),
		renewDone: make(chan struct{}),
		client: renewalClient{
			renew: func(context.Context, *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
				t.Error("expired lease must not be renewed")
				return nil, nil
			},
		},
	}
	backend.renewLoop(context.Background())
}

func TestRenewCancellationStopsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	backend := &Backend{
		sandboxID: "sandbox",
		expiresAt: time.Now().Add(900 * time.Millisecond),
		renewDone: make(chan struct{}),
		client: renewalClient{
			renew: func(ctx context.Context, _ *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go backend.renewLoop(ctx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal request did not start")
	}
	cancel()
	select {
	case <-backend.renewDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation did not stop the in-flight renewal")
	}
}

func TestDetachStopsRenewalWithoutReleasingSandbox(t *testing.T) {
	started := make(chan struct{})
	renewCtx, stopRenew := context.WithCancel(context.Background())
	defer stopRenew()
	backend := &Backend{
		sandboxID: "sandbox",
		expiresAt: time.Now().Add(900 * time.Millisecond),
		stopRenew: stopRenew,
		renewDone: make(chan struct{}),
		client: renewalClient{
			renew: func(ctx context.Context, _ *sdk.RenewSandboxRequest) (*sdk.SandboxLease, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}
	go backend.renewLoop(renewCtx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal request did not start")
	}
	detached := make(chan struct{})
	go func() {
		backend.Detach()
		close(detached)
	}()
	select {
	case <-detached:
	case <-time.After(time.Second):
		t.Fatal("detach did not stop the in-flight renewal")
	}
	if backend.Released() {
		t.Fatal("detach must not mark the sandbox released")
	}
	select {
	case <-backend.renewDone:
	default:
		t.Fatal("detach returned before renewal exited")
	}
	backend.Detach()
	if _, err := backend.executeScript(WithOperationID(context.Background(), "op"), "command", "true"); err == nil {
		t.Fatal("detached backend accepted a new command")
	}
}
