package engine

import (
	"context"
	"io"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
)

type claimSandboxClient struct {
	sdk.SandboxAPI

	requests []*sdk.ExecSandboxRequest
	errors   []error
	op       *sdk.SandboxOperation
}

func (c *claimSandboxClient) Exec(_ context.Context, req *sdk.ExecSandboxRequest) (*sdk.SandboxOperation, error) {
	c.requests = append(c.requests, req)
	if len(c.errors) >= len(c.requests) {
		return nil, c.errors[len(c.requests)-1]
	}
	return c.op, nil
}

func TestHTTPSandboxRunnerPassesEvaluationClaim(t *testing.T) {
	zero := 0
	receipt := &sdk.SandboxOperation{
		ID: "op-original", SandboxID: "sb-1", RequestKey: "eval-sb-1",
		Status: sdk.SandboxOpSucceeded, ExitCode: &zero,
	}
	client := &claimSandboxClient{errors: []error{io.ErrUnexpectedEOF}, op: receipt}
	ctx := withEvaluationClaim(context.Background(), &durable.Run{
		ID: "run-1", EvaluationClaimWorkerID: "worker-2", EvaluationEpoch: 7,
	})
	runner := HTTPSandboxRunner{Client: client}
	if _, err := runner.RunCommand(ctx, "sb-1", "true"); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 2 {
		t.Fatalf("got %d requests, want original plus replay", len(client.requests))
	}
	for name, req := range map[string]*sdk.ExecSandboxRequest{
		"original": client.requests[0], "replay": client.requests[1],
	} {
		if req.EvaluationWorkerID != "worker-2" || req.EvaluationEpoch != 7 {
			t.Fatalf(
				"%s request claim = %q/%d, want worker-2/7",
				name,
				req.EvaluationWorkerID,
				req.EvaluationEpoch,
			)
		}
		if req.RequestKey != "eval-sb-1" || req.Script != "true" {
			t.Fatalf("%s request changed: %+v", name, req)
		}
	}
}

func TestHTTPSandboxRunnerWithoutEvaluationClaim(t *testing.T) {
	zero := 0
	client := &claimSandboxClient{op: &sdk.SandboxOperation{
		ID: "op-original", SandboxID: "sb-1", RequestKey: "eval-sb-1",
		Status: sdk.SandboxOpSucceeded, ExitCode: &zero,
	}}
	runner := HTTPSandboxRunner{Client: client}
	if _, err := runner.RunCommand(context.Background(), "sb-1", "true"); err != nil {
		t.Fatal(err)
	}
	if got := client.requests[0]; got.EvaluationWorkerID != "" || got.EvaluationEpoch != 0 {
		t.Fatalf("unexpected claim without context: %+v", got)
	}
}
