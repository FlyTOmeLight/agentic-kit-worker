package sandbox

import (
	"context"
	"testing"
	"time"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	sdk "github.com/FlyTOmeLight/agentic-kit-api/sdk/go/agenticv1"
)

type workspaceAcquireClient struct {
	sdk.SandboxAPI
	view    *sdk.SandboxView
	request *sdk.AcquireSandboxRequest
}

func (client *workspaceAcquireClient) Acquire(
	_ context.Context,
	request *sdk.AcquireSandboxRequest,
) (*sdk.SandboxView, error) {
	client.request = request
	return client.view, nil
}

func TestAcquireRejectsMismatchedWorkspace(t *testing.T) {
	for _, scenario := range []string{"missing response", "ephemeral", "wrong source", "missing checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			view := &sdk.SandboxView{
				Sandbox: &sdk.Sandbox{
					ID: "new", Status: sdk.SandboxStatusActive, RequiredSave: true,
					RestoreFromSandboxID: "previous", ResumeCheckpoint: "checkpoint",
				},
				Lease: &sdk.SandboxLease{HardExpiresAt: time.Now().Add(time.Minute)},
			}
			switch scenario {
			case "missing response":
				view = nil
			case "ephemeral":
				view.Sandbox.RequiredSave = false
			case "wrong source":
				view.Sandbox.RestoreFromSandboxID = "other"
			case "missing checkpoint":
				view.Sandbox.ResumeCheckpoint = ""
			}
			client := &workspaceAcquireClient{view: view}
			backend := &Backend{
				client: client,
				cfg:    Config{RequiredSave: true, RestoreFromSandboxID: "previous"},
			}
			if err := backend.acquire(
				context.Background(),
				&durable.RunEnvironment{RequestKey: "acquire"},
			); err == nil {
				t.Fatal("unconfirmed restore was activated")
			}
			if client.request == nil || !client.request.RequiredSave ||
				client.request.RestoreFromSandboxID != "previous" {
				t.Fatalf("persistence intent was not transmitted: %+v", client.request)
			}
		})
	}
}

func TestClosePreservesUnconfirmedWorkspace(t *testing.T) {
	for _, scenario := range []struct {
		name            string
		status          string
		requiredSave    bool
		centerFinalized bool
		wantReleased    bool
	}{
		{name: "saving", status: sdk.SandboxStatusSaveBlocked, requiredSave: true, centerFinalized: false, wantReleased: false},
		{name: "releasing", status: sdk.SandboxStatusReleasing, requiredSave: true, centerFinalized: false, wantReleased: false},
		{name: "destroyed awaiting center projection", status: sdk.SandboxStatusDestroyed, requiredSave: true, centerFinalized: false, wantReleased: false},
		{name: "center finalized persistent workspace", status: sdk.SandboxStatusDestroyed, requiredSave: true, centerFinalized: true, wantReleased: true},
		{name: "center finalized ephemeral workspace", status: sdk.SandboxStatusDestroyed, requiredSave: false, centerFinalized: true, wantReleased: true},
		{name: "destroyed ephemeral workspace", status: sdk.SandboxStatusDestroyed, requiredSave: false, centerFinalized: false, wantReleased: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store := evaluationStore(t)
			run, env := evaluationEnvironment(t, store, true)
			client := evaluationClient{
				get: func(context.Context, string) (*sdk.SandboxView, error) { return evaluationView(run, env), nil },
				release: func(ctx context.Context, id string) (*sdk.Sandbox, error) {
					if scenario.centerFinalized {
						if err := store.MarkEnvironmentReleased(ctx, env.ID); err != nil {
							return nil, err
						}
					}
					return &sdk.Sandbox{
						ID:           id,
						Status:       scenario.status,
						RequiredSave: scenario.requiredSave,
					}, nil
				},
			}
			backend, err := ResumeEvaluation(context.Background(), store, client, run)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(backend.Detach)
			if err = backend.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			current, err := store.GetEnvironment(context.Background(), run.ID, env.RequestKey)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus := durable.EnvironmentActive
			if scenario.wantReleased {
				wantStatus = durable.EnvironmentReleased
			}
			if current.Status != wantStatus || !backend.Released() {
				t.Fatalf("environment=%+v want=%s", current, wantStatus)
			}
		})
	}
}
