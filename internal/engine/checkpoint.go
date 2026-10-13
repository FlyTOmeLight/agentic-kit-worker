package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/adk"
)

// State is a storage envelope. Agent messages, pending tools and iteration state
// belong to the opaque Eino checkpoint, not a second platform execution loop.
type State struct {
	Version     int    `json:"version"`
	Checkpoint  []byte `json:"checkpoint,omitempty"`
	Turn        int    `json:"turn"`
	WaitingStep string `json:"waitingStep,omitempty"`
}

type execution struct {
	adk.BaseChatModelAgentMiddleware
	store       *durable.Store
	run         *durable.Run
	state       State
	bindings    map[string]Binding
	decision    *durable.Message
	pauseStatus durable.Status
	detail      string
	retry       bool
	toolError   error
}

func (s *execution) encode() json.RawMessage {
	b, err := json.Marshal(s.state)
	if err != nil {
		panic(err)
	}
	return b
}

func (s *execution) fail(ctx context.Context, err error) error {
	return s.store.Save(ctx, s.run.Lease(), s.encode(), durable.Failed, "", err.Error(), "RunFailed")
}

func (s *execution) Get(_ context.Context, id string) ([]byte, bool, error) {
	if id != s.run.ID {
		return nil, false, errors.New("checkpoint run mismatch")
	}
	return s.state.Checkpoint, len(s.state.Checkpoint) > 0, nil
}

func (s *execution) Set(ctx context.Context, id string, checkpoint []byte) error {
	if id != s.run.ID {
		return errors.New("checkpoint run mismatch")
	}
	s.state.Checkpoint = checkpoint
	if s.decision != nil {
		if err := s.store.SaveDecision(ctx, s.run.Lease(), s.encode(), *s.decision); err != nil {
			return err
		}
		s.decision = nil
		return nil
	}
	if s.pauseStatus == "" {
		return errors.New("unexpected Eino checkpoint without a decision or pause")
	}
	kind := "RunNeedsReview"
	if s.pauseStatus == durable.WaitingClarification {
		kind = "ClarificationRequested"
	}
	return s.store.Save(ctx, s.run.Lease(), s.encode(), s.pauseStatus, "", s.detail, kind)
}
