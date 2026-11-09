package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cloudwego/eino/adk"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
)

// State is a storage envelope. Agent messages and the ReAct loop belong to the
// opaque Eino checkpoint; these fields track only execution budgets, waiting
// state and the durable terminal-tool result.
type State struct {
	Version            int     `json:"version"`
	Checkpoint         []byte  `json:"checkpoint,omitempty"`
	Turn               int     `json:"turn"`
	ReturnDirectlyStep string  `json:"returnDirectlyStep,omitempty"`
	Final              *string `json:"final,omitempty"`
}

type execution struct {
	adk.BaseChatModelAgentMiddleware
	store     *durable.Store
	run       *durable.Run
	state     State
	bindings  map[string]Binding
	decision  *durable.Message
	pending   *durable.Boundary
	retry     bool
	toolError error
	snapshot  *contextbuilder.Snapshot
}

func (s *execution) encode() json.RawMessage {
	b, err := json.Marshal(s.state)
	if err != nil {
		panic(err)
	}
	return b
}

func (s *execution) fail(ctx context.Context, err error) error {
	return s.store.CommitBoundary(ctx, s.run.Lease(), durable.Boundary{
		Kind:   durable.BoundaryFail,
		State:  s.encode(),
		Detail: err.Error(),
	})
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
		if err := s.store.RecordDecision(ctx, s.run.Lease(), s.encode(), *s.decision); err != nil {
			return err
		}
		s.decision = nil
		return nil
	}
	if s.pending == nil {
		return errors.New("unexpected Eino checkpoint without a decision or pause")
	}
	// A later clarification or review owns the next model turn; clear any
	// provisional direct-tool result so it cannot bypass the human answer.
	s.state.Final = nil
	pause := *s.pending
	pause.State = s.encode()
	return s.store.CommitBoundary(ctx, s.run.Lease(), pause)
}
