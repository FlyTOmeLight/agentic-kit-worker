package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/contextbuilder"
	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	executionpkg "github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// State is a storage envelope. Agent messages and the ReAct loop belong to the
// opaque Eino checkpoint; these fields track only execution budgets, waiting
// state and the durable terminal-tool result.

// InflightModel records that a model or summary request has been dispatched
// but its usage has not been durably recorded. A replacement worker finding
// this marker must block the run instead of re-dispatching, because the
// original call may have consumed tokens whose cost is unknown.
type InflightModel = executionpkg.InflightModelDispatch

type State struct {
	Version            int             `json:"version"`
	Checkpoint         []byte          `json:"checkpoint,omitempty"`
	Turn               int             `json:"turn"`
	SummaryCalls       int64           `json:"summaryCalls,omitempty"`
	InflightModel      *InflightModel  `json:"inflightModel,omitempty"`
	PendingModel       *schema.Message `json:"pendingModel,omitempty"`
	PricingFailure     string          `json:"pricingFailure,omitempty"`
	ReturnDirectlyStep string          `json:"returnDirectlyStep,omitempty"`
	Final              *string         `json:"final,omitempty"`
	Steers             []string        `json:"steers,omitempty"`
}

type execution struct {
	adk.BaseChatModelAgentMiddleware
	store              *durable.Store
	run                *durable.Run
	state              State
	bindings           map[string]Binding
	decision           *durable.Message
	pending            *durable.Boundary
	retry              bool
	toolError          error
	summarizer         Summarizer
	compactionMaxBytes int
	snapshot           *contextbuilder.Snapshot
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

// block parks the run when the worker cannot safely continue. The durable
// store releases the lease and blocks the associated goal so a human can
// inspect the unknown side effects before deciding to retry.
func (s *execution) block(ctx context.Context, detail string) error {
	return s.store.CommitBoundary(ctx, s.run.Lease(), durable.Boundary{
		Kind:   durable.BoundaryBlock,
		State:  s.encode(),
		Detail: detail,
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
		decision := *s.decision
		pending := s.state.PendingModel
		s.state.PendingModel = nil
		if err := s.store.RecordDecision(ctx, s.run.Lease(), s.encode(), decision); err != nil {
			s.state.PendingModel = pending
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
