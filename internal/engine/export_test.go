package engine

import (
	"encoding/json"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
)

func ModelBoundaryForTest(t *testing.T, store *durable.Store, run *durable.Run, base model.BaseChatModel) model.BaseChatModel {
	t.Helper()
	execution := &execution{store: store, run: run}
	if err := json.Unmarshal(run.State, &execution.state); err != nil {
		t.Fatal(err)
	}
	execution.state.Version = 3
	execution.bindings = map[string]Binding{"lookup": {Policy: durable.ReadOnly}}
	return &boundaryModel{BaseChatModel: base, execution: execution}
}
