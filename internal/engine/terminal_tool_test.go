package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

// Delivery closes the Coding round. Asking the model for another tool after it
// succeeds reproduces the user's late read_file -> STALE_EXECUTION failure.
func TestReturnDirectDeliveryDoesNotInvokeAnotherModelOrTool(t *testing.T) {
	s, _ := testDB(t)
	ctx := context.Background()
	r := submit(t, s, "deliver", durable.Unsafe)
	claimed := getClaim(t, s)
	var configured durable.ToolSpec
	if err := json.Unmarshal(
		[]byte(
			`{"name":"deliver","returnDirectly":true,"policy":"Unsafe","url":"http://localhost/deliver","parameters":{"type":"object"}}`,
		),
		&configured,
	); err != nil {
		t.Fatal(err)
	}
	claimed.Spec.Tools = []durable.ToolSpec{
		configured,
		{
			Name:       "read_file",
			Policy:     durable.ReadOnly,
			URL:        "http://localhost/read_file",
			Parameters: json.RawMessage(`{"type":"object"}`),
		},
	}
	var modelCalls, deliveries, lateReads int
	const delivered = `{"status":"Passed","head":"0123456789012345678901234567890123456789"}`
	e := &engine.Engine{
		Store: s,
		Build: func(context.Context, durable.Spec) (model.ToolCallingChatModel, map[string]engine.Binding, error) {
			return &fakeModel{generate: func(context.Context, []*schema.Message) (*schema.Message, error) {
				modelCalls++
				if deliveries > 0 {
					return toolReply("read_file"), nil
				}
				return toolReply("deliver"), nil
			}}, map[string]engine.Binding{
				"deliver": {
					Policy: durable.Unsafe,
					Tool: &fakeTool{
						name:   "deliver",
						invoke: func(context.Context, string) (string, error) { deliveries++; return delivered, nil },
					},
				},
				"read_file": {
					Policy: durable.ReadOnly,
					Tool: &fakeTool{
						name: "read_file",
						invoke: func(context.Context, string) (string, error) {
							lateReads++
							return "", errors.New(
								"tool read_file returned HTTP 409: STALE_EXECUTION: 本轮执行已变化",
							)
						},
					},
				},
			}, nil
		},
	}
	if err := e.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	done, err := s.Get(ctx, "owner", r.ID)
	if err != nil || done.Status != durable.Completed || done.Output != delivered || modelCalls != 1 ||
		deliveries != 1 ||
		lateReads != 0 {
		t.Fatalf(
			"status=%s output=%s error=%s modelCalls=%d deliveries=%d lateReads=%d err=%v",
			done.Status,
			done.Output,
			done.Error,
			modelCalls,
			deliveries,
			lateReads,
			err,
		)
	}
}
