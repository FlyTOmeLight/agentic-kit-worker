package engine

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-api/pkg/durable"
	executionspec "github.com/FlyTOmeLight/agentic-kit-api/pkg/execution"
)

func TestFrozenPricingValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*durable.Spec)
		wantErr bool
	}{
		{name: "valid"},
		{name: "token only", change: func(spec *durable.Spec) { spec.PricingSnapshot = nil }},
		{name: "legacy", change: func(spec *durable.Spec) { spec.PricingFrozen = false }, wantErr: true},
		{name: "empty version", change: func(spec *durable.Spec) { spec.PricingSnapshot.Version = " \t" }, wantErr: true},
		{name: "newline version", change: func(spec *durable.Spec) { spec.PricingSnapshot.Version = "v1\nv2" }, wantErr: true},
		{name: "carriage return version", change: func(spec *durable.Spec) { spec.PricingSnapshot.Version = "v1\rv2" }, wantErr: true},
		{name: "model mismatch", change: func(spec *durable.Spec) { spec.PricingSnapshot.Model = "other" }, wantErr: true},
		{name: "endpoint mismatch", change: func(spec *durable.Spec) { spec.PricingSnapshot.BaseURL = "https://other/v1" }, wantErr: true},
		{name: "negative input", change: func(spec *durable.Spec) { spec.PricingSnapshot.InputPerMillion = -1 }, wantErr: true},
		{name: "nan output", change: func(spec *durable.Spec) { spec.PricingSnapshot.OutputPerMillion = math.NaN() }, wantErr: true},
		{name: "infinite cached", change: func(spec *durable.Spec) { value := math.Inf(1); spec.PricingSnapshot.CachedInputPerMillion = &value }, wantErr: true},
		{name: "negative cached", change: func(spec *durable.Spec) { value := -1.0; spec.PricingSnapshot.CachedInputPerMillion = &value }, wantErr: true},
		{name: "free", change: func(spec *durable.Spec) {
			spec.PricingSnapshot.InputPerMillion = 0
			spec.PricingSnapshot.OutputPerMillion = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := durable.Spec{
				PricingFrozen:   true,
				Model:           durable.ModelSpec{Name: "model", BaseURL: "https://provider/v1"},
				PricingSnapshot: &executionspec.PricingSnapshot{Version: "v1", Model: "model", BaseURL: "https://provider/v1", InputPerMillion: 1, OutputPerMillion: 2},
			}
			if test.change != nil {
				test.change(&spec)
			}
			run := &execution{run: &durable.Run{Spec: spec}}
			if err := run.checkPricing(context.Background()); (err != nil) != test.wantErr || (err != nil && !errors.Is(err, ErrModelPricingUnavailable)) {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}
