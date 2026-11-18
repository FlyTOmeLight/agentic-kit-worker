package engine_test

import (
	"errors"
	"math"
	"testing"

	"github.com/cloudwego/eino/schema"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/engine"
)

func pricePointer(value float64) *float64 { return &value }

func TestModelPricingCost(t *testing.T) {
	t.Parallel()
	pricing := engine.ModelPricing{InputPerMillion: 2, CachedInputPerMillion: pricePointer(0.5), OutputPerMillion: 10}
	tests := []struct {
		name    string
		usage   *schema.TokenUsage
		want    float64
		wantErr error
	}{
		{name: "nil", wantErr: engine.ErrModelUsageUnavailable},
		{name: "empty", usage: &schema.TokenUsage{}},
		{
			name:  "uncached prompt and completion",
			usage: &schema.TokenUsage{PromptTokens: 1_000_000, CompletionTokens: 100_000, TotalTokens: 1_100_000},
			want:  2 + 1,
		},
		{
			name: "cached tokens use cached price",
			usage: &schema.TokenUsage{
				PromptTokens:       1_000_000,
				PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 400_000},
				CompletionTokens:   100_000,
				TotalTokens:        1_100_000,
			},
			want: 600_000.0/1e6*2 + 400_000.0/1e6*0.5 + 1,
		},
		{
			name: "cached clamped to prompt",
			usage: &schema.TokenUsage{
				PromptTokens:       10,
				PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 40},
				CompletionTokens:   5,
				TotalTokens:        15,
			},
			want: 10.0/1e6*0.5 + 5.0/1e6*10,
		},
		{
			name:    "total only cannot be priced",
			usage:   &schema.TokenUsage{TotalTokens: 1_000_000},
			wantErr: engine.ErrModelPricingUnavailable,
		},
		{
			name:  "free model",
			usage: &schema.TokenUsage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
			want:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pricing := pricing
			if test.name == "free model" {
				pricing = engine.ModelPricing{}
			}
			if got, err := pricing.Cost(test.usage); math.Abs(got-test.want) > 1e-12 || !errors.Is(err, test.wantErr) {
				t.Fatalf("cost=%v err=%v, want %v / %v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestCachedPricingDefaultAndFree(t *testing.T) {
	for _, test := range []struct {
		name   string
		cached *float64
		want   float64
	}{
		{name: "omitted", want: 2},
		{name: "explicit free", cached: pricePointer(0), want: 1},
		{name: "discounted", cached: pricePointer(0.5), want: 1.25},
	} {
		t.Run(test.name, func(t *testing.T) {
			pricing := engine.ModelPricing{InputPerMillion: 2, CachedInputPerMillion: test.cached}
			got, err := pricing.Cost(&schema.TokenUsage{
				PromptTokens:       1_000_000,
				PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 500_000},
			})
			if err != nil || got != test.want {
				t.Fatalf("cost=%v err=%v want=%v", got, err, test.want)
			}
		})
	}
}
