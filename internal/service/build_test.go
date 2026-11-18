package service

import (
	"strings"
	"testing"

	"github.com/FlyTOmeLight/agentic-kit-worker/internal/config"
)

func TestNewEngineRejectsWorkerPricing(t *testing.T) {
	for _, test := range []struct {
		name    string
		pricing config.PricingConfig
	}{
		{name: "omitted"},
		{name: "empty", pricing: config.PricingConfig{}},
		{name: "configured", pricing: config.PricingConfig{"Models": []any{map[string]any{"Name": "test"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			built, err := newEngine(&config.Config{
				Pricing:    test.pricing,
				Compaction: config.CompactionConfig{MaxInputBytes: 32768},
			}, nil)
			if test.pricing != nil {
				if err == nil || !strings.Contains(err.Error(), "no longer accepted") || built != nil {
					t.Fatalf("engine=%+v error=%v", built, err)
				}
				return
			}
			if err != nil || built.CompactionMaxBytes != 32768 {
				t.Fatalf("engine=%+v error=%v", built, err)
			}
		})
	}
}
