package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/cloudwego/eino/schema"
)

var ErrModelPricingUnavailable = errors.New("model pricing unavailable")

// ModelPricing prices one model in USD per million tokens. A zero price is a
// valid free tier; a missing entry is not.
type ModelPricing struct {
	InputPerMillion       float64
	CachedInputPerMillion *float64
	OutputPerMillion      float64
}

func (p ModelPricing) validate(name string) error {
	if name == "" {
		return errors.New("pricing model name is required")
	}
	prices := []float64{p.InputPerMillion, p.OutputPerMillion}
	if p.CachedInputPerMillion != nil {
		prices = append(prices, *p.CachedInputPerMillion)
	}
	for _, price := range prices {
		if price < 0 || math.IsNaN(price) || math.IsInf(price, 0) {
			return fmt.Errorf("pricing for model %s must be a finite non-negative number", name)
		}
	}
	return nil
}

// Cost prices a token usage report in USD. Reasoning tokens are part of
// CompletionTokens and must not be charged twice. PromptTokenDetails.CachedTokens
// larger than PromptTokens is clamped because providers report it inconsistently.
func (p ModelPricing) Cost(usage *schema.TokenUsage) (float64, error) {
	if usage == nil {
		return 0, ErrModelUsageUnavailable
	}
	prompt := usage.PromptTokens
	completion := usage.CompletionTokens
	if prompt+completion == 0 && usage.TotalTokens > 0 {
		return 0, fmt.Errorf("%w: input/output token breakdown is missing", ErrModelPricingUnavailable)
	}
	cached := usage.PromptTokenDetails.CachedTokens
	if cached > prompt {
		cached = prompt
	}
	uncached := prompt - cached
	cachedPrice := p.InputPerMillion
	if p.CachedInputPerMillion != nil {
		cachedPrice = *p.CachedInputPerMillion
	}
	return float64(uncached)/1e6*p.InputPerMillion +
		float64(cached)/1e6*cachedPrice +
		float64(completion)/1e6*p.OutputPerMillion, nil
}

func (s *execution) frozenPricing() (*ModelPricing, error) {
	if !s.run.Spec.PricingFrozen {
		return nil, fmt.Errorf("%w: run has no frozen pricing; resubmit through the API", ErrModelPricingUnavailable)
	}
	snapshot := s.run.Spec.PricingSnapshot
	if snapshot == nil {
		return nil, nil
	}
	if strings.TrimSpace(snapshot.Version) == "" {
		return nil, fmt.Errorf("%w: snapshot version is required", ErrModelPricingUnavailable)
	}
	if strings.ContainsAny(snapshot.Version, "\r\n") {
		return nil, fmt.Errorf("%w: snapshot version must not contain line breaks", ErrModelPricingUnavailable)
	}
	if snapshot.Model != s.run.Spec.Model.Name || snapshot.BaseURL != s.run.Spec.Model.BaseURL {
		return nil, fmt.Errorf("%w: snapshot model or base URL does not match the run", ErrModelPricingUnavailable)
	}
	pricing := &ModelPricing{
		InputPerMillion:       snapshot.InputPerMillion,
		CachedInputPerMillion: snapshot.CachedInputPerMillion,
		OutputPerMillion:      snapshot.OutputPerMillion,
	}
	if err := pricing.validate(snapshot.Model); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrModelPricingUnavailable, err)
	}
	return pricing, nil
}

func (s *execution) usageCost(usage *schema.TokenUsage) (float64, error) {
	if usage == nil {
		return 0, ErrModelUsageUnavailable
	}
	pricing, err := s.frozenPricing()
	if err != nil {
		return 0, err
	}
	if pricing == nil {
		return 0, nil
	}
	return pricing.Cost(usage)
}

func (s *execution) checkPricing(ctx context.Context) error {
	if s.state.PricingFailure != "" {
		return fmt.Errorf("%w: %s", ErrModelPricingUnavailable, s.state.PricingFailure)
	}
	pricing, err := s.frozenPricing()
	if err != nil {
		return err
	}
	if pricing == nil && s.run.GoalID != "" {
		goal, err := s.store.GetGoal(ctx, s.run.OwnerID, s.run.GoalID)
		if err != nil {
			return err
		}
		if goal.Budget.MaxCostUSD > 0 {
			return fmt.Errorf("%w: a cost budget requires a frozen pricing snapshot", ErrModelPricingUnavailable)
		}
	}
	return nil
}
