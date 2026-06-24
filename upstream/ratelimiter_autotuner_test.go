package upstream

import (
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestRateLimitAutoTunerSkipsWildcardRulesForCallerMethods(t *testing.T) {
	logger := zerolog.Nop()
	wildcard := &RateLimitRule{Config: &common.RateLimitRuleConfig{
		Method:   "eth_*",
		MaxCount: 100,
		Period:   common.RateLimitPeriodSecond,
	}}
	exact := &RateLimitRule{Config: &common.RateLimitRuleConfig{
		Method:   "eth_call",
		MaxCount: 10,
		Period:   common.RateLimitPeriodSecond,
	}}
	budget := &RateLimiterBudget{
		logger: &logger,
		Id:     "test-budget",
		Rules:  []*RateLimitRule{wildcard, exact},
	}
	tuner := NewRateLimitAutoTuner(
		&logger,
		budget,
		time.Second,
		0.5,
		2.0,
		0.5,
		1,
		1000,
	)

	tuner.mu.Lock()
	counter := tuner.getOrCreateCounter("eth_call")
	counter.totalCount = 10
	counter.errorCount = 10
	tuner.lastAdjustments["eth_call"] = time.Now().Add(-time.Minute)
	tuner.maybeAdjust("eth_call")
	tuner.mu.Unlock()

	assert.Equal(t, uint32(100), wildcard.Config.MaxCount, "caller method must not down-tune shared wildcard rule")
	assert.Equal(t, uint32(5), exact.Config.MaxCount, "exact method rule should still auto-tune")
}
