package upstream

import (
	"context"
	"testing"
	"time"

	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/limiter"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type delayedRateLimitCache struct {
	delay    time.Duration
	statuses []*pb.RateLimitResponse_DescriptorStatus
}

var _ limiter.RateLimitCache = (*delayedRateLimitCache)(nil)

func (c *delayedRateLimitCache) DoLimit(context.Context, *pb.RateLimitRequest, []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.statuses
}

func (c *delayedRateLimitCache) Flush() {}

type panicRateLimitCache struct{}

var _ limiter.RateLimitCache = (*panicRateLimitCache)(nil)

func (c *panicRateLimitCache) DoLimit(context.Context, *pb.RateLimitRequest, []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {
	panic("EOF")
}

func (c *panicRateLimitCache) Flush() {}

func histogramSampleCount(t *testing.T, hv *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	obs, err := hv.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	m, ok := obs.(prometheus.Metric)
	require.True(t, ok)
	pbMetric := &dto.Metric{}
	require.NoError(t, m.Write(pbMetric))
	return pbMetric.GetHistogram().GetSampleCount()
}

func newTestBudget(t *testing.T) *RateLimiterBudget {
	t.Helper()
	logger := zerolog.Nop()
	cfg := &common.RateLimiterConfig{
		Store: &common.RateLimitStoreConfig{Driver: "memory"},
		Budgets: []*common.RateLimitBudgetConfig{
			{
				Id: "test-budget",
				Rules: []*common.RateLimitRuleConfig{
					{
						Method:   "eth_test",
						MaxCount: 100,
						Period:   common.RateLimitPeriodSecond,
					},
				},
			},
		},
	}
	registry, err := NewRateLimitersRegistry(context.Background(), cfg, &logger)
	require.NoError(t, err)
	budget, err := registry.GetBudget("test-budget")
	require.NoError(t, err)
	require.NotNil(t, budget)
	return budget
}

func TestRateLimiterBudget_PermitTimingMetrics_Ok(t *testing.T) {
	budget := newTestBudget(t)

	beforeEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"ok",
	)
	beforeWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"ok",
	)

	ok, err := budget.TryAcquirePermit(context.Background(), "", nil, "eth_test", "", "", "", "")
	require.NoError(t, err)
	assert.True(t, ok)

	afterEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"ok",
	)
	afterWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"ok",
	)

	assert.GreaterOrEqual(t, afterEval-beforeEval, uint64(1))
	assert.GreaterOrEqual(t, afterWait-beforeWait, uint64(1))
}

func TestRateLimiterBudget_PermitTimingMetrics_TimeoutFailOpen(t *testing.T) {
	budget := newTestBudget(t)
	projectID := "project-a"
	budget.maxTimeout = 10 * time.Millisecond
	budget.registry.cacheMu.Lock()
	budget.registry.envoyCache = &delayedRateLimitCache{
		delay:    40 * time.Millisecond,
		statuses: []*pb.RateLimitResponse_DescriptorStatus{},
	}
	budget.registry.cacheMu.Unlock()

	beforeEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"timeout_fail_open",
	)
	beforeWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"timeout_fail_open",
	)
	ok, err := budget.TryAcquirePermit(context.Background(), projectID, nil, "eth_test", "", "", "", "")
	require.NoError(t, err)
	assert.True(t, ok)

	afterEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"timeout_fail_open",
	)
	afterWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"timeout_fail_open",
	)
	assert.GreaterOrEqual(t, afterEval-beforeEval, uint64(1))
	assert.GreaterOrEqual(t, afterWait-beforeWait, uint64(1))
}

func TestRateLimiterBudget_PermitTimingMetrics_PanicFailOpen(t *testing.T) {
	budget := newTestBudget(t)
	telemetry.MetricNetworkAttemptReasonTotal.Reset()
	projectID := "project-a"
	budget.registry.cfg.Store = &common.RateLimitStoreConfig{
		Driver: "redis",
		Redis:  &common.RedisConnectorConfig{URI: "redis://test"},
	}
	budget.registry.initializer = util.NewInitializer(context.Background(), budget.logger, &util.InitializerConfig{
		TaskTimeout:   time.Second,
		AutoRetry:     false,
		RetryFactor:   1,
		RetryMinDelay: time.Second,
		RetryMaxDelay: time.Second,
	})
	require.NoError(t, budget.registry.initializer.ExecuteTasks(
		context.Background(),
		util.NewBootstrapTask(redisRateLimiterConnectTaskName, func(context.Context) error { return nil }),
	))
	budget.registry.cacheMu.Lock()
	budget.registry.envoyCache = &panicRateLimitCache{}
	budget.registry.cacheMu.Unlock()

	beforeEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"panic_fail_open",
	)
	beforeWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"panic_fail_open",
	)
	ok, err := budget.TryAcquirePermit(context.Background(), projectID, nil, "eth_test", "", "", "", "")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Nil(t, budget.registry.GetCache())

	status := budget.registry.initializer.Status()
	require.Len(t, status.Tasks, 1)
	assert.Equal(t, util.TaskFailed, status.Tasks[0].State)
	assert.Error(t, status.Tasks[0].Err)

	afterEval := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitEvaluationDuration,
		"test-budget",
		"eth_test",
		"",
		"panic_fail_open",
	)
	afterWait := histogramSampleCount(
		t,
		telemetry.MetricRateLimiterPermitWaitDuration,
		"test-budget",
		"eth_test",
		"",
		"panic_fail_open",
	)
	assert.GreaterOrEqual(t, afterEval-beforeEval, uint64(1))
	assert.GreaterOrEqual(t, afterWait-beforeWait, uint64(1))
}

func TestNormalizeRateLimitMethodLabel(t *testing.T) {
	assert.Equal(t, "*", normalizeRateLimitMethodLabel(""))
	assert.Equal(t, "eth_getLogs", normalizeRateLimitMethodLabel("eth_getLogs"))
}
