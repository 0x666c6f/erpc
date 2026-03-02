package upstream

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/thirdparty"
	"github.com/erpc/erpc/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	telemetry.SetHistogramBuckets("0.05,0.5,5,30")
}

func getUpsByID(upsList []common.Upstream, ids ...string) []common.Upstream {
	var ups []common.Upstream
	for _, id := range ids {
		for _, u := range upsList {
			if u.Id() == id {
				ups = append(ups, u)
				break
			}
		}
	}
	return ups
}

// ---------------------------------------------------------------------------
// Penalty Dimension Tests: each test isolates one penalty component
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_ErrorRateOrdering(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

		simulateRequests(metricsTracker, upsList[0], method, 100, 20)
		simulateRequests(metricsTracker, upsList[1], method, 100, 30)
		simulateRequests(metricsTracker, upsList[2], method, 100, 10)

		registry.RefreshUpstreamNetworkMethodScores()

		expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForLatency", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequestsWithLatency(metricsTracker, upsList[0], method, 10, 0.20)
		simulateRequestsWithLatency(metricsTracker, upsList[1], method, 10, 0.70)
		simulateRequestsWithLatency(metricsTracker, upsList[2], method, 10, 0.02)

		registry.RefreshUpstreamNetworkMethodScores()

		expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForErrorRate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 10*time.Hour)
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequests(metricsTracker, upsList[0], method, 100, 30)
		simulateRequests(metricsTracker, upsList[1], method, 100, 80)
		simulateRequests(metricsTracker, upsList[2], method, 100, 10)

		registry.RefreshUpstreamNetworkMethodScores()

		expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForBlockLag", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 10*time.Hour)
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequests(metricsTracker, upsList[0], method, 100, 0)
		metricsTracker.SetLatestBlockNumber(upsList[0], 4000090, 0)
		simulateRequests(metricsTracker, upsList[1], method, 100, 0)
		metricsTracker.SetLatestBlockNumber(upsList[1], 4000100, 0)
		simulateRequests(metricsTracker, upsList[2], method, 100, 0)
		metricsTracker.SetLatestBlockNumber(upsList[2], 3005020, 0)

		registry.RefreshUpstreamNetworkMethodScores()

		expectedOrder := []string{"rpc2", "rpc1", "rpc3"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForFinalizationLag", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 10*time.Hour)
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequests(metricsTracker, upsList[0], method, 100, 0)
		metricsTracker.SetFinalizedBlockNumber(upsList[0], 4000090)
		simulateRequests(metricsTracker, upsList[1], method, 100, 0)
		metricsTracker.SetFinalizedBlockNumber(upsList[1], 3005020)
		simulateRequests(metricsTracker, upsList[2], method, 100, 0)
		metricsTracker.SetFinalizedBlockNumber(upsList[2], 4000100)

		registry.RefreshUpstreamNetworkMethodScores()

		expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForRespLatency", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequestsWithLatency(metricsTracker, upsList[0], method, 10, 0.05)
		simulateRequestsWithLatency(metricsTracker, upsList[1], method, 10, 0.03)
		simulateRequestsWithLatency(metricsTracker, upsList[2], method, 10, 0.01)

		expectedOrder := []string{"rpc3", "rpc2", "rpc1"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

			t.Run("CorrectOrderForErrorRateOverTime", func(t *testing.T) {
				// Deterministic: manual reset between phases. Avoid relying on the
				// tracker's reset ticker firing on busy CI.
				windowSize := 10 * time.Minute
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)
				l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
				upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		// Initial phase
		simulateRequests(metricsTracker, upsList[0], method, 100, 30)
		simulateRequests(metricsTracker, upsList[1], method, 100, 80)
		simulateRequests(metricsTracker, upsList[2], method, 100, 10)

			expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
				checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)

				// Reset metrics for a clean window.
				metricsTracker.ForceReset()

			// Second phase
			simulateRequests(metricsTracker, upsList[0], method, 100, 30)
			simulateRequests(metricsTracker, upsList[1], method, 100, 10)
			simulateRequests(metricsTracker, upsList[2], method, 100, 80)

		expectedOrder = []string{"rpc2", "rpc1", "rpc3"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForRateLimiting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)
		method := "eth_call"
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		time.Sleep(100 * time.Millisecond)

		simulateRequestsWithRateLimiting(metricsTracker, upsList[0], method, 100, 30, 30)
		simulateRequestsWithRateLimiting(metricsTracker, upsList[1], method, 100, 15, 15)
		simulateRequestsWithRateLimiting(metricsTracker, upsList[2], method, 100, 5, 5)

		expectedOrder := []string{"rpc3", "rpc2", "rpc1"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForTotalRequests", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)
		method := "eth_call"
		l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")

		simulateRequests(metricsTracker, upsList[0], method, 1000, 0)
		simulateRequests(metricsTracker, upsList[1], method, 20000, 0)
		simulateRequests(metricsTracker, upsList[2], method, 10, 0)

		expectedOrder := []string{"rpc3", "rpc1", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method, expectedOrder)
	})

	t.Run("CorrectOrderForMultipleMethodsRequests", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)

		methodGetLogs := "eth_getLogs"
		methodTraceTransaction := "eth_traceTransaction"
		l, _ := registry.GetSortedUpstreams(ctx, networkID, methodGetLogs)
		upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")
		_, _ = registry.GetSortedUpstreams(ctx, networkID, methodTraceTransaction)

		// Simulate performance for eth_getLogs
		simulateRequests(metricsTracker, upsList[0], methodGetLogs, 100, 10)
		simulateRequests(metricsTracker, upsList[1], methodGetLogs, 100, 30)
		simulateRequests(metricsTracker, upsList[2], methodGetLogs, 100, 20)

		// Simulate performance for eth_traceTransaction
		simulateRequests(metricsTracker, upsList[0], methodTraceTransaction, 100, 20)
		simulateRequests(metricsTracker, upsList[1], methodTraceTransaction, 100, 10)
		simulateRequests(metricsTracker, upsList[2], methodTraceTransaction, 100, 30)

		expectedOrderGetLogs := []string{"rpc1", "rpc3", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, methodGetLogs, expectedOrderGetLogs)

		expectedOrderTraceTransaction := []string{"rpc2", "rpc1", "rpc3"}
		checkUpstreamScoreOrder(t, registry, networkID, methodTraceTransaction, expectedOrderTraceTransaction)
	})

	t.Run("CorrectOrderForMultipleMethodsLatencyOverTime", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, windowSize)

		method1 := "eth_call"
		method2 := "eth_getBalance"
		l1, _ := registry.GetSortedUpstreams(ctx, networkID, method1)
		l2, _ := registry.GetSortedUpstreams(ctx, networkID, method2)
		upsList1 := getUpsByID(l1, "rpc1", "rpc2", "rpc3")
		upsList2 := getUpsByID(l2, "rpc1", "rpc2", "rpc3")

		// Phase 1: Initial performance
		simulateRequestsWithLatency(metricsTracker, upsList1[0], method1, 5, 0.01)
		simulateRequestsWithLatency(metricsTracker, upsList1[2], method1, 5, 0.3)
		simulateRequestsWithLatency(metricsTracker, upsList1[1], method1, 5, 0.8)

		expectedOrderMethod1Phase1 := []string{"rpc1", "rpc3", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method1, expectedOrderMethod1Phase1)

		// Wait so that latency averages are cycled out (add small buffer to avoid ticking exactly at window boundary)
		time.Sleep(windowSize + 10*time.Millisecond)

		simulateRequestsWithLatency(metricsTracker, upsList2[2], method2, 5, 0.01)
		simulateRequestsWithLatency(metricsTracker, upsList2[1], method2, 5, 0.03)
		simulateRequestsWithLatency(metricsTracker, upsList2[0], method2, 5, 0.05)

		expectedOrderMethod2Phase1 := []string{"rpc3", "rpc2", "rpc1"}
		checkUpstreamScoreOrder(t, registry, networkID, method2, expectedOrderMethod2Phase1)

		// Sleep slightly longer than windowSize to ensure metrics from phase 1 have cycled out
		time.Sleep(windowSize + 10*time.Millisecond)

		// Phase 2: Performance changes
		simulateRequestsWithLatency(metricsTracker, upsList1[1], method1, 5, 0.01)
		simulateRequestsWithLatency(metricsTracker, upsList1[2], method1, 5, 0.03)
		simulateRequestsWithLatency(metricsTracker, upsList1[0], method1, 5, 0.05)

		expectedOrderMethod1Phase2 := []string{"rpc2", "rpc3", "rpc1"}
		checkUpstreamScoreOrder(t, registry, networkID, method1, expectedOrderMethod1Phase2)

		// Sleep slightly longer than windowSize to ensure metrics from phase 2 for method1 have cycled out
		time.Sleep(windowSize + 10*time.Millisecond)

		simulateRequestsWithLatency(metricsTracker, upsList2[0], method2, 5, 0.01)
		simulateRequestsWithLatency(metricsTracker, upsList2[2], method2, 5, 0.03)
		simulateRequestsWithLatency(metricsTracker, upsList2[1], method2, 5, 0.05)

		expectedOrderMethod2Phase2 := []string{"rpc1", "rpc3", "rpc2"}
		checkUpstreamScoreOrder(t, registry, networkID, method2, expectedOrderMethod2Phase2)
	})
}

func TestUpstreamsRegistry_Scoring(t *testing.T) {
	projectID := "test-project"
	networkID := "evm:123"
	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequests(metricsTracker, ups[0], method, 100, 5)
	simulateRequests(metricsTracker, ups[1], method, 100, 50)
	simulateRequests(metricsTracker, ups[2], method, 100, 20)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	assert.Equal(t, "rpc1", ordered[0].Id(), "lowest error rate should be first")
	assert.Equal(t, "rpc3", ordered[1].Id(), "medium error rate should be second")
	assert.Equal(t, "rpc2", ordered[2].Id(), "highest error rate should be last")
}

func TestUpstreamsRegistry_LatencyOrdering(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 20, 0.020)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 20, 0.700)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 20, 0.200)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	assert.Equal(t, "rpc1", ordered[0].Id(), "lowest latency should be first")
	assert.Equal(t, "rpc3", ordered[1].Id(), "medium latency should be second")
	assert.Equal(t, "rpc2", ordered[2].Id(), "highest latency should be last")
}

func TestUpstreamsRegistry_BlockHeadLagOrdering(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Hour)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequests(metricsTracker, ups[0], method, 100, 0)
	metricsTracker.SetLatestBlockNumber(ups[0], 4000090, 0)
	simulateRequests(metricsTracker, ups[1], method, 100, 0)
	metricsTracker.SetLatestBlockNumber(ups[1], 4000100, 0)
	simulateRequests(metricsTracker, ups[2], method, 100, 0)
	metricsTracker.SetLatestBlockNumber(ups[2], 3005020, 0)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	assert.Equal(t, "rpc2", ordered[0].Id(), "zero block lag should be first")
	assert.Equal(t, "rpc1", ordered[1].Id(), "small block lag should be second")
	assert.Equal(t, "rpc3", ordered[2].Id(), "large block lag should be last")
}

func TestUpstreamsRegistry_FinalizationLagOrdering(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Hour)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequests(metricsTracker, ups[0], method, 100, 0)
	metricsTracker.SetFinalizedBlockNumber(ups[0], 4000090)
	simulateRequests(metricsTracker, ups[1], method, 100, 0)
	metricsTracker.SetFinalizedBlockNumber(ups[1], 3005020)
	simulateRequests(metricsTracker, ups[2], method, 100, 0)
	metricsTracker.SetFinalizedBlockNumber(ups[2], 4000100)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	assert.Equal(t, "rpc3", ordered[0].Id(), "zero finalization lag should be first")
	assert.Equal(t, "rpc1", ordered[1].Id(), "small finalization lag should be second")
	assert.Equal(t, "rpc2", ordered[2].Id(), "large finalization lag should be last")
}

func TestUpstreamsRegistry_ThrottlingOrdering(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_getLogs"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 10, 0.060)
	simulateRequestsWithRateLimiting(metricsTracker, ups[0], method, 20, 5)
	simulateRequestsWithRateLimiting(metricsTracker, ups[1], method, 20, 1)
	simulateRequestsWithRateLimiting(metricsTracker, ups[2], method, 20, 0)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	var i1, i2, i3 int
	for i, u := range ordered {
		switch u.Id() {
		case "rpc1":
			i1 = i
		case "rpc2":
			i2 = i
		case "rpc3":
			i3 = i
		}
	}
	assert.Less(t, i3, i2, "less throttled should rank before more throttled")
	assert.Less(t, i2, i1, "medium throttled should rank before heavily throttled")
}

func TestUpstreamsRegistry_MixedLatencyAndErrors(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	// rpc1: fast but extremely high error rate
	simulateRequestsWithLatency(metricsTracker, ups[0], method, 10, 0.050)
	simulateFailedRequests(metricsTracker, ups[0], method, 80)

	// rpc2: marginally slower but perfect reliability
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 100, 0.065)

	// rpc3: moderate speed, moderate errors
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 50, 0.060)
	simulateFailedRequests(metricsTracker, ups[2], method, 20)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)

	// rpc1: 89% errors (huge penalty), rpc2: 0% errors + mild latency above best,
	// rpc3: 29% errors + near-best latency. rpc2 should beat rpc1 despite being slightly slower.
	var i1, i2 int
	for i, u := range ordered {
		switch u.Id() {
		case "rpc1":
			i1 = i
		case "rpc2":
			i2 = i
		}
	}
	assert.Less(t, i2, i1, "reliable upstream with mild latency should rank before fast but error-prone upstream")
}

// ---------------------------------------------------------------------------
// Per-Method Isolation
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_PerMethodIsolation(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 5*time.Second)

	methodA := "eth_call"
	methodB := "eth_getBalance"
	networkID := "evm:123"
	lA, _ := registry.GetSortedUpstreams(ctx, networkID, methodA)
	lB, _ := registry.GetSortedUpstreams(ctx, networkID, methodB)
	upsA := getUpsByID(lA, "rpc1", "rpc2", "rpc3")
	upsB := getUpsByID(lB, "rpc1", "rpc2", "rpc3")

	// Method A: rpc1 is much faster
	simulateRequestsWithLatency(metricsTracker, upsA[0], methodA, 20, 0.020)
	simulateRequestsWithLatency(metricsTracker, upsA[1], methodA, 20, 0.500)
	simulateRequestsWithLatency(metricsTracker, upsA[2], methodA, 20, 0.300)

	// Method B: rpc2 is much faster
	simulateRequestsWithLatency(metricsTracker, upsB[0], methodB, 20, 0.500)
	simulateRequestsWithLatency(metricsTracker, upsB[1], methodB, 20, 0.020)
	simulateRequestsWithLatency(metricsTracker, upsB[2], methodB, 20, 0.300)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	orderedA, _ := registry.GetSortedUpstreams(ctx, networkID, methodA)
	orderedB, _ := registry.GetSortedUpstreams(ctx, networkID, methodB)
	assert.Equal(t, "rpc1", orderedA[0].Id(), "Method A should prefer rpc1 (lowest latency)")
	assert.Equal(t, "rpc2", orderedB[0].Id(), "Method B should prefer rpc2 (lowest latency)")
}

func TestUpstreamsRegistry_MultipleMethodsDifferentErrors(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	methodGetLogs := "eth_getLogs"
	methodTrace := "eth_traceTransaction"
	networkID := "evm:123"
	l1, _ := registry.GetSortedUpstreams(ctx, networkID, methodGetLogs)
	ups := getUpsByID(l1, "rpc1", "rpc2", "rpc3")
	_, _ = registry.GetSortedUpstreams(ctx, networkID, methodTrace)

	simulateRequests(metricsTracker, ups[0], methodGetLogs, 100, 10)
	simulateRequests(metricsTracker, ups[1], methodGetLogs, 100, 30)
	simulateRequests(metricsTracker, ups[2], methodGetLogs, 100, 20)

	simulateRequests(metricsTracker, ups[0], methodTrace, 100, 20)
	simulateRequests(metricsTracker, ups[1], methodTrace, 100, 10)
	simulateRequests(metricsTracker, ups[2], methodTrace, 100, 30)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	orderedGL, _ := registry.GetSortedUpstreams(ctx, networkID, methodGetLogs)
	orderedTr, _ := registry.GetSortedUpstreams(ctx, networkID, methodTrace)
	assert.Equal(t, "rpc1", orderedGL[0].Id(), "eth_getLogs should prefer rpc1 (fewest errors)")
	assert.Equal(t, "rpc2", orderedTr[0].Id(), "eth_traceTransaction should prefer rpc2 (fewest errors)")
}

// ---------------------------------------------------------------------------
// Stickiness
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_StickyPrimaryPreventsFlip(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger

	stickyCfg := &ScoringConfig{
		ScoreGranularity:  "method",
		SwitchHysteresis:  0.10,
		MinSwitchInterval: 2 * time.Minute,
	}
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 5*time.Second, stickyCfg)

	method := "eth_getBalance"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2")
	u1, u2 := ups[0], ups[1]

	// Phase 1: give rpc1 heavy errors so rpc2 takes over
	simulateFailedRequests(metricsTracker, u1, method, 50)
	simulateRequestsWithLatency(metricsTracker, u1, method, 10, 0.200)
	simulateRequestsWithLatency(metricsTracker, u2, method, 10, 0.050)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)
	ordered, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	firstPrimary := ordered[0].Id()

	// Phase 2: rpc1 becomes slightly faster — small advantage should NOT flip
	simulateRequestsWithLatency(metricsTracker, u1, method, 10, 0.048)
	simulateRequestsWithLatency(metricsTracker, u2, method, 10, 0.050)

	err = registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)
	ordered, _ = registry.GetSortedUpstreams(ctx, networkID, method)
	assert.Equal(t, firstPrimary, ordered[0].Id(), "Sticky primary should keep leading despite small latency advantage")
}

// ---------------------------------------------------------------------------
// Round-Robin
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_RoundRobinStrategy(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger

	metricsTracker := health.NewTracker(&logger, "test-project", 5*time.Second)
	metricsTracker.Bootstrap(ctx)
	upstreamConfigs := []*common.UpstreamConfig{
		{Id: "rpc1", Endpoint: "http://rpc1.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
		{Id: "rpc2", Endpoint: "http://rpc2.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
		{Id: "rpc3", Endpoint: "http://rpc3.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
	}
	vr := thirdparty.NewVendorsRegistry()
	pr, _ := thirdparty.NewProvidersRegistry(&logger, vr, nil, nil)
	ssr, _ := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{Driver: "memory", Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"}},
	})
	registry := NewUpstreamsRegistry(ctx, &logger, "test-project", upstreamConfigs, ssr, nil, vr, pr, nil, metricsTracker, 1*time.Second,
		&ScoringConfig{RoutingStrategy: "round-robin"},
		nil,
	)
	registry.Bootstrap(ctx)
	time.Sleep(100 * time.Millisecond)
	_ = registry.PrepareUpstreamsForNetwork(ctx, "evm:123")

	method := "eth_call"
	networkID := "evm:123"
	_, _ = registry.GetSortedUpstreams(ctx, networkID, method)

	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		err := registry.RefreshUpstreamNetworkMethodScores()
		require.NoError(t, err)
		ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
		require.NoError(t, err)
		assert.Len(t, ordered, 3)
		seen[ordered[0].Id()] = true
	}
	assert.Len(t, seen, 3, "Round-robin should rotate through all upstreams")
}

// ---------------------------------------------------------------------------
// Edge Cases
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_AllPeersNoSamplesNeutral(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, _ := createTestRegistry(ctx, "test-project", &logger, 5*time.Second)

	method := "eth_maxPriorityFeePerGas"
	networkID := "evm:123"

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	ordered, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	assert.Len(t, ordered, 3)
	ids := []string{ordered[0].Id(), ordered[1].Id(), ordered[2].Id()}
	assert.ElementsMatch(t, []string{"rpc1", "rpc2", "rpc3"}, ids)
}

func TestUpstreamsRegistry_PenaltyDecayOverTime(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 5*time.Second)

	method := "eth_chainId"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 10, 0.040)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 10, 0.100)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)
	registry.upstreamsMu.RLock()
	s1 := registry.upstreamScores["rpc1"][networkID][method]
	registry.upstreamsMu.RUnlock()
	assert.Greater(t, s1, 0.0, "first score should be > 0 for faster upstream")

	err = registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)
	registry.upstreamsMu.RLock()
	s2 := registry.upstreamScores["rpc1"][networkID][method]
	registry.upstreamsMu.RUnlock()
	assert.Greater(t, s2, 0.0, "score should remain positive after decay")
}

func TestUpstreamsRegistry_NaNGuardsPreventPropagation(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 5*time.Second)

	method := "eth_getBalance"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 10, 0.050)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 10, 0.070)

	for i := 0; i < 10; i++ {
		err := registry.RefreshUpstreamNetworkMethodScores()
		require.NoError(t, err)

		for upsID, networkScores := range registry.upstreamScores {
			for netID, methodScores := range networkScores {
				for meth, score := range methodScores {
					assert.False(t, math.IsNaN(score),
						"Score for %s/%s/%s should not be NaN (iteration %d)", upsID, netID, meth, i)
					assert.False(t, math.IsInf(score, 0),
						"Score for %s/%s/%s should not be Inf (iteration %d)", upsID, netID, meth, i)
				}
			}
		}

		simulateRequestsWithLatency(metricsTracker, ups[0], method, 5, 0.040+float64(i)*0.001)
		simulateRequestsWithLatency(metricsTracker, ups[1], method, 5, 0.055+float64(i)*0.002)
	}
}

func TestUpstreamsRegistry_PenaltyNaNInjection(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 5*time.Second)

	method := "eth_getBalance"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 10, 0.050)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 10, 0.070)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	for upsID := range registry.penaltyState {
		if registry.penaltyState[upsID][networkID] == nil {
			registry.penaltyState[upsID][networkID] = make(map[string]float64)
		}
		registry.penaltyState[upsID][networkID][method] = math.NaN()
	}

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 5, 0.040)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 5, 0.050)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 5, 0.060)

	err = registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	for upsID, networkScores := range registry.upstreamScores {
		for netID, methodScores := range networkScores {
			for meth, score := range methodScores {
				assert.False(t, math.IsNaN(score),
					"Score for %s/%s/%s should not be NaN after injection", upsID, netID, meth)
				assert.False(t, math.IsInf(score, 0),
					"Score for %s/%s/%s should not be Inf after injection", upsID, netID, meth)
				assert.GreaterOrEqual(t, score, 0.0,
					"Score for %s/%s/%s should be non-negative", upsID, netID, meth)
			}
		}
	}

	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	assert.Len(t, ordered, 3)
}

func TestUpstreamsRegistry_ZeroLatencyHandling(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx := context.Background()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	upstreams := registry.GetAllUpstreams()
	require.Len(t, upstreams, 3)

	var workingUpstream, failingUpstream *Upstream
	for _, ups := range upstreams {
		if ups.Id() == "rpc1" {
			workingUpstream = ups
		} else if ups.Id() == "rpc2" {
			failingUpstream = ups
		}
	}
	require.NotNil(t, workingUpstream)
	require.NotNil(t, failingUpstream)

	simulateRequestsWithLatency(metricsTracker, workingUpstream, method, 10, 0.1)
	simulateRequests(metricsTracker, workingUpstream, method, 10, 1)
	simulateRequests(metricsTracker, failingUpstream, method, 10, 10)

	_, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	sortedUpstreams, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)

	// Verify scores: working upstream should have higher score than failing one
	workingScore, ok := getUpstreamScore(registry, "rpc1", networkID, method)
	assert.True(t, ok)
	failingScore, ok := getUpstreamScore(registry, "rpc2", networkID, method)
	assert.True(t, ok)

	assert.Greater(t, workingScore, failingScore, "Working upstream should have higher score")

	workingRank := -1
	failingRank := -1
	for i, ups := range sortedUpstreams {
		if ups.Id() == "rpc1" {
			workingRank = i
		} else if ups.Id() == "rpc2" {
			failingRank = i
		}
	}
	assert.Less(t, workingRank, failingRank, "Working upstream should rank higher")
}

func TestUpstreamsRegistry_ScoreHigherIsBetter(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	simulateRequestsWithLatency(metricsTracker, ups[0], method, 50, 0.020)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 50, 0.500)
	simulateFailedRequests(metricsTracker, ups[1], method, 30)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 50, 0.100)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	// Phase 2: u3 gathers a few samples but below confidence threshold
	simulateRequestsWithLatency(metricsTracker, u3, method, 3, 0.030) // 30ms, but only 3 samples
	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	ordered, _ = registry.GetSortedUpstreams(ctx, networkID, method)
	assert.Equal(t, "rpc1", ordered[0].Id(), "Below confidence threshold, cold upstream should still not lead")

	// Phase 3: u3 reaches/exceeds confidence samples with very fast latency, then allow smoothing to catch up
	simulateRequestsWithLatency(metricsTracker, u3, method, 10, 0.030) // now >= 10 samples
	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	ordered, _ = registry.GetSortedUpstreams(ctx, networkID, method)
	assert.Equal(t, "rpc3", ordered[0].Id(), "After enough samples, the fast upstream should lead")
}

func TestUpstreamsRegistry_PerMethodIsolation(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	projectID := "test-project"
	networkID := "evm:123"
	methodA := "eth_call"
	methodB := "eth_getBalance"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 5*time.Second)

	// Pre-warm both methods
	lA, _ := registry.GetSortedUpstreams(ctx, networkID, methodA)
	lB, _ := registry.GetSortedUpstreams(ctx, networkID, methodB)
	upsA := getUpsByID(lA, "rpc1", "rpc2", "rpc3")
	upsB := getUpsByID(lB, "rpc1", "rpc2", "rpc3")

	// Method A: rpc1 fastest
	simulateRequestsWithLatency(metricsTracker, upsA[0], methodA, 10, 0.040) // rpc1 40ms
	simulateRequestsWithLatency(metricsTracker, upsA[1], methodA, 10, 0.070) // rpc2 70ms
	simulateRequestsWithLatency(metricsTracker, upsA[2], methodA, 10, 0.060) // rpc3 60ms

	// Method B: rpc2 fastest
	simulateRequestsWithLatency(metricsTracker, upsB[0], methodB, 10, 0.070) // rpc1 70ms
	simulateRequestsWithLatency(metricsTracker, upsB[1], methodB, 10, 0.040) // rpc2 40ms
	simulateRequestsWithLatency(metricsTracker, upsB[2], methodB, 10, 0.060) // rpc3 60ms

	err := registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	orderedA, _ := registry.GetSortedUpstreams(ctx, networkID, methodA)
	orderedB, _ := registry.GetSortedUpstreams(ctx, networkID, methodB)
	assert.Equal(t, "rpc1", orderedA[0].Id(), "Method A ordering should be independent and prefer rpc1")
	assert.Equal(t, "rpc2", orderedB[0].Id(), "Method B ordering should be independent and prefer rpc2")
}

func TestUpstreamsRegistry_ThrottlingPenalty(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	projectID := "test-project"
	networkID := "evm:123"
	method := "eth_getLogs"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 5*time.Second)

	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")
	u1 := upsList[0]
	u2 := upsList[1]

	// Equal latency for both
	simulateRequestsWithLatency(metricsTracker, u1, method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, u2, method, 10, 0.060)
	// Apply throttling to u1 significantly more than u2
	simulateRequestsWithRateLimiting(metricsTracker, u1, method, 20, 10, 5) // more throttling
	simulateRequestsWithRateLimiting(metricsTracker, u2, method, 20, 1, 1)  // less throttling

	err := registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	ordered, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	// We only assert relative ordering between u2 and u1 to avoid interference from other peers
	var i1, i2 int
	for i, u := range ordered {
		if u.Id() == u1.Id() {
			i1 = i
		}
		if u.Id() == u2.Id() {
			i2 = i
		}
	}
	assert.Less(t, i2, i1, "Higher throttling should demote an upstream with equal latency")
}

func TestUpstreamsRegistry_AllPeersNoSamplesNeutral(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	projectID := "test-project"
	networkID := "evm:123"
	method := "eth_maxPriorityFeePerGas"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, _ := createTestRegistry(ctx, projectID, &logger, 5*time.Second)

	// Do not record any metric samples
	err := registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	ordered, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	assert.Len(t, ordered, 3)
	// With all-equal effective metrics, ordering may be arbitrary; assert membership
	ids := []string{ordered[0].Id(), ordered[1].Id(), ordered[2].Id()}
	assert.ElementsMatch(t, []string{"rpc1", "rpc2", "rpc3"}, ids)
}

func TestUpstreamsRegistry_EMAFromZero_IncreasesOnSecondRefresh(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	projectID := "test-project"
	networkID := "evm:123"
	method := "eth_chainId"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, projectID, &logger, 5*time.Second)

	// Get upstreams and set two distinct latencies so instant score > 0 for the faster one
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	upsList := getUpsByID(l, "rpc1", "rpc2")
	faster := upsList[0]
	slower := upsList[1]

	// Assign latencies: faster < slower
	simulateRequestsWithLatency(metricsTracker, faster, method, 10, 0.040) // 40ms
	simulateRequestsWithLatency(metricsTracker, slower, method, 10, 0.100) // 100ms

	// First refresh → first smoothed score with prev==0
	err := registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	s1, ok := getUpstreamScore(registry, faster.Id(), networkID, method)
	assert.True(t, ok)
	assert.Greater(t, s1, 0.0, "first score should be > 0 for faster upstream")

	// Second refresh (same metrics) → EMA should increase compared to first
	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	s2, ok := getUpstreamScore(registry, faster.Id(), networkID, method)
	assert.True(t, ok)
	assert.Greater(t, s2, s1, "EMA should increase on the second refresh with identical metrics (prev==0 applied)")
}

func createTestRegistry(ctx context.Context, projectID string, logger *zerolog.Logger, windowSize time.Duration) (*UpstreamsRegistry, *health.Tracker) {
	upstreamConfigs := []*common.UpstreamConfig{
		{Id: "rpc1", Endpoint: "http://rpc1.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
		{Id: "rpc2", Endpoint: "http://rpc2.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
		{Id: "rpc3", Endpoint: "http://rpc3.localhost", Type: common.UpstreamTypeEvm, Evm: &common.EvmUpstreamConfig{ChainId: 123}},
	}
	return createTestRegistryWithUpstreams(ctx, projectID, logger, windowSize, upstreamConfigs)
}

func createTestRegistryWithUpstreams(ctx context.Context, projectID string, logger *zerolog.Logger, windowSize time.Duration, upstreamConfigs []*common.UpstreamConfig) (*UpstreamsRegistry, *health.Tracker) {
	metricsTracker := health.NewTracker(logger, projectID, windowSize)
	metricsTracker.Bootstrap(ctx)

	var scoringCfg *ScoringConfig
	if len(scoringCfgs) > 0 && scoringCfgs[0] != nil {
		scoringCfg = scoringCfgs[0]
	} else {
		scoringCfg = &ScoringConfig{
			ScoreGranularity:  "method",
			SwitchHysteresis:  -1,
			MinSwitchInterval: -1,
		}
	}

	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(logger, vr, nil, nil)
	if err != nil {
		panic(err)
	}
	ssr, err := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"},
		},
	})
	if err != nil {
		panic(err)
	}
	registry := NewUpstreamsRegistry(ctx, logger, projectID, upstreamConfigs, ssr, nil, vr, pr, nil, metricsTracker,
		1*time.Second,
		scoringCfg,
		nil,
	)

	registry.Bootstrap(ctx)
	networkIDs := map[string]struct{}{}
	for _, cfg := range upstreamConfigs {
		if cfg == nil || cfg.Evm == nil {
			continue
		}
		networkIDs[fmt.Sprintf("evm:%d", cfg.Evm.ChainId)] = struct{}{}
	}
	if len(networkIDs) == 0 {
		networkIDs["evm:123"] = struct{}{}
	}
	for networkID := range networkIDs {
		prepareCtx, prepareCancel := context.WithTimeout(ctx, 10*time.Second)
		err = registry.PrepareUpstreamsForNetwork(prepareCtx, networkID)
		prepareCancel()
		if err != nil {
			panic(err)
		}
	}

	return registry, metricsTracker
}

func simulateRequests(tracker *health.Tracker, upstream common.Upstream, method string, total, errors int) {
	for i := 0; i < total; i++ {
		tracker.RecordUpstreamRequest(upstream, method)
		if i < errors {
			tracker.RecordUpstreamFailure(upstream, method, fmt.Errorf("test problem"))
		}
	}
}

func simulateRequestsWithRateLimiting(tracker *health.Tracker, upstream common.Upstream, method string, total, remoteLimited int) {
	for i := 0; i < total; i++ {
		tracker.RecordUpstreamRequest(upstream, method)
		if i < remoteLimited {
			tracker.RecordUpstreamRemoteRateLimited(context.Background(), upstream, method, nil)
		}
	}
}

func simulateRequestsWithLatency(tracker *health.Tracker, upstream common.Upstream, method string, total int, latency float64) {
	wg := sync.WaitGroup{}
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.RecordUpstreamRequest(upstream, method)
			tracker.RecordUpstreamDuration(upstream, method, time.Duration(latency*float64(time.Second)), true, "none", common.DataFinalityStateUnknown, "n/a")
		}()
	}
	wg.Wait()
}

func simulateFailedRequests(tracker *health.Tracker, upstream common.Upstream, method string, count int) {
	for i := 0; i < count; i++ {
		tracker.RecordUpstreamRequest(upstream, method)
		tracker.RecordUpstreamFailure(upstream, method, fmt.Errorf("test problem"))
	}
}

func getSortedUpstreamsEventually(ctx context.Context, registry *UpstreamsRegistry, networkID, method string, timeout time.Duration) ([]common.Upstream, error) {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		ups, err := registry.GetSortedUpstreams(ctx, networkID, method)
		if err == nil {
			return ups, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func getUpstreamScore(registry *UpstreamsRegistry, upstreamID, networkID, method string) (float64, bool) {
	registry.RLockUpstreams()
	defer registry.RUnlockUpstreams()
	networkScores, ok := registry.upstreamScores[upstreamID]
	if !ok {
		return 0, false
	}
	methodScores, ok := networkScores[networkID]
	if !ok {
		return 0, false
	}
	score, ok := methodScores[method]
	return score, ok
}

func hasUpstreamScore(registry *UpstreamsRegistry, upstreamID, networkID, method string) bool {
	_, ok := getUpstreamScore(registry, upstreamID, networkID, method)
	return ok
}

func snapshotUpstreamScores(registry *UpstreamsRegistry) map[string]map[string]map[string]float64 {
	registry.RLockUpstreams()
	defer registry.RUnlockUpstreams()
	snapshot := make(map[string]map[string]map[string]float64, len(registry.upstreamScores))
	for upsID, networkScores := range registry.upstreamScores {
		nwSnapshot := make(map[string]map[string]float64, len(networkScores))
		for networkID, methodScores := range networkScores {
			methodSnapshot := make(map[string]float64, len(methodScores))
			for method, score := range methodScores {
				methodSnapshot[method] = score
			}
			nwSnapshot[networkID] = methodSnapshot
		}
		snapshot[upsID] = nwSnapshot
	}
	return snapshot
}


func checkUpstreamScoreOrder(t *testing.T, registry *UpstreamsRegistry, networkID, method string, expectedOrder []string) {
	registry.RefreshUpstreamNetworkMethodScores()

	for i, ups := range expectedOrder {
		if i+1 < len(expectedOrder) {
			score, ok := getUpstreamScore(registry, ups, networkID, method)
			assert.True(t, ok)
			nextScore, ok := getUpstreamScore(registry, expectedOrder[i+1], networkID, method)
			assert.True(t, ok)
			assert.Greater(
				t,
				score,
				nextScore,
				"Upstream %s should have a higher score than %s",
				ups,
				expectedOrder[i+1],
			)
		}
	}

	sortedUpstreams, err := registry.GetSortedUpstreams(context.Background(), networkID, method)

	assert.NoError(t, err)
	registry.RLockUpstreams()
	for i, ups := range sortedUpstreams {
		assert.Equal(t, expectedOrder[i], ups.Id())
	}
	registry.RUnlockUpstreams()
}

func TestUpstreamsRegistry_RefreshPrunesStaleMethodCaches(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)

	networkID := "evm:123"
	methodActive := "eth_getBalance"
	methodEvicted := "eth_customUnknownMethod"

	_, err := getSortedUpstreamsEventually(ctx, registry, networkID, methodActive, 5*time.Second)
	assert.NoError(t, err)
	_, err = getSortedUpstreamsEventually(ctx, registry, networkID, methodEvicted, 5*time.Second)
	assert.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	registry.RLockUpstreams()
	assert.Contains(t, registry.sortedUpstreams[networkID], methodActive)
	assert.Contains(t, registry.sortedUpstreams[networkID], methodEvicted)
	assert.Contains(t, registry.sortedUpstreams[defaultNetworkMethod], methodActive)
	assert.Contains(t, registry.sortedUpstreams[defaultNetworkMethod], methodEvicted)
	registry.RUnlockUpstreams()
	assert.True(t, hasUpstreamScore(registry, "rpc1", networkID, methodEvicted))

	// Mark the evicted method as stale and keep the active method warm.
	staleUsage := time.Now().Add(-2 * sortedMethodUsageTTL)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{network: networkID, method: methodEvicted}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{network: defaultNetworkMethod, method: methodEvicted}, staleUsage)

	keepUsage := time.Now()
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{network: networkID, method: methodActive}, keepUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{network: defaultNetworkMethod, method: methodActive}, keepUsage)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)
	registry.RLockUpstreams()
	networkMethods := registry.sortedUpstreams[networkID]
	wildcardMethods := registry.sortedUpstreams[defaultNetworkMethod]
	activeInNetwork := false
	evictedInNetwork := false
	activeInWildcard := false
	evictedInWildcard := false
	if networkMethods != nil {
		_, activeInNetwork = networkMethods[methodActive]
		_, evictedInNetwork = networkMethods[methodEvicted]
	}
	if wildcardMethods != nil {
		_, activeInWildcard = wildcardMethods[methodActive]
		_, evictedInWildcard = wildcardMethods[methodEvicted]
	}
	registry.RUnlockUpstreams()

	assert.True(t, activeInNetwork)
	assert.True(t, activeInWildcard)
	assert.False(t, evictedInNetwork)
	assert.False(t, evictedInWildcard)

	// stale method scores should be removed for both wildcard and per-network scopes
	if hasUpstreamScore(registry, "rpc1", networkID, methodEvicted) {
		t.Fatalf("stale network-scoped method score should be pruned")
	}
	assert.False(t, hasUpstreamScore(registry, "rpc1", defaultNetworkMethod, methodEvicted), "stale wildcard-scoped method score should be pruned")
}

func TestUpstreamsRegistry_RefreshPrunesMethodCachesToCap(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)
	networkID := "evm:123"

	methodCount := sortedMethodMaxPerNetwork + 3
	methods := make([]string, 0, methodCount)

	for i := 0; i < methodCount; i++ {
		method := fmt.Sprintf("eth_cap_method_%d", i)
		methods = append(methods, method)
		_, err := getSortedUpstreamsEventually(ctx, registry, networkID, method, 5*time.Second)
		assert.NoError(t, err)
	}

	err := registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	networkMethods := registry.sortedUpstreams[networkID]
	wildcardMethods := registry.sortedUpstreams[defaultNetworkMethod]
	assert.Equal(t, sortedMethodMaxPerNetwork, len(networkMethods))
	assert.Equal(t, sortedMethodMaxPerNetwork, len(wildcardMethods))
	registry.RUnlockUpstreams()

	now := time.Now()
	for i, method := range methods {
		usage := now.Add(time.Duration(i) * time.Millisecond)
		registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
			network: networkID,
			method:  method,
		}, usage)
		registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
			network: defaultNetworkMethod,
			method:  method,
		}, usage)
	}

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	var oldestPruned bool
	var newestKept bool
	var oldestWildcardPruned bool
	var newestWildcardKept bool

	registry.RLockUpstreams()
	networkMethods = registry.sortedUpstreams[networkID]
	wildcardMethods = registry.sortedUpstreams[defaultNetworkMethod]
	assert.Equal(t, sortedMethodMaxPerNetwork, len(networkMethods))
	assert.Equal(t, sortedMethodMaxPerNetwork, len(wildcardMethods))
	_, oldestPruned = networkMethods[methods[0]]
	_, newestKept = networkMethods[methods[len(methods)-1]]
	_, oldestWildcardPruned = wildcardMethods[methods[0]]
	_, newestWildcardKept = wildcardMethods[methods[len(methods)-1]]
	registry.RUnlockUpstreams()

	assert.False(t, oldestPruned, "oldest method should be pruned by LRU overflow policy")
	assert.True(t, newestKept, "most recently used method should remain under cap")
	assert.False(t, oldestWildcardPruned, "oldest wildcard method should be pruned by LRU overflow policy")
	assert.True(t, newestWildcardKept, "most recently used wildcard method should remain under cap")

	for _, upstreamID := range []string{"rpc1", "rpc2", "rpc3"} {
		assert.False(t, hasUpstreamScore(registry, upstreamID, networkID, methods[0]))
		assert.False(t, hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methods[0]))
		assert.True(t, hasUpstreamScore(registry, upstreamID, networkID, methods[len(methods)-1]))
		assert.True(t, hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methods[len(methods)-1]))
	}
}

func TestUpstreamsRegistry_RefreshPrunesMethodCachesWithoutUsageRecord(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)
	networkID := "evm:123"
	methodWithUsage := "eth_stale_with_usage"
	methodWithoutUsage := "eth_no_usage_record"

	_, err := getSortedUpstreamsEventually(ctx, registry, networkID, methodWithUsage, 5*time.Second)
	assert.NoError(t, err)
	_, err = getSortedUpstreamsEventually(ctx, registry, networkID, methodWithoutUsage, 5*time.Second)
	assert.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	staleUsage := time.Now().Add(-2 * sortedMethodUsageTTL)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodWithUsage,
	}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodWithUsage,
	}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Delete(methodUsageKey{
		network: networkID,
		method:  methodWithoutUsage,
	})
	registry.sortedUpstreamsMethodUsage.Delete(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodWithoutUsage,
	})

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	networkMethods := registry.sortedUpstreams[networkID]
	wildcardMethods := registry.sortedUpstreams[defaultNetworkMethod]
	_, inNetworkWithUsage := networkMethods[methodWithUsage]
	_, inWildcardWithUsage := wildcardMethods[methodWithUsage]
	_, inNetworkWithoutUsage := networkMethods[methodWithoutUsage]
	_, inWildcardWithoutUsage := wildcardMethods[methodWithoutUsage]
	registry.RUnlockUpstreams()

	assert.False(t, inNetworkWithUsage, "stale method should be pruned when usage is present and stale")
	assert.False(t, inWildcardWithUsage, "stale wildcard scope should be pruned when usage is present and stale")
	assert.True(t, inNetworkWithoutUsage, "method without usage should stay via fallback-to-now path")
	assert.True(t, inWildcardWithoutUsage, "wildcard method without usage should stay via fallback-to-now path")
}

func TestUpstreamsRegistry_RefreshPrunesOnlyStaleNetworkScopedMethod(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)
	networkID := "evm:123"
	methodScoped := "eth_network_scope_only"
	methodWildcard := "eth_wildcard_scope_kept"

	_, err := getSortedUpstreamsEventually(ctx, registry, networkID, methodScoped, 5*time.Second)
	assert.NoError(t, err)
	_, err = getSortedUpstreamsEventually(ctx, registry, networkID, methodWildcard, 5*time.Second)
	assert.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	_, inNetworkScoped := registry.sortedUpstreams[networkID][methodScoped]
	_, inNetworkWildcard := registry.sortedUpstreams[networkID][methodWildcard]
	_, inWildcardScoped := registry.sortedUpstreams[defaultNetworkMethod][methodScoped]
	_, inWildcardFresh := registry.sortedUpstreams[defaultNetworkMethod][methodWildcard]
	registry.RUnlockUpstreams()
	assert.True(t, inNetworkScoped)
	assert.True(t, inNetworkWildcard)
	assert.True(t, inWildcardScoped)
	assert.True(t, inWildcardFresh)

	// Mark only the scoped method as stale for network scope and keep it fresh in wildcard scope.
	staleUsage := time.Now().Add(-2 * sortedMethodUsageTTL)
	keepUsage := time.Now()
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodScoped,
	}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodScoped,
	}, keepUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodWildcard,
	}, keepUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodWildcard,
	}, keepUsage)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	_, prunedFromNetworkScoped := registry.sortedUpstreams[networkID][methodScoped]
	_, keptInNetworkWildcard := registry.sortedUpstreams[networkID][methodWildcard]
	_, keptInWildcardScope := registry.sortedUpstreams[defaultNetworkMethod][methodScoped]
	_, keptInWildcardFresh := registry.sortedUpstreams[defaultNetworkMethod][methodWildcard]
	registry.RUnlockUpstreams()

	assert.False(t, prunedFromNetworkScoped, "stale per-network scope should be removed")
	assert.True(t, keptInNetworkWildcard, "fresh scoped-by-method should stay in network scope")
	assert.True(t, keptInWildcardScope, "fresh wildcard scope should keep stale-network method")
	assert.True(t, keptInWildcardFresh, "fresh wildcard method should stay in wildcard scope")

	// stale network scoped method score should be removed for all upstream IDs
	for _, upstreamID := range []string{"rpc1", "rpc2", "rpc3"} {
		if hasUpstreamScore(registry, upstreamID, networkID, methodScoped) {
			t.Fatalf("method %s should be pruned from stale network-scoped scores", methodScoped)
		}
		if !hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methodScoped) {
			t.Fatalf("method %s should stay in wildcard scope", methodScoped)
		}
		if !hasUpstreamScore(registry, upstreamID, networkID, methodWildcard) {
			t.Fatalf("method %s should stay in network scope", methodWildcard)
		}
		if !hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methodWildcard) {
			t.Fatalf("method %s should stay in wildcard scope", methodWildcard)
		}
	}
}

func TestUpstreamsRegistry_RefreshPrunesOnlyStaleWildcardMethodScope(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)
	networkID := "evm:123"
	methodScoped := "eth_network_scope_kept"
	methodWildcard := "eth_wildcard_scope_only"

	_, err := getSortedUpstreamsEventually(ctx, registry, networkID, methodScoped, 5*time.Second)
	assert.NoError(t, err)
	_, err = getSortedUpstreamsEventually(ctx, registry, networkID, methodWildcard, 5*time.Second)
	assert.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	_, inNetworkScoped := registry.sortedUpstreams[networkID][methodScoped]
	_, inNetworkWildcard := registry.sortedUpstreams[networkID][methodWildcard]
	_, inWildcardScoped := registry.sortedUpstreams[defaultNetworkMethod][methodScoped]
	_, inWildcardOnly := registry.sortedUpstreams[defaultNetworkMethod][methodWildcard]
	registry.RUnlockUpstreams()
	assert.True(t, inNetworkScoped)
	assert.True(t, inNetworkWildcard)
	assert.True(t, inWildcardScoped)
	assert.True(t, inWildcardOnly)

	staleUsage := time.Now().Add(-2 * sortedMethodUsageTTL)
	keepUsage := time.Now()
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodScoped,
	}, keepUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodScoped,
	}, keepUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodWildcard,
	}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodWildcard,
	}, staleUsage)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	registry.RLockUpstreams()
	_, keptInNetworkScoped := registry.sortedUpstreams[networkID][methodScoped]
	_, prunedFromNetworkWildcard := registry.sortedUpstreams[networkID][methodWildcard]
	_, keptInWildcardScoped := registry.sortedUpstreams[defaultNetworkMethod][methodScoped]
	_, prunedInWildcard := registry.sortedUpstreams[defaultNetworkMethod][methodWildcard]
	registry.RUnlockUpstreams()

	assert.True(t, keptInNetworkScoped, "fresh network scope should keep method")
	assert.True(t, keptInWildcardScoped, "fresh wildcard scope should keep method")
	assert.False(t, prunedFromNetworkWildcard, "stale network scope should remove method")
	assert.False(t, prunedInWildcard, "stale wildcard scope should remove method")

	for _, upstreamID := range []string{"rpc1", "rpc2", "rpc3"} {
		if !hasUpstreamScore(registry, upstreamID, networkID, methodScoped) {
			t.Fatalf("method %s should stay in fresh network-scoped scores", methodScoped)
		}
		if !hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methodScoped) {
			t.Fatalf("method %s should stay in fresh wildcard scores", methodScoped)
		}
		if hasUpstreamScore(registry, upstreamID, networkID, methodWildcard) {
			t.Fatalf("method %s should be pruned from stale network-scoped scores", methodWildcard)
		}
		if hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methodWildcard) {
			t.Fatalf("method %s should be pruned from stale wildcard scores", methodWildcard)
		}
	}
}

func TestUpstreamsRegistry_RefreshPrunesStaleMethodScoresForAllUpstreams(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, _ := createTestRegistry(ctx, "test-project", &logger, time.Minute)
	networkID := "evm:123"
	methodEvicted := "eth_stale_prune"

	_, err := getSortedUpstreamsEventually(ctx, registry, networkID, methodEvicted, 5*time.Second)
	assert.NoError(t, err)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	staleUsage := time.Now().Add(-2 * sortedMethodUsageTTL)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: networkID,
		method:  methodEvicted,
	}, staleUsage)
	registry.sortedUpstreamsMethodUsage.Store(methodUsageKey{
		network: defaultNetworkMethod,
		method:  methodEvicted,
	}, staleUsage)

	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	for _, upstreamID := range []string{"rpc1", "rpc2", "rpc3"} {
		if hasUpstreamScore(registry, upstreamID, networkID, methodEvicted) {
			t.Fatalf("method %s should be pruned from all upstream network scores", methodEvicted)
		}
		if hasUpstreamScore(registry, upstreamID, defaultNetworkMethod, methodEvicted) {
			t.Fatalf("method %s should be pruned from all upstream wildcard scores", methodEvicted)
		}
	}
}


func TestUpstreamsRegistry_NaNGuardsPreventPropagation(t *testing.T) {
	// This test verifies that NaN values in scores don't propagate through
	// EMA smoothing and don't get emitted to Prometheus metrics.
	// NaN can occur from edge cases in metrics collection and once present
	// would propagate indefinitely through EMA calculations without guards.
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	// Get upstreams
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	upsList := getUpsByID(l, "rpc1", "rpc2", "rpc3")
	u1 := upsList[0]
	u2 := upsList[1]
	u3 := upsList[2]

	// Simulate some requests to establish initial scores
	simulateRequestsWithLatency(metricsTracker, u1, method, 10, 0.050)
	simulateRequestsWithLatency(metricsTracker, u2, method, 10, 0.060)
	simulateRequestsWithLatency(metricsTracker, u3, method, 10, 0.070)

	// Run multiple refresh cycles to verify scores remain valid
	for i := 0; i < 10; i++ {
		err := registry.RefreshUpstreamNetworkMethodScores()
		assert.NoError(t, err)

		// Verify no scores are NaN after refresh
		for upsID, networkScores := range snapshotUpstreamScores(registry) {
			for netID, methodScores := range networkScores {
				for meth, score := range methodScores {
					assert.False(t, math.IsNaN(score),
						"Score for upstream %s, network %s, method %s should not be NaN (iteration %d)",
						upsID, netID, meth, i)
					assert.False(t, math.IsInf(score, 0),
						"Score for upstream %s, network %s, method %s should not be Inf (iteration %d)",
						upsID, netID, meth, i)
				}
			}
		}

		// Add more requests between refreshes to vary conditions
		simulateRequestsWithLatency(metricsTracker, u1, method, 5, 0.040+float64(i)*0.001)
		simulateRequestsWithLatency(metricsTracker, u2, method, 5, 0.055+float64(i)*0.002)
	}

	// Verify final sorted order is valid (no NaN-induced sorting issues)
	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	assert.NoError(t, err)
	assert.Len(t, ordered, 3, "Should have 3 upstreams in sorted order")

	// Verify scores are in descending order (higher score = higher priority)
	scores := snapshotUpstreamScores(registry)
	for i := 0; i < len(ordered)-1; i++ {
		curr := ordered[i]
		next := ordered[i+1]
		currScore, ok := scores[curr.Id()][networkID][method]
		assert.True(t, ok)
		nextScore, ok := scores[next.Id()][networkID][method]
		assert.True(t, ok)
		assert.GreaterOrEqual(t, currScore, nextScore,
			"Upstream %s (score %.4f) should have >= score than %s (score %.4f)",
			curr.Id(), currScore, next.Id(), nextScore)
	}
}

func TestUpstreamsRegistry_CalculateScoreEdgeCases(t *testing.T) {
	// Test that calculateScore handles edge cases without producing NaN
	registry := &UpstreamsRegistry{
		scoreRefreshInterval: time.Second,
		logger:               &log.Logger,
	}

	routingConfig := &common.RoutingConfig{}
	err := routingConfig.SetDefaults()
	assert.NoError(t, err)

	upstream := &Upstream{
		config: &common.UpstreamConfig{
			Id:      "test-upstream",
			Routing: routingConfig,
		},
	}

	testCases := []struct {
		name                string
		normTotalRequests   float64
		normRespLatency     float64
		normErrorRate       float64
		normThrottledRate   float64
		normBlockHeadLag    float64
		normFinalizationLag float64
		normMisbehaviorRate float64
	}{
		{
			name:              "All zeros",
			normTotalRequests: 0, normRespLatency: 0, normErrorRate: 0,
			normThrottledRate: 0, normBlockHeadLag: 0, normFinalizationLag: 0, normMisbehaviorRate: 0,
		},
		{
			name:              "All ones",
			normTotalRequests: 1, normRespLatency: 1, normErrorRate: 1,
			normThrottledRate: 1, normBlockHeadLag: 1, normFinalizationLag: 1, normMisbehaviorRate: 1,
		},
		{
			name:              "Mixed values",
			normTotalRequests: 0.5, normRespLatency: 0.3, normErrorRate: 0.1,
			normThrottledRate: 0.2, normBlockHeadLag: 0.4, normFinalizationLag: 0.05, normMisbehaviorRate: 0.01,
		},
		{
			name:              "Boundary high",
			normTotalRequests: 0.999, normRespLatency: 0.999, normErrorRate: 0.999,
			normThrottledRate: 0.999, normBlockHeadLag: 0.999, normFinalizationLag: 0.999, normMisbehaviorRate: 0.999,
		},
		{
			name:              "Boundary low",
			normTotalRequests: 0.001, normRespLatency: 0.001, normErrorRate: 0.001,
			normThrottledRate: 0.001, normBlockHeadLag: 0.001, normFinalizationLag: 0.001, normMisbehaviorRate: 0.001,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			score := registry.calculateScore(
				upstream,
				"evm:1",
				"eth_getBalance",
				[]common.DataFinalityState{common.DataFinalityStateFinalized},
				tc.normTotalRequests,
				tc.normRespLatency,
				tc.normErrorRate,
				tc.normThrottledRate,
				tc.normBlockHeadLag,
				tc.normFinalizationLag,
				tc.normMisbehaviorRate,
			)

			assert.False(t, math.IsNaN(score), "Score should not be NaN for test case: %s", tc.name)
			assert.False(t, math.IsInf(score, 0), "Score should not be Inf for test case: %s", tc.name)
			assert.GreaterOrEqual(t, score, 0.0, "Score should be non-negative for test case: %s", tc.name)
		})
	}
}

func TestNormalizeValues_HandlesNaNAndInf(t *testing.T) {
	// Test that normalizeValues handles NaN and Inf inputs correctly
	testCases := []struct {
		name     string
		input    []float64
		expected []float64
	}{
		{
			name:     "Normal values",
			input:    []float64{1.0, 2.0, 4.0},
			expected: []float64{0.25, 0.5, 1.0},
		},
		{
			name:     "With NaN",
			input:    []float64{1.0, math.NaN(), 4.0},
			expected: []float64{0.25, 0.0, 1.0},
		},
		{
			name:     "With Inf",
			input:    []float64{1.0, math.Inf(1), 4.0},
			expected: []float64{0.25, 0.0, 1.0},
		},
		{
			name:     "All NaN",
			input:    []float64{math.NaN(), math.NaN(), math.NaN()},
			expected: []float64{0.0, 0.0, 0.0},
		},
		{
			name:     "Mixed invalid",
			input:    []float64{math.NaN(), 2.0, math.Inf(-1), 4.0},
			expected: []float64{0.0, 0.5, 0.0, 1.0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := normalizeValues(tc.input)
			assert.Equal(t, len(tc.expected), len(result))
			for i := range result {
				assert.False(t, math.IsNaN(result[i]), "Result[%d] should not be NaN", i)
				assert.False(t, math.IsInf(result[i], 0), "Result[%d] should not be Inf", i)
				assert.InDelta(t, tc.expected[i], result[i], 0.001, "Result[%d] mismatch", i)
			}
		})
	}
}

func TestNormalizeValuesLog_HandlesNaNAndInf(t *testing.T) {
	// Test that normalizeValuesLog handles NaN and Inf inputs correctly
	testCases := []struct {
		name  string
		input []float64
	}{
		{
			name:  "Normal values",
			input: []float64{1.0, 10.0, 100.0},
		},
		{
			name:  "With NaN",
			input: []float64{1.0, math.NaN(), 100.0},
		},
		{
			name:  "With Inf",
			input: []float64{1.0, math.Inf(1), 100.0},
		},
		{
			name:  "All NaN",
			input: []float64{math.NaN(), math.NaN(), math.NaN()},
		},
		{
			name:  "Mixed invalid",
			input: []float64{math.NaN(), 10.0, math.Inf(-1), 100.0},
		},
		{
			name:  "NaN at start",
			input: []float64{math.NaN(), 1.0, 10.0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := normalizeValuesLog(tc.input)
			assert.Equal(t, len(tc.input), len(result))
			for i := range result {
				assert.False(t, math.IsNaN(result[i]), "Result[%d] should not be NaN for input %v", i, tc.input)
				assert.False(t, math.IsInf(result[i], 0), "Result[%d] should not be Inf for input %v", i, tc.input)
				assert.GreaterOrEqual(t, result[i], 0.0, "Result[%d] should be >= 0", i)
				assert.LessOrEqual(t, result[i], 1.0, "Result[%d] should be <= 1", i)
			}
		})
	}
}

func TestUpstreamsRegistry_EMANaNInjection(t *testing.T) {
	// This test directly injects NaN values into the previous scores map
	// to verify that the EMA smoothing guards correctly handle them.
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	projectID := "test-project"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	// rpc1 and rpc2 each handle 100 requests successfully
	simulateRequestsWithLatency(metricsTracker, ups[0], method, 100, 0.050)
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 100, 0.050)
	simulateRequestsWithLatency(metricsTracker, ups[2], method, 100, 0.050)

	// Simulate 50 hedge cancellations on rpc2 (rpc2 always lost the hedge race).
	// These should NOT affect rpc2's score because they aren't real failures.
	for i := 0; i < 50; i++ {
		metricsTracker.RecordUpstreamRequest(ups[1], method)
		metricsTracker.RecordUpstreamFailure(ups[1], method,
			common.NewErrEndpointRequestCanceled(fmt.Errorf("context canceled")))
	}

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	// Inject NaN into the scores map to simulate corrupted previous scores
	// This tests the guard at the EMA smoothing level
	registry.upstreamsMu.Lock()
	for upsID := range registry.upstreamScores {
		if registry.upstreamScores[upsID][networkID] == nil {
			registry.upstreamScores[upsID][networkID] = make(map[string]float64)
		}
		registry.upstreamScores[upsID][networkID][method] = math.NaN()
	}
	registry.upstreamsMu.Unlock()

	assert.InDelta(t, scoreAfter1, scoreAfter2, 0.01,
		"rpc2 (with hedge cancellations) should score the same as rpc1 (clean)")

	// Run refresh - NaN guards should reset prev to 0
	err = registry.RefreshUpstreamNetworkMethodScores()
	assert.NoError(t, err)

	// Verify all scores are now valid (not NaN/Inf)
	for upsID, networkScores := range snapshotUpstreamScores(registry) {
		for netID, methodScores := range networkScores {
			for meth, score := range methodScores {
				assert.False(t, math.IsNaN(score),
					"Score for upstream %s, network %s, method %s should not be NaN after NaN injection recovery",
					upsID, netID, meth)
				assert.False(t, math.IsInf(score, 0),
					"Score for upstream %s, network %s, method %s should not be Inf after NaN injection recovery",
					upsID, netID, meth)
				assert.GreaterOrEqual(t, score, 0.0,
					"Score for upstream %s, network %s, method %s should be non-negative",
					upsID, netID, meth)
			}
		}
	}

	// Verify sorting still works correctly
	ordered, err := registry.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)

	rpc2Rank := -1
	for i, u := range ordered {
		if u.Id() == "rpc2" {
			rpc2Rank = i
			break
		}
	}
	assert.LessOrEqual(t, rpc2Rank, 2, "rpc2 should still be in the top positions")
}

func TestUpstreamsRegistry_RealErrorsDegradeButHedgeCancellationsDont(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2", "rpc3")

	// rpc1: clean record
	simulateRequests(metricsTracker, ups[0], method, 100, 0)
	// rpc2: 50 hedge cancellations (should be ignored) + 0 real errors
	simulateRequests(metricsTracker, ups[1], method, 100, 0)
	for i := 0; i < 50; i++ {
		metricsTracker.RecordUpstreamRequest(ups[1], method)
		metricsTracker.RecordUpstreamFailure(ups[1], method,
			common.NewErrEndpointRequestCanceled(fmt.Errorf("context canceled")))
	}
	// rpc3: 30 real errors (should degrade score)
	simulateRequests(metricsTracker, ups[2], method, 100, 30)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	s1 := registry.GetUpstreamScore(ups[0].Id(), networkID, method)
	s2 := registry.GetUpstreamScore(ups[1].Id(), networkID, method)
	s3 := registry.GetUpstreamScore(ups[2].Id(), networkID, method)

	// rpc2 (hedge cancellations only) should score similarly to rpc1 (clean)
	assert.InDelta(t, s1, s2, 0.05,
		"upstream with hedge cancellations should score similarly to clean upstream")
	// rpc3 (real errors) should score worse than both
	assert.Greater(t, s1, s3, "clean upstream should score higher than one with real errors")
	assert.Greater(t, s2, s3, "upstream with hedge cancellations should score higher than one with real errors")
}

// ---------------------------------------------------------------------------
// GetUpstreamScoreBreakdown
// ---------------------------------------------------------------------------

func TestUpstreamsRegistry_GetUpstreamScoreBreakdown(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	defer util.AssertNoPendingMocks(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.Logger
	registry, metricsTracker := createTestRegistry(ctx, "test-project", &logger, 10*time.Second)

	method := "eth_call"
	networkID := "evm:123"
	l, _ := registry.GetSortedUpstreams(ctx, networkID, method)
	ups := getUpsByID(l, "rpc1", "rpc2")

	// rpc1: some errors and latency
	simulateRequests(metricsTracker, ups[0], method, 100, 20)
	simulateRequestsWithLatency(metricsTracker, ups[0], method, 50, 0.100)
	// rpc2: clean
	simulateRequestsWithLatency(metricsTracker, ups[1], method, 100, 0.020)

	err := registry.RefreshUpstreamNetworkMethodScores()
	require.NoError(t, err)

	// Inject positive and negative Inf into the scores map
	i := 0
	registry.upstreamsMu.Lock()
	for upsID := range registry.upstreamScores {
		if registry.upstreamScores[upsID][networkID] == nil {
			registry.upstreamScores[upsID][networkID] = make(map[string]float64)
		}
		if i%2 == 0 {
			registry.upstreamScores[upsID][networkID][method] = math.Inf(1) // +Inf
		} else {
			registry.upstreamScores[upsID][networkID][method] = math.Inf(-1) // -Inf
		}
		i++
	}
	registry.upstreamsMu.Unlock()

	// Basic sanity: scores should be in (0, 1]
	assert.Greater(t, bd1.Score, 0.0)
	assert.LessOrEqual(t, bd1.Score, 1.0)
	assert.Greater(t, bd2.Score, 0.0)
	assert.LessOrEqual(t, bd2.Score, 1.0)

	// rpc1 has errors so its error rate should be > 0
	assert.Greater(t, bd1.ErrorRate, 0.0, "rpc1 should have nonzero error rate")
	assert.Equal(t, 0.0, bd2.ErrorRate, "rpc2 should have zero error rate")

	// Verify all scores are now valid (not NaN/Inf)
	for upsID, networkScores := range snapshotUpstreamScores(registry) {
		for netID, methodScores := range networkScores {
			for meth, score := range methodScores {
				assert.False(t, math.IsNaN(score),
					"Score for upstream %s, network %s, method %s should not be NaN after Inf injection recovery",
					upsID, netID, meth)
				assert.False(t, math.IsInf(score, 0),
					"Score for upstream %s, network %s, method %s should not be Inf after Inf injection recovery",
					upsID, netID, meth)
			}
		}
	}
}
