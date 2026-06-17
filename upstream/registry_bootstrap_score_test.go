package upstream

import (
	"context"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/thirdparty"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// TestUpstreamsRegistry_Bootstrap_SchedulesScoreRefresh is a regression guard
// for PLA-1620: the upstream-sync merge dropped the
// `u.scheduleScoreCalculationTimers(ctx)` call from Bootstrap, which silently
// disabled ALL score-based/rendezvous routing in production (GetSortedUpstreams
// kept serving upstreams in static registration order forever). The bug was
// invisible to the rest of the suite because every other scoring test calls
// RefreshUpstreamNetworkMethodScores() manually.
//
// This test deliberately does NOT call RefreshUpstreamNetworkMethodScores(): it
// asserts that Bootstrap alone drives the refresh loop, so a faster upstream is
// promoted ahead of a slower one without any manual nudge.
func TestUpstreamsRegistry_Bootstrap_SchedulesScoreRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := &log.Logger
	const networkID = "evm:123"
	const projectID = "test-bootstrap-score-refresh"
	const method = "eth_getBalance"

	mt := health.NewTracker(logger, projectID, time.Minute)
	mt.Bootstrap(ctx)

	vr := thirdparty.NewVendorsRegistry()
	pr, err := thirdparty.NewProvidersRegistry(logger, vr, nil, nil)
	require.NoError(t, err)
	rlr, err := NewRateLimitersRegistry(ctx, nil, logger)
	require.NoError(t, err)
	ssr, err := data.NewSharedStateRegistry(ctx, logger, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"},
		},
	})
	require.NoError(t, err)

	// Short refresh interval so the Bootstrap-scheduled timer fires quickly;
	// per-method granularity so the refresh reads exactly the method bucket we
	// feed below (avoids depending on the "*" aggregate).
	reg := NewUpstreamsRegistry(
		ctx, logger, projectID, nil, ssr, rlr, vr, pr, nil, mt,
		50*time.Millisecond,
		&ScoringConfig{RoutingStrategy: "score-based", ScoreGranularity: "method"},
	)

	mkUps := func(id, endpoint string) *Upstream {
		cfg := &common.UpstreamConfig{
			Id:         id,
			Type:       common.UpstreamTypeEvm,
			Endpoint:   endpoint,
			VendorName: "memory",
			Evm:        &common.EvmUpstreamConfig{ChainId: 123},
		}
		ups, err := reg.NewUpstream(cfg)
		require.NoError(t, err)
		ups.SetNetworkConfig(&common.NetworkConfig{
			Architecture: common.ArchitectureEvm,
			Alias:        networkID,
			Evm:          &common.EvmNetworkConfig{ChainId: 123},
		})
		reg.doRegisterBootstrappedUpstream(ups)
		return ups
	}

	// Registration order is [slow, fast]; score-based routing must flip it.
	slow := mkUps("rpc-slow", "http://slow.localhost")
	fast := mkUps("rpc-fast", "http://fast.localhost")

	// Prime the sorted cache for this (network, method) so the refresh loop has
	// a work item, and assert the precondition: registration order is [slow, fast].
	primed, err := reg.GetSortedUpstreams(ctx, networkID, method)
	require.NoError(t, err)
	require.Len(t, primed, 2)
	require.Equal(t, "rpc-slow", primed[0].Id(), "precondition: registration order must be [slow, fast]")

	// Inject a large, unambiguous latency gap (RespLatency has the dominant
	// default multiplier of 8.0). Recording with a concrete finality also feeds
	// the all-finalities bucket the refresh reads.
	for i := 0; i < 50; i++ {
		mt.RecordUpstreamRequest(slow, method, common.DataFinalityStateUnfinalized)
		mt.RecordUpstreamDuration(slow, method, 1500*time.Millisecond, true, "none", common.DataFinalityStateUnfinalized)
		mt.RecordUpstreamRequest(fast, method, common.DataFinalityStateUnfinalized)
		mt.RecordUpstreamDuration(fast, method, 2*time.Millisecond, true, "none", common.DataFinalityStateUnfinalized)
	}

	// Bootstrap MUST schedule the score-refresh timer. No manual refresh here.
	reg.Bootstrap(ctx)

	require.Eventually(t, func() bool {
		list, e := reg.GetSortedUpstreams(ctx, networkID, method)
		return e == nil && len(list) == 2 && list[0].Id() == "rpc-fast"
	}, 5*time.Second, 50*time.Millisecond,
		"Bootstrap must schedule scheduleScoreCalculationTimers so the faster upstream "+
			"is ranked first automatically; if this times out, the score-refresh timer "+
			"was dropped from UpstreamsRegistry.Bootstrap (PLA-1620 regression)")
}
