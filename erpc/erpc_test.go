package erpc

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/internal/policy"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/util"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	telemetry.SetHistogramBuckets("0.05,0.5,5,30")
}

func TestErpc_UpstreamsRegistryCorrectPriorityChange(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()

	// Set up required chainId/latest/finalized/syncing mocks BEFORE any components start
	// so upstream detectFeatures and state pollers don't hang or steal test mocks.
	util.SetupMocksForEvmStatePoller()

	port := rand.Intn(1000) + 2000
	cfg := &common.Config{
		Server: &common.ServerConfig{
			HttpHostV4: util.StringPtr("0.0.0.0"),
			HttpHostV6: util.StringPtr("[::]"),
			HttpPortV4: util.IntPtr(port),
			MaxTimeout: common.Duration(5 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test",
				Networks: []*common.NetworkConfig{
					{
						Architecture: "evm",
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
						},
						Failsafe: []*common.FailsafeConfig{
							{
								Retry: &common.RetryPolicyConfig{
									MaxAttempts: 3,
									Delay:       common.Duration(10 * time.Millisecond),
								},
							},
						},
						SelectionPolicy: &common.SelectionPolicyConfig{
							EvalInterval: common.Duration(100 * time.Millisecond),
							EvalTimeout:  common.Duration(50 * time.Millisecond),
							EvalFunc: `(upstreams, ctx) =>
								upstreams.sortByScore({ errorRate: 5 })`,
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Type:     "evm",
						Endpoint: "http://rpc1.localhost",
						Evm:      &common.EvmUpstreamConfig{ChainId: 123},
						JsonRpc:  &common.JsonRpcUpstreamConfig{SupportsBatch: &common.FALSE},
					},
					{
						Id:       "rpc2",
						Type:     "evm",
						Endpoint: "http://rpc2.localhost",
						Evm:      &common.EvmUpstreamConfig{ChainId: 123},
						JsonRpc:  &common.JsonRpcUpstreamConfig{SupportsBatch: &common.FALSE},
					},
				},
			},
		},
	}

	lg := log.With().Logger()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ssr, err := data.NewSharedStateRegistry(ctx1, &lg, &common.SharedStateConfig{
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{
				MaxItems: 100_000, MaxTotalSize: "1GB",
			},
		},
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	erpcInstance, err := NewERPC(ctx1, &lg, ssr, nil, cfg)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	erpcInstance.Bootstrap(ctx1)

	nw, err := erpcInstance.GetNetwork(ctx1, "test", "evm:123")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	nw.upstreamsRegistry.PrepareUpstreamsForNetwork(ctx1, "evm:123")

	require.Eventually(t, func() bool {
		return len(nw.upstreamsRegistry.GetNetworkUpstreams(ctx1, "evm:123")) == 2
	}, 2*time.Second, 20*time.Millisecond)

	// Seed the tracker directly; gock + retry scheduling makes the final
	// policy order timing-sensitive while exercising the same Record* path.
	for _, ups := range nw.upstreamsRegistry.GetNetworkUpstreams(ctx1, "evm:123") {
		if m := nw.metricsTracker.GetUpstreamMethodMetrics(ups, "*", common.DataFinalityStateAll); m != nil {
			m.Reset()
		}
	}
	policy.ResetSlotStateForTest(nw.policyEngine, "evm:123", "*")
	policy.TickForTest(nw.policyEngine, "evm:123", "*")

	seedDegraded(nw.metricsTracker, upstreamByID(t, nw, "rpc1"), seedSpec{
		method: "eth_getTransactionReceipt",
		failed: 30,
	})
	seedDegraded(nw.metricsTracker, upstreamByID(t, nw, "rpc2"), seedSpec{
		method:       "eth_getTransactionReceipt",
		successful:   30,
		successAvgMs: 10,
	})
	policy.TickForTest(nw.policyEngine, "evm:123", "*")

	sortedUpstreams := nw.policyEngine.GetOrdered("evm:123", "*", "*")
	expectedOrder := []string{"rpc2", "rpc1"}
	assert.Len(t, sortedUpstreams, 2)
	for i, ups := range sortedUpstreams {
		assert.Equal(t, expectedOrder[i], ups.Id())
	}
}
