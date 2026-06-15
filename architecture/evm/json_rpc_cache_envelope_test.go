package evm

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/util"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type testNetworkCacheEnvelope struct {
	cfg *common.NetworkConfig
}

var _ common.Network = (*testNetworkCacheEnvelope)(nil)

func (n *testNetworkCacheEnvelope) Id() string { return "evm:8453" }
func (n *testNetworkCacheEnvelope) Label() string {
	return "evm:8453"
}
func (n *testNetworkCacheEnvelope) ProjectId() string { return "test" }
func (n *testNetworkCacheEnvelope) Architecture() common.NetworkArchitecture {
	return common.ArchitectureEvm
}
func (n *testNetworkCacheEnvelope) Config() *common.NetworkConfig { return n.cfg }
func (n *testNetworkCacheEnvelope) Logger() *zerolog.Logger       { return &log.Logger }
func (n *testNetworkCacheEnvelope) GetMethodMetrics(method string) common.TrackedMetrics {
	return nil
}
func (n *testNetworkCacheEnvelope) Cache() common.CacheDAL { return nil }
func (n *testNetworkCacheEnvelope) Forward(ctx context.Context, nq *common.NormalizedRequest) (*common.NormalizedResponse, error) {
	panic("not used")
}
func (n *testNetworkCacheEnvelope) GetFinality(ctx context.Context, req *common.NormalizedRequest, resp *common.NormalizedResponse) common.DataFinalityState {
	return common.DataFinalityStateUnknown
}
func (n *testNetworkCacheEnvelope) EvmHighestLatestBlockNumber(ctx context.Context) int64 {
	return 0
}
func (n *testNetworkCacheEnvelope) EvmHighestFinalizedBlockNumber(ctx context.Context) int64 {
	return 0
}
func (n *testNetworkCacheEnvelope) EvmLeaderUpstream(ctx context.Context) common.Upstream { return nil }

func newTestEvmJsonRpcCacheWithEmptyAllow(t *testing.T, ctx context.Context, lg *zerolog.Logger) *EvmJsonRpcCache {
	t.Helper()

	cfg := &common.CacheConfig{
		Envelope: util.BoolPtr(true),
		Connectors: []*common.ConnectorConfig{
			{
				Id:     "mem",
				Driver: common.DriverMemory,
				Memory: &common.MemoryConnectorConfig{
					MaxItems:     10_000,
					MaxTotalSize: "64MiB",
				},
			},
		},
		Policies: []*common.CachePolicyConfig{
			{
				Network:   "*",
				Method:    "*",
				Finality:  common.DataFinalityStateUnknown,
				Empty:     common.CacheEmptyBehaviorAllow,
				Connector: "mem",
				TTL:       common.FixedDuration(0),
			},
		},
	}

	cache, err := NewEvmJsonRpcCache(ctx, lg, cfg)
	require.NoError(t, err)
	return cache.WithProjectId("p1")
}

func newTestEvmCacheRequest() *common.NormalizedRequest {
	jrq := common.NewJsonRpcRequest("eth_call", []interface{}{
		map[string]interface{}{
			"to":   "0x9896a8605763106e57A51aa0a97Fe8099E806bb3",
			"data": "0x18160ddd",
		},
		"0x1a59129",
	})
	req := common.NewNormalizedRequestFromJsonRpcRequest(jrq)
	req.SetNetwork(&testNetworkCacheEnvelope{cfg: &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 999}}})
	return req
}

func TestEvmJsonRpcCache_SkipsJsonRpcPayloadMissingResultAndError(t *testing.T) {
	util.ConfigureTestLogger()

	ctx := context.Background()
	lg := log.Logger
	cache := newTestEvmJsonRpcCacheWithEmptyAllow(t, ctx, &lg)
	req := newTestEvmCacheRequest()

	missingResult, err := common.NewJsonRpcResponseFromBytes(nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, missingResult.SetID(1))
	resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(missingResult)

	require.NoError(t, cache.Set(ctx, req, resp))

	gotResp, err := cache.Get(ctx, req)
	require.NoError(t, err)
	require.Nil(t, gotResp)
}

func TestEvmJsonRpcCache_TreatsCachedPayloadMissingResultAndErrorAsMiss(t *testing.T) {
	util.ConfigureTestLogger()

	ctx := context.Background()
	lg := log.Logger
	cache := newTestEvmJsonRpcCacheWithEmptyAllow(t, ctx, &lg)
	req := newTestEvmCacheRequest()

	blockRef, _, err := ExtractBlockReferenceFromRequest(ctx, req)
	require.NoError(t, err)
	groupKey, requestKey, err := generateKeysForJsonRpcRequest(req, blockRef, ctx)
	require.NoError(t, err)

	policies := cache.currentPolicySnapshot().policies
	require.Len(t, policies, 1)
	connector := policies[0].GetConnector()
	payload, wrapped := wrapCacheEnvelope(nil)
	require.True(t, wrapped)
	require.NoError(t, connector.Set(ctx, groupKey, requestKey, payload, policies[0].GetTTL()))
	deadline := time.Now().Add(1 * time.Second)
	for {
		_, err = connector.Get(ctx, data.ConnectorMainIndex, groupKey, requestKey, req)
		if err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "malformed fixture cache entry did not become visible: %v", err)
		time.Sleep(10 * time.Millisecond)
	}

	gotResp, err := cache.Get(ctx, req)
	require.NoError(t, err)
	require.Nil(t, gotResp)
}

func TestEvmJsonRpcCache_Envelope_RoundTrip(t *testing.T) {
	util.ConfigureTestLogger()

	ctx := context.Background()
	lg := log.Logger

	waitHit := func(t *testing.T, cache *EvmJsonRpcCache, req *common.NormalizedRequest) *common.NormalizedResponse {
		t.Helper()
		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) {
			r, err := cache.Get(ctx, req)
			require.NoError(t, err)
			if r != nil {
				return r
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	t.Run("uncompressed", func(t *testing.T) {
		cfg := &common.CacheConfig{
			Envelope: util.BoolPtr(true),
			Connectors: []*common.ConnectorConfig{
				{
					Id:     "mem",
					Driver: common.DriverMemory,
					Memory: &common.MemoryConnectorConfig{
						MaxItems:     10_000,
						MaxTotalSize: "64MiB",
					},
				},
			},
			Policies: []*common.CachePolicyConfig{
				{
					Network:   "*",
					Method:    "*",
					Finality:  common.DataFinalityStateUnknown,
					Empty:     common.CacheEmptyBehaviorAllow,
					Connector: "mem",
					TTL:       common.FixedDuration(0),
				},
			},
		}

		cache, err := NewEvmJsonRpcCache(ctx, &lg, cfg)
		require.NoError(t, err)
		cache = cache.WithProjectId("p1")

		jrq := common.NewJsonRpcRequest("eth_getBlockByNumber", []interface{}{"0x1", false})
		req := common.NewNormalizedRequestFromJsonRpcRequest(jrq)
		req.SetNetwork(&testNetworkCacheEnvelope{cfg: &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 8453}}})

		want := []byte(`{"number":"0x1"}`)
		jrr, err := common.NewJsonRpcResponseFromBytes(nil, want, nil)
		require.NoError(t, err)
		_ = jrr.SetID(1)

		resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
		require.NoError(t, cache.Set(ctx, req, resp))

		gotResp := waitHit(t, cache, req)
		require.NotNil(t, gotResp)
		gotJrr, err := gotResp.JsonRpcResponse(ctx)
		require.NoError(t, err)
		require.Equal(t, want, gotJrr.GetResultBytes())
	})

	t.Run("compressed", func(t *testing.T) {
		cfg := &common.CacheConfig{
			Envelope: util.BoolPtr(true),
			Compression: &common.CompressionConfig{
				Enabled:   util.BoolPtr(true),
				Algorithm: "zstd",
				ZstdLevel: "fastest",
				Threshold: 1,
			},
			Connectors: []*common.ConnectorConfig{
				{
					Id:     "mem",
					Driver: common.DriverMemory,
					Memory: &common.MemoryConnectorConfig{
						MaxItems:     10_000,
						MaxTotalSize: "256MiB",
					},
				},
			},
			Policies: []*common.CachePolicyConfig{
				{
					Network:   "*",
					Method:    "*",
					Finality:  common.DataFinalityStateUnknown,
					Empty:     common.CacheEmptyBehaviorAllow,
					Connector: "mem",
					TTL:       common.FixedDuration(0),
				},
			},
		}

		cache, err := NewEvmJsonRpcCache(ctx, &lg, cfg)
		require.NoError(t, err)
		cache = cache.WithProjectId("p1")

		jrq := common.NewJsonRpcRequest("eth_getBlockByNumber", []interface{}{"0x1", false})
		req := common.NewNormalizedRequestFromJsonRpcRequest(jrq)
		req.SetNetwork(&testNetworkCacheEnvelope{cfg: &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 8453}}})

		// Large JSON string to exercise the streaming decompression path.
		want := []byte(`{"data":"` + strings.Repeat("x", 4096) + `"}`)
		jrr, err := common.NewJsonRpcResponseFromBytes(nil, want, nil)
		require.NoError(t, err)
		_ = jrr.SetID(1)

		resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
		require.NoError(t, cache.Set(ctx, req, resp))

		gotResp := waitHit(t, cache, req)
		require.NotNil(t, gotResp)
		gotJrr, err := gotResp.JsonRpcResponse(ctx)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = gotJrr.WriteResultTo(&buf, false)
		require.NoError(t, err)
		require.Equal(t, want, buf.Bytes())
	})
}
