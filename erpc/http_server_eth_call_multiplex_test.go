package erpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/architecture/evm"
	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHttpServer_EthCallSyntheticMulticall_BypassesStaleMultiplexer(t *testing.T) {
	var ethCallCount atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]interface{}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		method, _ := req["method"].(string)
		result := "0x1"
		switch method {
		case "eth_chainId":
			result = "0x7b"
		case "eth_blockNumber":
			result = "0x3e8"
		case "eth_call":
			ethCallCount.Add(1)
			encoded, err := evm.EncodeMulticall3Aggregate3Results([]evm.Multicall3Result{
				{Success: true, ReturnData: []byte{0xaa}},
			})
			require.NoError(t, err)
			result = "0x" + hex.EncodeToString(encoded)
		}

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result":  result,
		}))
	}))
	defer upstream.Close()

	cfg := &common.Config{
		Server: &common.ServerConfig{
			MaxTimeout: common.Duration(5 * time.Second).Ptr(),
		},
		Projects: []*common.ProjectConfig{
			{
				Id: "test_project",
				Networks: []*common.NetworkConfig{
					{
						Architecture: common.ArchitectureEvm,
						Evm: &common.EvmNetworkConfig{
							ChainId: 123,
						},
					},
				},
				Upstreams: []*common.UpstreamConfig{
					{
						Id:       "rpc1",
						Type:     common.UpstreamTypeEvm,
						Endpoint: upstream.URL,
						Evm: &common.EvmUpstreamConfig{
							ChainId: 123,
						},
					},
				},
			},
		},
		RateLimiters: &common.RateLimiterConfig{},
	}

	sendRequest, _, _, shutdown, erpcInstance := createServerTestFixtures(cfg, t)
	defer shutdown()

	project, err := erpcInstance.GetProject("test_project")
	require.NoError(t, err)
	network, err := project.GetNetwork(context.Background(), "evm:123")
	require.NoError(t, err)

	perCallReq := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x1111111111111111111111111111111111111111","data":"0x01"},"latest"]}`))
	topReq, _, err := evm.BuildMulticall3Request([]*common.NormalizedRequest{perCallReq}, "latest")
	require.NoError(t, err)

	staleSyntheticReq, _, err := evm.BuildMulticall3Request([]*common.NormalizedRequest{perCallReq}, "latest")
	require.NoError(t, err)
	staleSyntheticReq.SetParentRequestId(topReq.ID())
	staleSyntheticReq.SetCompositeType(common.CompositeTypeMulticall3)
	hash, err := staleSyntheticReq.CacheHash()
	require.NoError(t, err)
	network.inFlightRequests.Store(hash, NewMultiplexer(hash))

	jrq, err := topReq.JsonRpcRequest()
	require.NoError(t, err)
	body, err := common.SonicCfg.Marshal(jrq)
	require.NoError(t, err)

	startedAt := time.Now()
	statusCode, _, respBody := sendRequest(string(body), nil, nil)
	elapsed := time.Since(startedAt)

	require.Equal(t, http.StatusOK, statusCode)
	assert.Contains(t, respBody, `"result":"0x`)
	assert.NotContains(t, respBody, `"error"`)
	assert.Less(t, elapsed, 500*time.Millisecond, "http path should bypass stale synthetic multiplexer instead of waiting for follower bailout")
	assert.Equal(t, int32(1), ethCallCount.Load(), "synthetic multicall should hit upstream exactly once")
}
