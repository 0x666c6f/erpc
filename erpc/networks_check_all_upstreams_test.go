package erpc

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
)

// upstreamCheckEntry mirrors allUpstreamsCheckUpstreamResult for JSON decoding
// in tests. Field tags must stay in sync with the production struct.
type upstreamCheckEntry struct {
	ID                 string `json:"id"`
	Vendor             string `json:"vendor"`
	Succeeded          bool   `json:"succeeded"`
	ExecutionException bool   `json:"executionException,omitempty"`
	RetryableSkip      bool   `json:"retryableSkip,omitempty"`
	DurationMs         int64  `json:"durationMs"`
	ResultSize         int    `json:"resultSize,omitempty"`
	Error              string `json:"error,omitempty"`
	ErrorCode          string `json:"errorCode,omitempty"`
	ErrorSummary       string `json:"errorSummary,omitempty"`
	ErrorFingerprint   string `json:"errorFingerprint,omitempty"`
}

type upstreamCheckResult struct {
	AllSucceeded        bool                 `json:"allSucceeded"`
	Total               int                  `json:"total"`
	Succeeded           int                  `json:"succeeded"`
	ExecutionExceptions int                  `json:"executionExceptions"`
	Failed              int                  `json:"failed"`
	Upstreams           []upstreamCheckEntry `json:"upstreams"`
}

func TestNetworkForwardCheckAllUpstreams(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, upstreamURL := range []string{"http://rpc1.localhost", "http://rpc2.localhost"} {
		gock.New(upstreamURL).
			Post("").
			Persist().
			Filter(func(request *http.Request) bool {
				return strings.Contains(util.SafeReadBody(request), "eth_call")
			}).
			Reply(200).
			JSON(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      1,
				"result":  "0x1234",
			})
	}

	gock.New("http://rpc3.localhost").
		Post("").
		Persist().
		Filter(func(request *http.Request) bool {
			return strings.Contains(util.SafeReadBody(request), "eth_call")
		}).
		Reply(500).
		JSON(map[string]interface{}{
			"error": "payload too large",
		})

	network := setupTestNetworkForTiming(t, ctx, &common.FailsafeConfig{})

	req := common.NewNormalizedRequest([]byte(`{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "eth_call",
		"params": [{"data":"0x1234"}, "latest"]
	}`))
	req.SetDirectives(&common.RequestDirectives{CheckAllUpstreams: true})

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	jrr, err := resp.JsonRpcResponse(ctx)
	require.NoError(t, err)
	require.NotNil(t, jrr)
	require.Nil(t, jrr.Error)

	var result upstreamCheckResult
	require.NoError(t, json.Unmarshal(jrr.GetResultBytes(), &result))
	require.False(t, result.AllSucceeded)
	require.Equal(t, 3, result.Total)
	require.Equal(t, 2, result.Succeeded)
	require.Equal(t, 0, result.ExecutionExceptions)
	require.Equal(t, 1, result.Failed)
	require.Len(t, result.Upstreams, 3)

	seen := map[string]upstreamCheckEntry{}
	for _, u := range result.Upstreams {
		seen[u.ID] = u
	}
	require.Len(t, seen, 3)

	for _, id := range []string{"rpc1", "rpc2"} {
		entry := seen[id]
		require.True(t, entry.Succeeded, id)
		require.False(t, entry.ExecutionException, id)
		require.Empty(t, entry.Error, id)
		require.Empty(t, entry.ErrorCode, id)
		require.Greater(t, entry.ResultSize, 0, "ResultSize should reflect non-empty result for %s", id)
		require.GreaterOrEqual(t, entry.DurationMs, int64(0), id)
	}

	rpc3 := seen["rpc3"]
	require.False(t, rpc3.Succeeded)
	require.False(t, rpc3.ExecutionException)
	require.Contains(t, rpc3.Error, "payload too large")
	require.NotEmpty(t, rpc3.ErrorSummary)
	require.NotEmpty(t, rpc3.ErrorFingerprint)
}

// TestNetworkForwardCheckAllUpstreams_ExecutionException covers the headline
// behavior: an upstream that returns a JSON-RPC revert with data must be
// reported as Succeeded=true with ExecutionException=true, and counted under
// `executionExceptions` in the summary.
func TestNetworkForwardCheckAllUpstreams_ExecutionException(t *testing.T) {
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// rpc1 + rpc2 succeed; rpc3 returns an EVM revert with data.
	for _, upstreamURL := range []string{"http://rpc1.localhost", "http://rpc2.localhost"} {
		gock.New(upstreamURL).
			Post("").
			Persist().
			Filter(func(request *http.Request) bool {
				return strings.Contains(util.SafeReadBody(request), "eth_call")
			}).
			Reply(200).
			JSON(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      1,
				"result":  "0x1234",
			})
	}

	gock.New("http://rpc3.localhost").
		Post("").
		Persist().
		Filter(func(request *http.Request) bool {
			return strings.Contains(util.SafeReadBody(request), "eth_call")
		}).
		Reply(200).
		JSON(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"error": map[string]interface{}{
				"code":    3,
				"message": "execution reverted",
				"data":    "0x08c379a0",
			},
		})

	network := setupTestNetworkForTiming(t, ctx, &common.FailsafeConfig{})

	req := common.NewNormalizedRequest([]byte(`{
		"jsonrpc": "2.0",
		"id": 1,
		"method": "eth_call",
		"params": [{"data":"0x1234"}, "latest"]
	}`))
	req.SetDirectives(&common.RequestDirectives{CheckAllUpstreams: true})

	resp, err := network.Forward(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer resp.Release()

	jrr, err := resp.JsonRpcResponse(ctx)
	require.NoError(t, err)
	require.NotNil(t, jrr)
	require.Nil(t, jrr.Error)

	var result upstreamCheckResult
	require.NoError(t, json.Unmarshal(jrr.GetResultBytes(), &result))
	require.True(t, result.AllSucceeded, "every upstream executed (revert counts as execution)")
	require.Equal(t, 3, result.Total)
	require.Equal(t, 3, result.Succeeded)
	require.Equal(t, 1, result.ExecutionExceptions)
	require.Equal(t, 0, result.Failed)
	require.Len(t, result.Upstreams, 3)

	var rpc3 upstreamCheckEntry
	for _, u := range result.Upstreams {
		if u.ID == "rpc3" {
			rpc3 = u
		}
	}
	require.True(t, rpc3.Succeeded)
	require.True(t, rpc3.ExecutionException)
	// ErrorCode is the outermost StandardError code; it may be the upstream-
	// request wrapper, but it must be populated so operators can correlate
	// telemetry. The execution-exception classification is conveyed via
	// ExecutionException above (which walks the wrap chain).
	require.NotEmpty(t, rpc3.ErrorCode)
	require.NotEmpty(t, rpc3.ErrorSummary)
	require.NotEmpty(t, rpc3.ErrorFingerprint)
}
