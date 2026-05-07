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

func TestNetworkForwardCheckAllUpstreams(t *testing.T) {
	util.SetupMocksForEvmStatePoller()
	defer util.ResetGock()

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

	var result struct {
		AllSucceeded bool `json:"allSucceeded"`
		Total        int  `json:"total"`
		Succeeded    int  `json:"succeeded"`
		Failed       int  `json:"failed"`
		Upstreams    []struct {
			ID        string `json:"id"`
			Succeeded bool   `json:"succeeded"`
			Error     string `json:"error,omitempty"`
		} `json:"upstreams"`
	}
	require.NoError(t, json.Unmarshal(jrr.GetResultBytes(), &result))
	require.False(t, result.AllSucceeded)
	require.Equal(t, 3, result.Total)
	require.Equal(t, 2, result.Succeeded)
	require.Equal(t, 1, result.Failed)
	require.Len(t, result.Upstreams, 3)

	seen := map[string]bool{}
	for _, upstream := range result.Upstreams {
		seen[upstream.ID] = true
		if upstream.ID == "rpc3" {
			require.False(t, upstream.Succeeded)
			require.Contains(t, upstream.Error, "payload too large")
		} else {
			require.True(t, upstream.Succeeded)
			require.Empty(t, upstream.Error)
		}
	}
	require.Equal(t, map[string]bool{"rpc1": true, "rpc2": true, "rpc3": true}, seen)
}
