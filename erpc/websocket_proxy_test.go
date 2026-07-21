package erpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/require"
)

func TestBridgeWebsocketsProxiesBidirectionally(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")
		var message map[string]any
		require.NoError(t, wsjson.Read(r.Context(), conn, &message))
		require.NoError(t, wsjson.Write(r.Context(), conn, message))
	}))
	defer upstream.Close()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientConn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer clientConn.Close(websocket.StatusNormalClosure, "done")
		wsURL := "ws" + strings.TrimPrefix(upstream.URL, "http")
		upstreamConn, _, err := websocket.Dial(r.Context(), wsURL, nil)
		require.NoError(t, err)
		defer upstreamConn.Close(websocket.StatusNormalClosure, "done")
		require.NoError(t, bridgeWebsockets(r.Context(), clientConn, upstreamConn))
	}))
	defer proxy.Close()

	wsURL := "ws" + strings.TrimPrefix(proxy.URL, "http")
	client, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer client.Close(websocket.StatusNormalClosure, "done")

	want := map[string]any{"jsonrpc": "2.0", "id": float64(1), "method": "eth_subscribe", "params": []any{"newHeads"}}
	require.NoError(t, wsjson.Write(ctx, client, want))
	var got map[string]any
	require.NoError(t, wsjson.Read(ctx, client, &got))
	require.Equal(t, want, got)
}
