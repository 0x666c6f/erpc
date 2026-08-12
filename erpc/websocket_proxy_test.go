package erpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/erpc/erpc/auth"
	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestBridgeWebsocketsProxiesBidirectionally(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil { return }
		defer conn.Close(websocket.StatusNormalClosure, "done")
		var message map[string]any
		require.NoError(t, wsjson.Read(r.Context(), conn, &message))
		require.NoError(t, wsjson.Write(r.Context(), conn, message))
	}))
	defer upstream.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientConn, err := websocket.Accept(w, r, nil)
		if err != nil { return }
		defer clientConn.Close(websocket.StatusNormalClosure, "done")
		upstreamConn, _, err := websocket.Dial(r.Context(), "ws"+strings.TrimPrefix(upstream.URL, "http"), nil)
		require.NoError(t, err)
		defer upstreamConn.Close(websocket.StatusNormalClosure, "done")
		require.NoError(t, bridgeWebsockets(r.Context(), clientConn, upstreamConn, time.Hour, nil))
	}))
	defer proxy.Close()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(proxy.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close(websocket.StatusNormalClosure, "done")
	want := map[string]any{"jsonrpc": "2.0", "id": float64(1), "method": "eth_subscribe", "params": []any{"newHeads"}}
	require.NoError(t, wsjson.Write(ctx, client, want))
	var got map[string]any
	require.NoError(t, wsjson.Read(ctx, client, &got))
	require.Equal(t, want, got)
}

func TestParseUrlPathAcceptsWebsocketUpgrade(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/main/evm/1", nil)
	req.Header.Set("Connection", "keep-alive, Upgrade")
	req.Header.Set("Upgrade", "websocket")
	projectID, architecture, chainID, isAdmin, isHealthCheck, err := (&HttpServer{}).parseUrlPath(req, "", "", "")
	require.NoError(t, err)
	require.Equal(t, "main", projectID)
	require.Equal(t, "evm", architecture)
	require.Equal(t, "1", chainID)
	require.False(t, isAdmin)
	require.False(t, isHealthCheck)
}

func TestWebsocketOriginAllowedUsesProjectCORS(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/main/evm/1", nil)
	req.Header.Set("Origin", "https://app.example.com")
	cors := &common.CORSConfig{AllowedOrigins: []string{"https://*.example.com"}}
	require.True(t, websocketOriginAllowed(req, cors))
	req.Header.Set("Origin", "https://evil.example")
	require.False(t, websocketOriginAllowed(req, cors))
}

func TestAuthorizeWebsocketFrameEnforcesMethodFilters(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	registry, err := auth.NewAuthRegistry(context.Background(), &logger, "test", &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{{Type: common.AuthTypeSecret, AllowWebsocket: true, IgnoreMethods: []string{"*"}, AllowMethods: []string{"eth_subscribe"}, Secret: &common.SecretStrategyConfig{Id: "subscriptions", Value: "key"}}}}, nil)
	require.NoError(t, err)
	project := &PreparedProject{Config: &common.ProjectConfig{Id: "test", IgnoreMethods: []string{"*"}, AllowMethods: []string{"eth_subscribe"}}, consumerAuthRegistry: registry}
	network := &Network{}
	payload := &auth.AuthPayload{Type: common.AuthTypeSecret, Secret: &auth.SecretPayload{Value: "key"}}
	allowed := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)
	require.NoError(t, authorizeWebsocketFrame(context.Background(), websocket.MessageText, allowed, project, network, payload, "127.0.0.1"))
	denied := []byte(`{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0x"]}`)
	require.ErrorContains(t, authorizeWebsocketFrame(context.Background(), websocket.MessageText, denied, project, network, payload, "127.0.0.1"), "method not supported")
	batch := []byte(`[{"jsonrpc":"2.0","id":3,"method":"eth_subscribe","params":["newHeads"]},{"jsonrpc":"2.0","id":4,"method":"eth_sendRawTransaction","params":["0x"]}]`)
	require.Error(t, authorizeWebsocketFrame(context.Background(), websocket.MessageText, batch, project, network, payload, "127.0.0.1"))
	require.Error(t, authorizeWebsocketFrame(context.Background(), websocket.MessageBinary, allowed, project, network, payload, "127.0.0.1"))
}

func TestDialWebsocketUpstreamFailsOver(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil { return }
		defer conn.Close(websocket.StatusNormalClosure, "done")
		_, _, _ = conn.Read(context.Background())
	}))
	defer upstream.Close()
	conn, id, err := dialWebsocketUpstream(context.Background(), []websocketUpstreamCandidate{{id: "down", endpoint: "ws://127.0.0.1:1"}, {id: "healthy", endpoint: "ws"+strings.TrimPrefix(upstream.URL, "http")}}, 250*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, "healthy", id)
	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "done"))
}

func TestWebsocketManagerLimitsAndShutsDownConnections(t *testing.T) {
	t.Parallel()
	max := 1
	dial, idle, lifetime := common.Duration(time.Second), common.Duration(time.Minute), common.Duration(time.Hour)
	manager := newWebsocketManager(&common.WebsocketServerConfig{DialTimeout: &dial, IdleTimeout: &idle, MaxLifetime: &lifetime, MaxConnectionsPerUser: &max})
	release, ok := manager.acquire("user")
	require.True(t, ok)
	_, ok = manager.acquire("user")
	require.False(t, ok)
	release()
	_, ok = manager.acquire("user")
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	manager.mu.Lock()
	manager.connections[nil] = cancel
	manager.mu.Unlock()
	manager.shutdown()
	require.Eventually(t, func() bool { return ctx.Err() != nil }, time.Second, 10*time.Millisecond)
}
