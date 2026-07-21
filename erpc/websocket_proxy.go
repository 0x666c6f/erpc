package erpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/coder/websocket"
	"github.com/erpc/erpc/auth"
	"github.com/erpc/erpc/common"
)

const websocketConnectMethod = "websocket_connect"

func websocketUpgradeRequested(r *http.Request) bool {
	return r != nil && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

func (s *HttpServer) handleWebsocket(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	project *PreparedProject,
	architecture string,
	chainID string,
) {
	if project == nil {
		http.Error(w, "project not found", http.StatusNotFound)
		return
	}

	networkID := fmt.Sprintf("%s:%s", architecture, chainID)
	network, err := project.GetNetwork(ctx, networkID)
	if err != nil {
		http.Error(w, "network not found", http.StatusNotFound)
		return
	}

	nq := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":null,"method":"websocket_connect"}`))
	nq.SetClientIP(s.resolveRealClientIP(r))
	nq.SetNetwork(network)

	payload, err := auth.NewPayloadFromHttp(websocketConnectMethod, r.RemoteAddr, r.Header, r.URL.Query())
	if err != nil {
		http.Error(w, "invalid authentication", http.StatusUnauthorized)
		return
	}
	user, err := project.AuthenticateWebsocket(ctx, nq, websocketConnectMethod, payload)
	if err != nil {
		http.Error(w, "WebSocket access denied", http.StatusUnauthorized)
		return
	}
	nq.SetUser(user)

	endpoint, headers, err := websocketUpstream(network)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !websocketOriginAllowed(r, project.Config.CORS) {
		http.Error(w, "WebSocket origin denied", http.StatusForbidden)
		return
	}

	upstreamConn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		http.Error(w, "failed to connect to WebSocket upstream", http.StatusBadGateway)
		return
	}
	defer upstreamConn.Close(websocket.StatusNormalClosure, "client disconnected")

	clientConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	defer clientConn.Close(websocket.StatusNormalClosure, "upstream disconnected")

	if err := bridgeWebsockets(ctx, clientConn, upstreamConn); err != nil {
		s.logger.Debug().Err(err).Str("projectId", project.Config.Id).Str("networkId", networkID).Msg("WebSocket proxy closed")
	}
}

func websocketUpstream(network *Network) (string, http.Header, error) {
	upstreams := network.AllUpstreams()
	if len(upstreams) == 0 {
		return "", nil, fmt.Errorf("no WebSocket upstream configured")
	}

	orderedIDs := network.PolicyOrderedUpstreams("eth_subscribe")
	ordered := make([]string, 0, len(upstreams))
	ordered = append(ordered, orderedIDs...)
	for _, upstream := range upstreams {
		ordered = append(ordered, upstream.Id())
	}

	seen := make(map[string]struct{}, len(ordered))
	for _, id := range ordered {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		for _, upstream := range upstreams {
			if upstream.Id() != id || upstream.Config().WebsocketEndpoint == "" {
				continue
			}
			endpoint := upstream.Config().WebsocketEndpoint
			parsed, err := url.Parse(endpoint)
			if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
				continue
			}
			headers := make(http.Header)
			if upstream.Config().JsonRpc != nil {
				for key, value := range upstream.Config().JsonRpc.Headers {
					headers.Set(key, value)
				}
			}
			return endpoint, headers, nil
		}
	}
	return "", nil, fmt.Errorf("no WebSocket upstream configured")
}

func websocketOriginAllowed(r *http.Request, cors *common.CORSConfig) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if cors == nil {
		return false
	}
	for _, pattern := range cors.AllowedOrigins {
		matched, err := common.WildcardMatch(pattern, origin)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func bridgeWebsockets(ctx context.Context, clientConn *websocket.Conn, upstreamConn *websocket.Conn) error {
	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 2)
	copyMessages := func(dst *websocket.Conn, src *websocket.Conn) {
		for {
			messageType, payload, err := src.Read(bridgeCtx)
			if err != nil {
				errs <- err
				return
			}
			if err := dst.Write(bridgeCtx, messageType, payload); err != nil {
				errs <- err
				return
			}
		}
	}
	go copyMessages(upstreamConn, clientConn)
	go copyMessages(clientConn, upstreamConn)

	err := <-errs
	cancel()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return nil
	}
	return err
}
