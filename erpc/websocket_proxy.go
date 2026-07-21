package erpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/erpc/erpc/auth"
	"github.com/erpc/erpc/common"
)

const websocketConnectMethod = "websocket_connect"

var errWebsocketPolicyViolation = errors.New("WebSocket request rejected")

type websocketManager struct {
	cfg         *common.WebsocketServerConfig
	mu          sync.Mutex
	connections map[*websocket.Conn]context.CancelFunc
	byUser      map[string]int
}

func newWebsocketManager(cfg *common.WebsocketServerConfig) *websocketManager {
	if cfg == nil {
		cfg = &common.WebsocketServerConfig{}
	}
	if cfg.DialTimeout == nil {
		d := common.Duration(10 * time.Second)
		cfg.DialTimeout = &d
	}
	if cfg.IdleTimeout == nil {
		d := common.Duration(5 * time.Minute)
		cfg.IdleTimeout = &d
	}
	if cfg.MaxLifetime == nil {
		d := common.Duration(24 * time.Hour)
		cfg.MaxLifetime = &d
	}
	if cfg.MaxConnectionsPerUser == nil {
		v := 10
		cfg.MaxConnectionsPerUser = &v
	}
	return &websocketManager{cfg: cfg, connections: make(map[*websocket.Conn]context.CancelFunc), byUser: make(map[string]int)}
}

func (m *websocketManager) acquire(userID string) (func(), bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byUser[userID] >= *m.cfg.MaxConnectionsPerUser {
		return nil, false
	}
	m.byUser[userID]++
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.byUser[userID]--
		if m.byUser[userID] == 0 {
			delete(m.byUser, userID)
		}
	}, true
}

func (m *websocketManager) register(conn *websocket.Conn, cancel context.CancelFunc) func() {
	m.mu.Lock()
	m.connections[conn] = cancel
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		delete(m.connections, conn)
		m.mu.Unlock()
	}
}

func (m *websocketManager) shutdown() {
	m.mu.Lock()
	connections := make(map[*websocket.Conn]context.CancelFunc, len(m.connections))
	for conn, cancel := range m.connections {
		connections[conn] = cancel
	}
	m.mu.Unlock()
	for conn, cancel := range connections {
		cancel()
		if conn != nil {
			_ = conn.CloseNow()
		}
	}
}

func headerHasToken(header http.Header, name, want string) bool {
	for _, value := range header.Values(name) {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func websocketUpgradeRequested(r *http.Request) bool {
	return r != nil && r.Method == http.MethodGet && headerHasToken(r.Header, "Upgrade", "websocket") && headerHasToken(r.Header, "Connection", "upgrade")
}

func (s *HttpServer) handleWebsocket(requestCtx context.Context, w http.ResponseWriter, r *http.Request, project *PreparedProject, architecture, chainID string) {
	if project == nil {
		http.Error(w, "project not found", http.StatusNotFound)
		return
	}
	if !websocketOriginAllowed(r, project.Config.CORS) {
		http.Error(w, "WebSocket origin denied", http.StatusForbidden)
		return
	}

	networkID := fmt.Sprintf("%s:%s", architecture, chainID)
	network, err := project.GetNetwork(requestCtx, networkID)
	if err != nil {
		http.Error(w, "network not found", http.StatusNotFound)
		return
	}
	candidates := websocketUpstreamCandidates(network)
	if len(candidates) == 0 {
		http.Error(w, "no eligible WebSocket upstream configured", http.StatusServiceUnavailable)
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
	user, err := project.AuthenticateWebsocket(requestCtx, nq, websocketConnectMethod, payload)
	if err != nil || user == nil {
		http.Error(w, "WebSocket access denied", http.StatusUnauthorized)
		return
	}
	nq.SetUser(user)

	releaseUser, ok := s.websocketManager.acquire(user.Id)
	if !ok {
		http.Error(w, "WebSocket connection limit exceeded", http.StatusTooManyRequests)
		return
	}
	defer releaseUser()

	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Time{})
	_ = controller.SetWriteDeadline(time.Time{})
	clientConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	defer clientConn.Close(websocket.StatusNormalClosure, "connection closed")
	clientConn.SetReadLimit(8 * 1024 * 1024)

	connectionCtx, cancel := context.WithTimeout(s.appCtx, s.websocketManager.cfg.MaxLifetime.Duration())
	defer cancel()
	unregister := s.websocketManager.register(clientConn, cancel)
	defer unregister()

	upstreamConn, upstreamID, err := dialWebsocketUpstream(connectionCtx, candidates, s.websocketManager.cfg.DialTimeout.Duration())
	if err != nil {
		s.logger.Warn().Err(err).Str("projectId", project.Config.Id).Str("networkId", networkID).Msg("failed to connect to WebSocket upstreams")
		_ = clientConn.Close(websocket.StatusInternalError, "failed to connect to upstream")
		return
	}
	defer upstreamConn.Close(websocket.StatusNormalClosure, "client disconnected")
	upstreamConn.SetReadLimit(8 * 1024 * 1024)

	authorize := func(frameType websocket.MessageType, frame []byte) error {
		if err := authorizeWebsocketFrame(connectionCtx, frameType, frame, project, network, payload, nq.ClientIP()); err != nil {
			return fmt.Errorf("%w: %v", errWebsocketPolicyViolation, err)
		}
		return nil
	}
	if err := bridgeWebsockets(connectionCtx, clientConn, upstreamConn, s.websocketManager.cfg.IdleTimeout.Duration(), authorize); err != nil {
		if errors.Is(err, errWebsocketPolicyViolation) {
			_ = clientConn.Close(websocket.StatusPolicyViolation, "WebSocket request denied")
		}
		s.logger.Debug().Err(err).Str("projectId", project.Config.Id).Str("networkId", networkID).Str("upstreamId", upstreamID).Msg("WebSocket proxy closed")
	}
}

type websocketUpstreamCandidate struct {
	id       string
	endpoint string
	headers  http.Header
}

func websocketUpstreamCandidates(network *Network) []websocketUpstreamCandidate {
	upstreams := network.AllUpstreams()
	if len(upstreams) == 0 {
		return nil
	}
	byID := make(map[string]int, len(upstreams))
	for i, upstream := range upstreams {
		byID[upstream.Id()] = i
	}
	orderedIDs := network.PolicyOrderedUpstreams("eth_subscribe")
	if len(orderedIDs) == 0 {
		orderedIDs = make([]string, 0, len(upstreams))
		for _, upstream := range upstreams {
			orderedIDs = append(orderedIDs, upstream.Id())
		}
	}
	candidates := make([]websocketUpstreamCandidate, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		index, ok := byID[id]
		if !ok {
			continue
		}
		upstream := upstreams[index]
		cfg := upstream.Config()
		if cfg.WebsocketEndpoint == "" || (cfg.Shadow != nil && cfg.Shadow.Enabled) || upstream.EvmSyncingState() == common.EvmSyncingStateSyncing {
			continue
		}
		eligible, err := upstream.ShouldHandleMethod("eth_subscribe")
		if err != nil || !eligible {
			continue
		}
		parsed, err := url.Parse(cfg.WebsocketEndpoint)
		if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
			continue
		}
		headers := make(http.Header)
		if cfg.JsonRpc != nil {
			for key, value := range cfg.JsonRpc.Headers {
				headers.Set(key, value)
			}
		}
		candidates = append(candidates, websocketUpstreamCandidate{id: id, endpoint: cfg.WebsocketEndpoint, headers: headers})
	}
	return candidates
}

func dialWebsocketUpstream(ctx context.Context, candidates []websocketUpstreamCandidate, timeout time.Duration) (*websocket.Conn, string, error) {
	var errs []error
	for _, candidate := range candidates {
		dialCtx, cancel := context.WithTimeout(ctx, timeout)
		conn, response, err := websocket.Dial(dialCtx, candidate.endpoint, &websocket.DialOptions{HTTPHeader: candidate.headers})
		cancel()
		if err == nil {
			return conn, candidate.id, nil
		}
		if response != nil {
			_ = response.Body.Close()
		}
		errs = append(errs, fmt.Errorf("upstream %s: %w", candidate.id, err))
	}
	return nil, "", errors.Join(errs...)
}

func authorizeWebsocketFrame(ctx context.Context, messageType websocket.MessageType, frame []byte, project *PreparedProject, network *Network, payload *auth.AuthPayload, clientIP string) error {
	if messageType != websocket.MessageText {
		return fmt.Errorf("binary WebSocket frames are not supported")
	}
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return fmt.Errorf("empty WebSocket frame")
	}
	requests := []json.RawMessage{trimmed}
	if trimmed[0] == byte([) {
		if err := common.SonicCfg.Unmarshal(trimmed, &requests); err != nil || len(requests) == 0 {
			return fmt.Errorf("invalid JSON-RPC batch")
		}
	}
	for _, raw := range requests {
		nq := common.NewNormalizedRequest(raw)
		if err := nq.Validate(); err != nil {
			return err
		}
		method, err := nq.Method()
		if err != nil {
			return err
		}
		allowed, err := project.SupportsMethod(method)
		if err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("method not supported: %s", method)
		}
		nq.SetClientIP(clientIP)
		nq.SetNetwork(network)
		methodPayload := *payload
		methodPayload.Method = method
		user, err := project.AuthenticateWebsocket(ctx, nq, method, &methodPayload)
		if err != nil {
			return err
		}
		nq.SetUser(user)
		if err := project.AcquireRateLimitPermit(ctx, nq); err != nil {
			return err
		}
	}
	return nil
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

func bridgeWebsockets(ctx context.Context, clientConn, upstreamConn *websocket.Conn, idleTimeout time.Duration, authorize func(websocket.MessageType, []byte) error) error {
	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	errs := make(chan error, 3)
	copyMessages := func(dst, src *websocket.Conn, authorizeFrame bool) {
		for {
			messageType, frame, err := src.Read(bridgeCtx)
			if err != nil {
				errs <- err
				return
			}
			lastActivity.Store(time.Now().UnixNano())
			if authorizeFrame && authorize != nil {
				if err := authorize(messageType, frame); err != nil {
					errs <- err
					return
				}
			}
			if err := dst.Write(bridgeCtx, messageType, frame); err != nil {
				errs <- err
				return
			}
		}
	}
	go copyMessages(upstreamConn, clientConn, true)
	go copyMessages(clientConn, upstreamConn, false)
	go func() {
		ticker := time.NewTicker(min(idleTimeout/2, time.Minute))
		defer ticker.Stop()
		for {
			select {
			case <-bridgeCtx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, lastActivity.Load())) >= idleTimeout {
					errs <- fmt.Errorf("WebSocket idle timeout exceeded")
					return
				}
			}
		}
	}()
	err := <-errs
	cancel()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return nil
	}
	return err
}
