package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/erpc/erpc/auth"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/upstream"
	"github.com/rs/zerolog"
)

type authProxy struct {
	proxy    *httputil.ReverseProxy
	projects map[string]*auth.AuthRegistry
}

func runAuthProxy(ctx context.Context, logger zerolog.Logger, cfg *common.Config, upstreamURL string) error {
	target, err := url.Parse(upstreamURL)
	if err != nil {
		return fmt.Errorf("invalid upstream URL: %w", err)
	}
	if target.Scheme == "" || target.Host == "" {
		return fmt.Errorf("upstream URL must include scheme and host")
	}

	limits, err := upstream.NewRateLimitersRegistry(ctx, cfg.RateLimiters, &logger)
	if err != nil {
		return fmt.Errorf("failed to initialize rate limiters: %w", err)
	}

	projects := make(map[string]*auth.AuthRegistry, len(cfg.Projects))
	for _, project := range cfg.Projects {
		if project == nil || project.Auth == nil {
			continue
		}
		registry, err := auth.NewAuthRegistry(ctx, &logger, project.Id, project.Auth, limits)
		if err != nil {
			return fmt.Errorf("failed to initialize auth for project %s: %w", project.Id, err)
		}
		projects[project.Id] = registry
	}

	ap := &authProxy{
		proxy:    httputil.NewSingleHostReverseProxy(target),
		projects: projects,
	}

	addr := "0.0.0.0:4000"
	if cfg.Server != nil && cfg.Server.HttpHostV4 != nil && cfg.Server.HttpPortV4 != nil {
		addr = fmt.Sprintf("%s:%d", *cfg.Server.HttpHostV4, *cfg.Server.HttpPortV4)
	}

	readTimeout := 30 * time.Second
	writeTimeout := 120 * time.Second
	if cfg.Server != nil {
		if cfg.Server.ReadTimeout != nil {
			readTimeout = cfg.Server.ReadTimeout.Duration()
		}
		if cfg.Server.WriteTimeout != nil {
			writeTimeout = cfg.Server.WriteTimeout.Duration()
		}
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           ap,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       300 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", addr).Str("upstream", target.Redacted()).Msg("starting auth proxy")
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownTimeout := 10 * time.Second
		if cfg.Server != nil && cfg.Server.WaitAfterShutdown != nil {
			shutdownTimeout = cfg.Server.WaitAfterShutdown.Duration()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (p *authProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/healthcheck" || strings.HasSuffix(r.URL.Path, "/healthcheck")) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.Method == http.MethodOptions {
		p.proxy.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		writeProxyError(w, nil, common.NewErrInvalidRequest(fmt.Errorf("method not allowed")))
		return
	}

	projectID, err := projectFromPath(r.URL.Path)
	if err != nil {
		writeProxyError(w, nil, common.NewErrInvalidUrlPath(err.Error(), r.URL.Path))
		return
	}
	registry := p.projects[projectID]
	if registry == nil {
		writeProxyError(w, nil, common.NewErrProjectNotFound(projectID))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProxyError(w, nil, common.NewErrInvalidRequest(err))
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	methods, err := requestMethods(body)
	if err != nil {
		writeProxyError(w, nil, common.NewErrJsonRpcRequestUnmarshal(err, body))
		return
	}

	clientIP := clientIPFromRemoteAddr(r.RemoteAddr)
	for _, methodBody := range methods {
		req := common.NewNormalizedRequest(methodBody.raw)
		req.SetClientIP(clientIP)
		method, err := req.Method()
		if err != nil {
			writeProxyError(w, req, common.NewErrJsonRpcRequestUnmarshal(err, methodBody.raw))
			return
		}
		payload, err := auth.NewPayloadFromHttp(method, clientIP, r.Header, r.URL.Query())
		if err != nil {
			writeProxyError(w, req, common.NewErrInvalidRequest(err))
			return
		}
		if _, err := registry.Authenticate(r.Context(), req, method, payload); err != nil {
			writeProxyError(w, req, err)
			return
		}
	}

	p.proxy.ServeHTTP(w, r)
}

type rpcMethodBody struct {
	raw []byte
}

func requestMethods(body []byte) ([]rpcMethodBody, error) {
	var batch []json.RawMessage
	if err := json.Unmarshal(body, &batch); err == nil {
		if len(batch) == 0 {
			return nil, fmt.Errorf("empty JSON-RPC batch")
		}
		methods := make([]rpcMethodBody, 0, len(batch))
		for _, raw := range batch {
			methods = append(methods, rpcMethodBody{raw: raw})
		}
		return methods, nil
	}

	var single map[string]json.RawMessage
	if err := json.Unmarshal(body, &single); err != nil {
		return nil, fmt.Errorf("invalid JSON-RPC request")
	}
	return []rpcMethodBody{{raw: body}}, nil
}

func projectFromPath(rawPath string) (string, error) {
	clean := path.Clean(rawPath)
	if clean == "." || clean == "/" {
		return "", fmt.Errorf("project is required in path")
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	if parts[0] == "" {
		return "", fmt.Errorf("project is required in path")
	}
	return parts[0], nil
}

func clientIPFromRemoteAddr(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	if ip := net.ParseIP(remoteAddr); ip != nil {
		return ip.String()
	}
	if strings.HasPrefix(remoteAddr, "[") {
		if end := strings.Index(remoteAddr, "]"); end > 0 {
			if ip := net.ParseIP(remoteAddr[1:end]); ip != nil {
				return ip.String()
			}
		}
	}
	return remoteAddr
}

func writeProxyError(w http.ResponseWriter, req *common.NormalizedRequest, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(proxyErrorStatus(err))

	translated := common.TranslateToJsonRpcException(err)
	code := common.JsonRpcErrorServerSideException
	message := err.Error()
	jre := &common.ErrJsonRpcExceptionInternal{}
	if errors.As(translated, &jre) {
		code = jre.NormalizedCode()
		message = jre.Message
	}

	var id interface{}
	if req != nil {
		if jrr, _ := req.JsonRpcRequest(); jrr != nil {
			id = jrr.ID
		}
	}
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	})
	_, _ = w.Write(body)
}

func proxyErrorStatus(err error) int {
	switch {
	case common.HasErrorCode(err, common.ErrCodeInvalidUrlPath, common.ErrCodeJsonRpcRequestUnmarshal, common.ErrCodeInvalidRequest):
		return http.StatusBadRequest
	case common.HasErrorCode(err, common.ErrCodeAuthUnauthorized, common.ErrCodeEndpointUnauthorized):
		return http.StatusUnauthorized
	case common.HasErrorCode(err, common.ErrCodeProjectNotFound, common.ErrCodeNetworkNotFound, common.ErrCodeNetworkNotSupported):
		return http.StatusNotFound
	case common.HasErrorCode(err, common.ErrCodeEndpointRequestTooLarge):
		return http.StatusRequestEntityTooLarge
	case common.HasErrorCode(
		err,
		common.ErrCodeAuthRateLimitRuleExceeded,
		common.ErrCodeProjectRateLimitRuleExceeded,
		common.ErrCodeNetworkRateLimitRuleExceeded,
		common.ErrCodeEndpointCapacityExceeded,
	):
		return http.StatusTooManyRequests
	default:
		return http.StatusOK
	}
}
