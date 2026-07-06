package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	server := &http.Server{Addr: addr, Handler: ap}
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
		writeProxyError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	projectID, err := projectFromPath(r.URL.Path)
	if err != nil {
		writeProxyError(w, http.StatusOK, err.Error())
		return
	}
	registry := p.projects[projectID]
	if registry == nil {
		writeProxyError(w, http.StatusOK, "project auth is not configured")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProxyError(w, http.StatusOK, "failed to read request body")
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	methods, err := requestMethods(body)
	if err != nil {
		writeProxyError(w, http.StatusOK, err.Error())
		return
	}

	for _, methodBody := range methods {
		req := common.NewNormalizedRequest(methodBody.raw)
		req.SetClientIP(r.RemoteAddr)
		method, err := req.Method()
		if err != nil {
			writeProxyError(w, http.StatusOK, err.Error())
			return
		}
		payload, err := auth.NewPayloadFromHttp(method, r.RemoteAddr, r.Header, r.URL.Query())
		if err != nil {
			writeProxyError(w, http.StatusOK, err.Error())
			return
		}
		if _, err := registry.Authenticate(r.Context(), req, method, payload); err != nil {
			writeProxyError(w, http.StatusOK, err.Error())
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

func writeProxyError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(message)
	_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32016,"message":%s}}`, body)
}
