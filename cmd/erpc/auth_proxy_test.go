package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"github.com/erpc/erpc/auth"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/upstream"
	"github.com/rs/zerolog"
)

func TestAuthProxySecretAuthAndRateLimit(t *testing.T) {
	var forwarded int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded++
		if r.URL.Path != "/cache/evm/1" {
			t.Fatalf("unexpected forwarded path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer backend.Close()

	proxy := newTestAuthProxy(t, backend.URL, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{
		{
			Type:            common.AuthTypeSecret,
			Secret:          &common.SecretStrategyConfig{Id: "tester", Value: "good"},
			RateLimitBudget: "one",
		},
	}}, &common.RateLimiterConfig{
		Store: &common.RateLimitStoreConfig{Driver: "memory"},
		Budgets: []*common.RateLimitBudgetConfig{{
			Id: "one",
			Rules: []*common.RateLimitRuleConfig{{
				Method:   "*",
				MaxCount: 1,
				Period:   common.RateLimitPeriodMinute,
				PerUser:  true,
			}},
		}},
	})

	first := httptest.NewRecorder()
	proxy.ServeHTTP(first, rpcRequest("/cache/evm/1?secret=good"))
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"result":"0x1"`) {
		t.Fatalf("expected valid request to forward, code=%d body=%s", first.Code, first.Body.String())
	}

	bad := httptest.NewRecorder()
	proxy.ServeHTTP(bad, rpcRequest("/cache/evm/1?secret=bad"))
	if bad.Code != http.StatusUnauthorized || !strings.Contains(bad.Body.String(), "unauthorized") {
		t.Fatalf("expected bad secret to be rejected, code=%d body=%s", bad.Code, bad.Body.String())
	}

	limited := httptest.NewRecorder()
	proxy.ServeHTTP(limited, rpcRequest("/cache/evm/1?secret=good"))
	if limited.Code != http.StatusTooManyRequests || !strings.Contains(limited.Body.String(), "rate-limit exceeded") {
		t.Fatalf("expected second valid request to be rate-limited, code=%d body=%s", limited.Code, limited.Body.String())
	}
	if forwarded != 1 {
		t.Fatalf("expected only one forwarded request, got %d", forwarded)
	}
}

func TestAuthProxyNormalizesClientIPForNetworkAuth(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer backend.Close()

	proxy := newTestAuthProxy(t, backend.URL, &common.AuthConfig{Strategies: []*common.AuthStrategyConfig{
		{
			Type:    common.AuthTypeNetwork,
			Network: &common.NetworkStrategyConfig{AllowedIPs: []string{"192.0.2.1"}},
		},
	}}, nil)

	resp := httptest.NewRecorder()
	proxy.ServeHTTP(resp, rpcRequest("/cache/evm/1"))
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"result":"0x1"`) {
		t.Fatalf("expected network auth to use host without port, code=%d body=%s", resp.Code, resp.Body.String())
	}
}

func newTestAuthProxy(t *testing.T, upstreamURL string, authCfg *common.AuthConfig, rateLimiters *common.RateLimiterConfig) *authProxy {
	t.Helper()

	logger := zerolog.New(io.Discard)
	limits, err := upstream.NewRateLimitersRegistry(context.Background(), rateLimiters, &logger)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := auth.NewAuthRegistry(context.Background(), &logger, "cache", authCfg, limits)
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse(upstreamURL)
	if err != nil {
		t.Fatal(err)
	}
	return &authProxy{
		proxy: httputil.NewSingleHostReverseProxy(target),
		projects: map[string]*auth.AuthRegistry{
			"cache": registry,
		},
	}
}

func rpcRequest(path string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`))
	req.RemoteAddr = "192.0.2.1:12345"
	return req
}
