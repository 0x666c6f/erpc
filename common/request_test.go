package common

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// mockUpstreamForSelection is a minimal mock for testing upstream selection logic
type mockUpstreamForSelection struct {
	id string
}

func (m *mockUpstreamForSelection) Id() string              { return m.id }
func (m *mockUpstreamForSelection) VendorName() string      { return "mock" }
func (m *mockUpstreamForSelection) NetworkId() string       { return "evm:1" }
func (m *mockUpstreamForSelection) NetworkLabel() string    { return "test" }
func (m *mockUpstreamForSelection) Config() *UpstreamConfig { return &UpstreamConfig{Id: m.id} }
func (m *mockUpstreamForSelection) Logger() *zerolog.Logger { return nil }
func (m *mockUpstreamForSelection) Vendor() Vendor          { return nil }
func (m *mockUpstreamForSelection) Tracker() HealthTracker  { return nil }
func (m *mockUpstreamForSelection) Forward(ctx context.Context, nq *NormalizedRequest, byPass, isHedgeAttempt bool) (*NormalizedResponse, error) {
	return nil, nil
}
func (m *mockUpstreamForSelection) Cordon(method string, reason string)   {}
func (m *mockUpstreamForSelection) Uncordon(method string, reason string) {}
func (m *mockUpstreamForSelection) IgnoreMethod(method string)            {}
func (m *mockUpstreamForSelection) ShouldHandleMethod(method string) (bool, error) {
	return true, nil
}

func newMockUpstream(id string) *mockUpstreamForSelection {
	return &mockUpstreamForSelection{id: id}
}

// TestUpstreamSelection_NonRetryableError_Skipped tests that non-retryable permanent
// errors (like method not supported) cause the upstream to be gated on subsequent
// selections. The ErrorsByUpstream gate blocks non-retryable, non-MissingData errors.
func TestUpstreamSelection_NonRetryableError_Skipped(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	req.SetUpstreams([]Upstream{up1})

	// First selection
	selected1, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("first NextUpstream should succeed: %v", err)
	}

	// Simulate a NON-retryable error (like method not supported)
	nonRetryableErr := NewErrUpstreamRequestSkipped(nil, "upstream1")
	req.MarkUpstreamCompleted(ctx, selected1, nil, nonRetryableErr)

	// Verify upstream is gated (ErrorsByUpstream gate blocks non-retryable,
	// non-MissingData errors).
	_, err = req.NextUpstream()
	if !HasErrorCode(err, ErrCodeNoUpstreamsLeftToSelect) {
		t.Fatalf("expected no upstreams left after non-retryable permanent error, got: %v", err)
	}
}

// TestUpstreamSelection_RetryableError_ClearedInSameCall tests that retryable errors
// are cleared and upstream is returned in the SAME call (no wasted attempts).
// This implements "try others first, then come back to retry" within a single NextUpstream call.
func TestUpstreamSelection_RetryableError_ClearedInSameCall(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	req.SetUpstreams([]Upstream{up1})

	// First selection
	selected1, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("first NextUpstream should succeed: %v", err)
	}
	if selected1.Id() != "upstream1" {
		t.Fatalf("expected upstream1, got %s", selected1.Id())
	}

	// Simulate a retryable error - upstream stays consumed but error is stored
	retryableErr := NewErrUpstreamBlockUnavailable("upstream1", 1000, 500, 400)
	req.MarkUpstreamCompleted(ctx, up1, nil, retryableErr)

	// Second call: upstream1 is consumed with retryable error.
	// NextUpstream should clear it at midpoint and return it in the SAME call.
	selected2, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("second NextUpstream should succeed (cleared and returned in same call): %v", err)
	}
	if selected2.Id() != "upstream1" {
		t.Fatalf("expected upstream1 to be re-selected after clearing, got %s", selected2.Id())
	}
}

// TestUpstreamSelection_ErrorsAccumulate tests that errors from multiple upstreams
// are accumulated in ErrorsByUpstream.
func TestUpstreamSelection_ErrorsAccumulate(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	up2 := newMockUpstream("upstream2")
	req.SetUpstreams([]Upstream{up1, up2})

	// Select and fail upstream1 with retryable error
	selected1, _ := req.NextUpstream()
	req.MarkUpstreamCompleted(ctx, selected1, nil, NewErrUpstreamBlockUnavailable("upstream1", 1000, 500, 400))

	// Select and fail upstream2 with non-retryable error
	selected2, _ := req.NextUpstream()
	req.MarkUpstreamCompleted(ctx, selected2, nil, NewErrUpstreamRequestSkipped(nil, "upstream2"))

	// Verify both errors are stored
	errorCount := 0
	req.ErrorsByUpstream.Range(func(key, value interface{}) bool {
		errorCount++
		return true
	})
	if errorCount != 2 {
		t.Fatalf("expected 2 errors in ErrorsByUpstream, got %d", errorCount)
	}
}

// TestUpstreamSelection_ExhaustionShouldReturnUpstreamNotError tests that when
// NextUpstream exhausts and clears retryable errors, it should immediately return
// an available upstream instead of returning an error. This prevents "wasted"
// attempts where one call is sacrificed just to trigger the clearing.
//
// Current behavior (FLAWED): exhaustion returns error, next call gets the upstream
// Desired behavior: exhaustion clears and returns upstream in same call
func TestUpstreamSelection_ExhaustionShouldReturnUpstreamNotError(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	up2 := newMockUpstream("upstream2")
	req.SetUpstreams([]Upstream{up1, up2})

	// Select both upstreams and mark them with retryable errors
	selected1, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("first NextUpstream should succeed: %v", err)
	}
	req.MarkUpstreamCompleted(ctx, selected1, nil, NewErrUpstreamBlockUnavailable("upstream1", 1000, 500, 400))

	selected2, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("second NextUpstream should succeed: %v", err)
	}
	req.MarkUpstreamCompleted(ctx, selected2, nil, NewErrUpstreamBlockUnavailable("upstream2", 1000, 500, 400))

	// Third call: both upstreams are consumed with retryable errors.
	// DESIRED: NextUpstream should clear retryables AND return an upstream in the same call.
	// CURRENT (FLAWED): NextUpstream returns error, "wasting" this attempt.
	selected3, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("third NextUpstream should return an upstream after clearing retryables, but got error: %v", err)
	}
	if selected3 == nil {
		t.Fatalf("third NextUpstream should return a valid upstream")
	}
	t.Logf("third call returned upstream: %s", selected3.Id())
}

// TestUpstreamSelection_MultipleExhaustionsNoWastedAttempts tests that with multiple
// upstreams and multiple rounds of exhaustion, no attempts are wasted.
// Each call to NextUpstream should either return an upstream or a final error.
func TestUpstreamSelection_MultipleExhaustionsNoWastedAttempts(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	up2 := newMockUpstream("upstream2")
	up3 := newMockUpstream("upstream3")
	req.SetUpstreams([]Upstream{up1, up2, up3})

	// Simulate 6 consecutive calls (2 rounds of 3 upstreams)
	// Each upstream should be selectable twice without any "wasted" error-only calls
	selectedCount := 0
	for i := 0; i < 6; i++ {
		selected, err := req.NextUpstream()
		if err != nil {
			t.Fatalf("call %d: expected upstream but got error: %v", i+1, err)
		}
		selectedCount++
		t.Logf("call %d: selected %s", i+1, selected.Id())

		// Mark with retryable error
		req.MarkUpstreamCompleted(ctx, selected, nil, NewErrUpstreamBlockUnavailable(selected.Id(), 1000, 500, 400))
	}

	if selectedCount != 6 {
		t.Fatalf("expected 6 successful selections, got %d", selectedCount)
	}
}

// TestUpstreamSelection_EmptyResponses_DontBlockReselection tests that upstreams
// which returned empty results can be re-selected on a subsequent retry round.
// BUG (before fix): EmptyResponses gate in NextUpstream permanently blocks
// upstreams that returned empty, preventing useful retries.
func TestUpstreamSelection_EmptyResponses_DontBlockReselection(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("rpc1")
	up2 := newMockUpstream("rpc2")
	req.SetUpstreams([]Upstream{up1, up2})

	// Round 1: select both upstreams and mark them as returning empty
	selected1, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("first NextUpstream should succeed: %v", err)
	}
	emptyResp1 := createEmptyNormalizedResponse(t)
	req.MarkUpstreamCompleted(ctx, selected1, emptyResp1, nil)

	selected2, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("second NextUpstream should succeed: %v", err)
	}
	emptyResp2 := createEmptyNormalizedResponse(t)
	req.MarkUpstreamCompleted(ctx, selected2, emptyResp2, nil)

	// Simulate "next retry round": both upstreams should be re-selectable.
	// BUG (before fix): EmptyResponses gate permanently blocks both upstreams
	// → NextUpstream returns ErrNoUpstreamsLeftToSelect.
	reselected, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("upstreams that returned empty should be re-selectable for retry, but got: %v", err)
	}
	if reselected == nil {
		t.Fatalf("expected a valid upstream to be returned")
	}
	t.Logf("re-selected upstream: %s", reselected.Id())
}

// TestUpstreamSelection_MissingDataError_DontBlockReselection tests that upstreams
// which returned MissingData errors can be re-selected on a subsequent retry round.
// BUG (before fix): ErrorsByUpstream gate treats ErrEndpointMissingData as
// non-retryable and permanently blocks the upstream.
func TestUpstreamSelection_MissingDataError_DontBlockReselection(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("rpc1")
	up2 := newMockUpstream("rpc2")
	req.SetUpstreams([]Upstream{up1, up2})

	// Round 1: select both upstreams and mark them with MissingData errors
	selected1, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("first NextUpstream should succeed: %v", err)
	}
	missingErr1 := NewErrEndpointMissingData(fmt.Errorf("missing trie node"), up1)
	req.MarkUpstreamCompleted(ctx, selected1, nil, missingErr1)

	selected2, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("second NextUpstream should succeed: %v", err)
	}
	missingErr2 := NewErrEndpointMissingData(fmt.Errorf("missing trie node"), up2)
	req.MarkUpstreamCompleted(ctx, selected2, nil, missingErr2)

	// Simulate "next retry round": both upstreams should be re-selectable.
	// BUG (before fix): ErrorsByUpstream gate sees ErrEndpointMissingData as
	// non-retryable toward upstream → permanently blocks both.
	reselected, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("upstreams with MissingData errors should be re-selectable for retry, but got: %v", err)
	}
	if reselected == nil {
		t.Fatalf("expected a valid upstream to be returned")
	}
	t.Logf("re-selected upstream: %s", reselected.Id())
}

func createEmptyNormalizedResponse(t *testing.T) *NormalizedResponse {
	t.Helper()
	jrr, err := NewJsonRpcResponse(1, nil, nil)
	if err != nil {
		t.Fatalf("failed to create empty JSON-RPC response: %v", err)
	}
	return NewNormalizedResponse().WithJsonRpcResponse(jrr)
}

func TestEnrichFromHttpHandlesBloomValidationHeaders(t *testing.T) {
	req := NewNormalizedRequest(nil)
	headers := http.Header{}
	headers.Set(headerDirectiveValidateLogsBloomEmpty, "true")

	req.EnrichFromHttp(headers, nil, UserAgentTrackingModeSimplified)

	dir := req.Directives()
	if dir == nil {
		t.Fatalf("expected directives to be initialized when headers are provided")
	}
	if !dir.ValidateLogsBloomEmptiness {
		t.Fatalf("expected ValidateLogsBloomEmptiness to be true")
	}
}

func TestEnrichFromHttpHandlesBloomValidationQueryParams(t *testing.T) {
	req := NewNormalizedRequest(nil)
	query := url.Values{}
	query.Set(queryDirectiveValidateLogsBloomMatch, "true")

	req.EnrichFromHttp(nil, query, UserAgentTrackingModeSimplified)

	dir := req.Directives()
	if dir == nil {
		t.Fatalf("expected directives to be initialized when query params are provided")
	}
	if !dir.ValidateLogsBloomMatch {
		t.Fatalf("expected ValidateLogsBloomMatch to be true")
	}
}

func TestEnrichFromHttp_CacheMaxAgeDirective(t *testing.T) {
	t.Run("HeaderValue", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		headers := http.Header{}
		headers.Set("X-ERPC-Cache-Max-Age", "15")

		req.EnrichFromHttp(headers, nil, UserAgentTrackingModeSimplified)

		dirs := req.Directives()
		if dirs == nil || dirs.CacheMaxAgeSeconds == nil {
			t.Fatalf("expected CacheMaxAgeSeconds from header")
		}
		if *dirs.CacheMaxAgeSeconds != 15 {
			t.Fatalf("expected CacheMaxAgeSeconds=15, got %d", *dirs.CacheMaxAgeSeconds)
		}
	})

	t.Run("QueryOverridesHeader", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		headers := http.Header{}
		headers.Set("X-ERPC-Cache-Max-Age", "15")
		query := url.Values{}
		query.Set("cache-max-age", "7")

		req.EnrichFromHttp(headers, query, UserAgentTrackingModeSimplified)

		dirs := req.Directives()
		if dirs == nil || dirs.CacheMaxAgeSeconds == nil {
			t.Fatalf("expected CacheMaxAgeSeconds from query")
		}
		if *dirs.CacheMaxAgeSeconds != 7 {
			t.Fatalf("expected CacheMaxAgeSeconds=7, got %d", *dirs.CacheMaxAgeSeconds)
		}
	})

	t.Run("InvalidValuesIgnored", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		headers := http.Header{}
		headers.Set("X-ERPC-Cache-Max-Age", "-1")
		query := url.Values{}
		query.Set("cache-max-age", "not-a-number")

		req.EnrichFromHttp(headers, query, UserAgentTrackingModeSimplified)

		dirs := req.Directives()
		if dirs != nil && dirs.CacheMaxAgeSeconds != nil {
			t.Fatalf("expected CacheMaxAgeSeconds to be nil for invalid values")
		}
	})
}

func TestEnrichFromHttp_CheckAllUpstreamsDirective(t *testing.T) {
	t.Run("header_sets_directive", func(t *testing.T) {
		req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))
		headers := http.Header{}
		headers.Set("X-ERPC-Check-All-Upstreams", "true")

		req.EnrichFromHttp(headers, nil, UserAgentTrackingModeSimplified)

		if dir := req.Directives(); dir == nil || !dir.CheckAllUpstreams {
			t.Fatalf("expected CheckAllUpstreams=true after header directive")
		}
	})

	t.Run("query_sets_directive", func(t *testing.T) {
		req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))
		query := url.Values{}
		query.Set("check-all-upstreams", "true")

		req.EnrichFromHttp(nil, query, UserAgentTrackingModeSimplified)

		if dir := req.Directives(); dir == nil || !dir.CheckAllUpstreams {
			t.Fatalf("expected CheckAllUpstreams=true after query directive")
		}
	})

	t.Run("parsing_edge_cases", func(t *testing.T) {
		cases := []struct {
			value   string
			enabled bool
		}{
			{"true", true},
			{"TRUE", true},
			{" true ", true},
			{"false", false},
			{"FALSE", false},
			{"1", false}, // only literal "true" (case-insensitive) enables
			{"yes", false},
			{"", false}, // empty value should leave directive unchanged
		}
		for _, tc := range cases {
			t.Run("header="+tc.value, func(t *testing.T) {
				req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))
				headers := http.Header{}
				if tc.value != "" {
					headers.Set("X-ERPC-Check-All-Upstreams", tc.value)
				}
				req.EnrichFromHttp(headers, nil, UserAgentTrackingModeSimplified)
				if got := req.Directives() != nil && req.Directives().CheckAllUpstreams; got != tc.enabled {
					t.Fatalf("value=%q: expected CheckAllUpstreams=%v, got %v", tc.value, tc.enabled, got)
				}
			})
		}
	})
}

func TestShouldCheckAllUpstreams_NilSafety(t *testing.T) {
	var nilReq *NormalizedRequest
	if nilReq.ShouldCheckAllUpstreams() {
		t.Fatalf("expected false from nil receiver")
	}

	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))
	if req.ShouldCheckAllUpstreams() {
		t.Fatalf("expected false from request with no directives set")
	}
}

// TestHeaderOverridesConfigDefault_ValidateTransactionsRoot verifies that when the
// config defaults set ValidateTransactionsRoot=true, a header/query-string can
// override it to false.
func TestHeaderOverridesConfigDefault_ValidateTransactionsRoot(t *testing.T) {
	trueVal := true
	cfgDefaults := &DirectiveDefaultsConfig{
		ValidateTransactionsRoot: &trueVal,
	}

	t.Run("header_overrides_config_true_to_false", func(t *testing.T) {
		req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber"}`))
		req.ApplyDirectiveDefaults(cfgDefaults)

		if dir := req.Directives(); dir == nil || !dir.ValidateTransactionsRoot {
			t.Fatalf("expected ValidateTransactionsRoot=true after ApplyDirectiveDefaults")
		}

		headers := http.Header{}
		headers.Set("X-ERPC-Validate-Transactions-Root", "false")
		req.EnrichFromHttp(headers, nil, UserAgentTrackingModeSimplified)

		if dir := req.Directives(); dir == nil || dir.ValidateTransactionsRoot {
			t.Fatalf("expected ValidateTransactionsRoot=false after header override, but got true")
		}
	})

	t.Run("query_string_overrides_config_true_to_false", func(t *testing.T) {
		req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber"}`))
		req.ApplyDirectiveDefaults(cfgDefaults)

		query := url.Values{}
		query.Set("validate-transactions-root", "false")
		req.EnrichFromHttp(nil, query, UserAgentTrackingModeSimplified)

		if dir := req.Directives(); dir == nil || dir.ValidateTransactionsRoot {
			t.Fatalf("expected ValidateTransactionsRoot=false after query string override, but got true")
		}
	})

	t.Run("header_and_query_both_false_override_config_true", func(t *testing.T) {
		req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber"}`))
		req.ApplyDirectiveDefaults(cfgDefaults)

		headers := http.Header{}
		headers.Set("X-ERPC-Validate-Transactions-Root", "false")
		headers.Set("X-ERPC-Skip-Cache-Read", "true")

		query := url.Values{}
		query.Set("validate-transactions-root", "false")

		req.EnrichFromHttp(headers, query, UserAgentTrackingModeSimplified)

		dir := req.Directives()
		if dir == nil || dir.ValidateTransactionsRoot {
			t.Fatalf("expected ValidateTransactionsRoot=false after header+query override, but got true")
		}
		if dir.SkipCacheRead != "true" {
			t.Fatalf("expected SkipCacheRead='true' from header, got '%s'", dir.SkipCacheRead)
		}
	})
}

func TestRequestDirectivesClone_PreservesGroundTruthLogsSemantics(t *testing.T) {
	t.Run("PreservesExplicitEmptyGroundTruthLogs", func(t *testing.T) {
		dirs := (&RequestDirectives{
			GroundTruthLogs:         []*GroundTruthLog{},
			GroundTruthLogsComplete: true,
		}).Clone()

		if dirs.GroundTruthLogs == nil {
			t.Fatalf("expected explicit empty GroundTruthLogs slice to be preserved")
		}
		if len(dirs.GroundTruthLogs) != 0 {
			t.Fatalf("expected empty GroundTruthLogs slice, got %d items", len(dirs.GroundTruthLogs))
		}
		if !dirs.GroundTruthLogsComplete {
			t.Fatalf("expected GroundTruthLogsComplete to be preserved")
		}
	})
}

func TestNormalizedRequestForwardBody_RawFastPathWhenUnmodified(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	req := NewNormalizedRequest(raw)

	jrq, err := req.JsonRpcRequest()
	if err != nil {
		t.Fatalf("expected JsonRpcRequest parse to succeed: %v", err)
	}
	if jrq == nil {
		t.Fatalf("expected JsonRpcRequest to be non-nil")
	}
	if jrq.IsModified() {
		t.Fatalf("expected parsed request to be unmodified")
	}

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}
	if string(forwardBody) != string(raw) {
		t.Fatalf("expected forward body to reuse raw bytes, got %s", string(forwardBody))
	}
	if req.Body() == nil {
		t.Fatalf("expected raw body to remain available when request is unmodified")
	}
}

func TestNormalizedRequestForwardBody_InvalidatesRawAfterNormalization(t *testing.T) {
	raw := []byte(`{"method":"eth_blockNumber","params":[]}`)
	req := NewNormalizedRequest(raw)

	jrq, err := req.JsonRpcRequest()
	if err != nil {
		t.Fatalf("expected JsonRpcRequest parse to succeed: %v", err)
	}
	if jrq == nil {
		t.Fatalf("expected JsonRpcRequest to be non-nil")
	}
	if !jrq.WasNormalized() {
		t.Fatalf("expected parsed request to be marked normalized")
	}
	if req.Body() != nil {
		t.Fatalf("expected raw body to be hidden after normalization")
	}

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}
	if string(forwardBody) == string(raw) {
		t.Fatalf("expected ForwardBody to re-marshal normalized request")
	}

	var payload map[string]interface{}
	if err := SonicCfg.Unmarshal(forwardBody, &payload); err != nil {
		t.Fatalf("expected marshaled body to be valid json: %v", err)
	}
	if payload["jsonrpc"] != "2.0" {
		t.Fatalf("expected marshaled body to include jsonrpc=2.0, got: %v", payload["jsonrpc"])
	}
	if payload["id"] == nil {
		t.Fatalf("expected marshaled body to include generated id")
	}
}

func TestNormalizedRequestForwardBody_InvalidatesRawAfterMutation(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x0000000000000000000000000000000000000001"}]}`)
	req := NewNormalizedRequest(raw)

	jrq, err := req.JsonRpcRequest()
	if err != nil {
		t.Fatalf("expected JsonRpcRequest parse to succeed: %v", err)
	}
	if jrq == nil {
		t.Fatalf("expected JsonRpcRequest to be non-nil")
	}

	if err := jrq.AppendParam("latest"); err != nil {
		t.Fatalf("expected AppendParam to succeed: %v", err)
	}
	if !jrq.IsModified() {
		t.Fatalf("expected request to be marked modified after param mutation")
	}
	if req.Body() != nil {
		t.Fatalf("expected raw body to be hidden after mutation")
	}

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}
	if string(forwardBody) == string(raw) {
		t.Fatalf("expected ForwardBody to re-marshal after mutation")
	}
	if !strings.Contains(string(forwardBody), `"latest"`) {
		t.Fatalf("expected marshaled body to contain updated params, got %s", string(forwardBody))
	}
}

func TestNormalizedRequestForwardBody_MarshalsWhenNoRawBody(t *testing.T) {
	jrq := NewJsonRpcRequest("eth_blockNumber", []interface{}{})
	if err := jrq.SetID(1); err != nil {
		t.Fatalf("expected SetID to succeed: %v", err)
	}
	req := NewNormalizedRequestFromJsonRpcRequest(jrq)

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}

	expected := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	if string(forwardBody) != expected {
		t.Fatalf("unexpected marshaled body, expected %s got %s", expected, string(forwardBody))
	}
}

func TestNormalizedRequestConstructorsInitializeForwardHeaders(t *testing.T) {
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`))
	req.ForwardHeaders.Add("x-test", "from-raw")
	if got := req.ForwardHeaders.Get("x-test"); got != "from-raw" {
		t.Fatalf("expected raw-body request to accept forwarded headers, got %q", got)
	}

	jrq := NewJsonRpcRequest("eth_blockNumber", []interface{}{})
	fromJSONRPC := NewNormalizedRequestFromJsonRpcRequest(jrq)
	fromJSONRPC.ForwardHeaders.Add("x-test", "from-jsonrpc")
	if got := fromJSONRPC.ForwardHeaders.Get("x-test"); got != "from-jsonrpc" {
		t.Fatalf("expected json-rpc request to accept forwarded headers, got %q", got)
	}
}

func TestNormalizedRequestForwardBody_StripsEthGetLogsMaxSizeFromUpstreamPayload(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2","maxSize":2}]}`)
	req := NewNormalizedRequest(raw)

	hashWithLimit, err := req.CacheHash()
	if err != nil {
		t.Fatalf("expected CacheHash to succeed: %v", err)
	}

	withoutLimit := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2"}]}`))
	hashWithoutLimit, err := withoutLimit.CacheHash()
	if err != nil {
		t.Fatalf("expected CacheHash without limit to succeed: %v", err)
	}
	if hashWithLimit == hashWithoutLimit {
		t.Fatalf("expected maxSize to affect cache hash")
	}

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}
	if strings.Contains(string(forwardBody), `"maxSize"`) {
		t.Fatalf("expected maxSize to be stripped from upstream payload, got %s", string(forwardBody))
	}
}

func TestNormalizedRequestForwardBody_EthGetLogsRawFastPathWithoutMaxSize(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2"}]}`)
	req := NewNormalizedRequest(raw)

	forwardBody, err := req.ForwardBody()
	if err != nil {
		t.Fatalf("expected ForwardBody to succeed: %v", err)
	}
	if string(forwardBody) != string(raw) {
		t.Fatalf("expected raw fast path for eth_getLogs without maxSize, got %s", string(forwardBody))
	}
}

func TestMarkUpstreamCompleted_SingleUpstreamBlockUnavailable_DisablesNetworkRetry(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	req.SetUpstreams([]Upstream{up1})

	selected, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("expected upstream selection to succeed: %v", err)
	}

	req.MarkUpstreamCompleted(
		ctx,
		selected,
		nil,
		NewErrUpstreamBlockUnavailable(selected.Id(), 1000, 995, 900),
	)

	stored, ok := req.ErrorsByUpstream.Load(selected)
	if !ok {
		t.Fatalf("expected stored error for upstream")
	}
	storedErr, ok := stored.(error)
	if !ok {
		t.Fatalf("expected stored value to be an error")
	}
	if !HasErrorCode(storedErr, ErrCodeUpstreamBlockUnavailable) {
		t.Fatalf("expected ErrCodeUpstreamBlockUnavailable, got: %v", storedErr)
	}
	if IsRetryableTowardNetwork(storedErr) {
		t.Fatalf("single-upstream block-unavailable should not be retryable toward network")
	}
	if !IsRetryableTowardsUpstream(storedErr) {
		t.Fatalf("block-unavailable should remain retryable toward upstream")
	}

	exhaustedErr := NewErrUpstreamsExhausted(
		req,
		&req.ErrorsByUpstream,
		"project",
		"evm:1",
		"eth_call",
		10*time.Millisecond,
		1,
		0,
		0,
		1,
	)
	if IsRetryableTowardNetwork(exhaustedErr) {
		t.Fatalf("single-upstream exhausted error should not be retryable toward network")
	}
}

func TestMarkUpstreamCompleted_MultiUpstreamBlockUnavailable_RemainsNetworkRetryable(t *testing.T) {
	ctx := context.Background()
	req := NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call"}`))

	up1 := newMockUpstream("upstream1")
	up2 := newMockUpstream("upstream2")
	req.SetUpstreams([]Upstream{up1, up2})

	selected, err := req.NextUpstream()
	if err != nil {
		t.Fatalf("expected upstream selection to succeed: %v", err)
	}

	req.MarkUpstreamCompleted(
		ctx,
		selected,
		nil,
		NewErrUpstreamBlockUnavailable(selected.Id(), 1000, 995, 900),
	)

	stored, ok := req.ErrorsByUpstream.Load(selected)
	if !ok {
		t.Fatalf("expected stored error for upstream")
	}
	storedErr, ok := stored.(error)
	if !ok {
		t.Fatalf("expected stored value to be an error")
	}
	if !IsRetryableTowardNetwork(storedErr) {
		t.Fatalf("multi-upstream block-unavailable should remain retryable toward network")
	}
}

func TestDirectiveAllowFilter(t *testing.T) {
	ptr := func(s string) *string { return &s }
	tests := []struct {
		name    string
		pattern *string
		key     string
		want    bool
	}{
		{"nil allows all", nil, "skip-cache-read", true},
		{"nil allows any key", nil, "use-upstream", true},
		{"empty string blocks all", ptr(""), "skip-cache-read", false},
		{"empty string blocks any key", ptr(""), "retry-empty", false},
		{"wildcard allows all", ptr("*"), "skip-cache-read", true},
		{"wildcard allows any key", ptr("*"), "use-upstream", true},
		{"exact match allows", ptr("skip-cache-read"), "skip-cache-read", true},
		{"exact match denies other", ptr("skip-cache-read"), "use-upstream", false},
		{"OR allows first", ptr("skip-cache-read | use-upstream"), "skip-cache-read", true},
		{"OR allows second", ptr("skip-cache-read | use-upstream"), "use-upstream", true},
		{"OR denies other", ptr("skip-cache-read | use-upstream"), "retry-empty", false},
		{"negation denies target", ptr("!skip-cache-read"), "skip-cache-read", false},
		{"negation allows other", ptr("!skip-cache-read"), "use-upstream", true},
		{"AND negation denies first", ptr("!skip-cache-read & !use-upstream"), "skip-cache-read", false},
		{"AND negation denies second", ptr("!skip-cache-read & !use-upstream"), "use-upstream", false},
		{"AND negation allows other", ptr("!skip-cache-read & !use-upstream"), "retry-empty", true},
		{"glob allows matching", ptr("retry-*"), "retry-empty", true},
		{"glob allows matching 2", ptr("retry-*"), "retry-pending", true},
		{"glob denies non-matching", ptr("retry-*"), "skip-cache-read", false},
		{"glob OR allows glob match", ptr("retry-* | skip-consensus"), "retry-empty", true},
		{"glob OR allows exact match", ptr("retry-* | skip-consensus"), "skip-consensus", true},
		{"glob OR denies other", ptr("retry-* | skip-consensus"), "use-upstream", false},
		{"grouped negation denies first", ptr("!(skip-cache-read | use-upstream)"), "skip-cache-read", false},
		{"grouped negation denies second", ptr("!(skip-cache-read | use-upstream)"), "use-upstream", false},
		{"grouped negation allows other", ptr("!(skip-cache-read | use-upstream)"), "retry-empty", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := NewNormalizedRequest(nil)
			if tc.pattern != nil {
				if *tc.pattern == "" {
					req.SetAllowClientDirectiveMatcher(DenyAllClientDirectives)
				} else {
					matcher, err := NewWildcardMatcher(*tc.pattern)
					if err != nil {
						t.Fatalf("failed to compile pattern %q: %v", *tc.pattern, err)
					}
					req.SetAllowClientDirectiveMatcher(matcher)
				}
			}
			got := req.isDirectiveAllowed(tc.key)
			if got != tc.want {
				patternStr := "<nil>"
				if tc.pattern != nil {
					patternStr = *tc.pattern
				}
				t.Fatalf("isDirectiveAllowed(%q, %q) = %v, want %v", tc.key, patternStr, got, tc.want)
			}
		})
	}
}

func setDirectiveFilter(t *testing.T, req *NormalizedRequest, pattern string) {
	t.Helper()
	if pattern == "" {
		req.SetAllowClientDirectiveMatcher(DenyAllClientDirectives)
		return
	}
	matcher, err := NewWildcardMatcher(pattern)
	if err != nil {
		t.Fatalf("failed to compile pattern %q: %v", pattern, err)
	}
	req.SetAllowClientDirectiveMatcher(matcher)
}

func TestEnrichFromHttp_AllowClientDirectives(t *testing.T) {
	t.Run("nil allows all directives", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		h := http.Header{}
		h.Set("X-ERPC-Skip-Cache-Read", "true")
		h.Set("X-ERPC-Use-Upstream", "alchemy")
		req.EnrichFromHttp(h, nil, UserAgentTrackingModeSimplified)
		dir := req.Directives()
		if dir.SkipCacheRead != "true" {
			t.Fatalf("expected SkipCacheRead=true, got %q", dir.SkipCacheRead)
		}
		if dir.UseUpstream != "alchemy" {
			t.Fatalf("expected UseUpstream=alchemy, got %q", dir.UseUpstream)
		}
	})

	t.Run("empty string blocks all directives", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		setDirectiveFilter(t, req, "")
		h := http.Header{}
		h.Set("X-ERPC-Skip-Cache-Read", "true")
		h.Set("X-ERPC-Use-Upstream", "alchemy")
		h.Set("X-ERPC-Skip-Consensus", "true")
		req.EnrichFromHttp(h, nil, UserAgentTrackingModeSimplified)
		dir := req.Directives()
		if dir == nil {
			t.Fatal("expected directives struct to exist")
		}
		if dir.SkipCacheRead != "" {
			t.Fatalf("expected SkipCacheRead blocked, got %q", dir.SkipCacheRead)
		}
		if dir.UseUpstream != "" {
			t.Fatalf("expected UseUpstream blocked, got %q", dir.UseUpstream)
		}
		if dir.SkipConsensus {
			t.Fatal("expected SkipConsensus=false")
		}
	})

	t.Run("negation blocks specific directive", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		setDirectiveFilter(t, req, "!skip-cache-read")
		h := http.Header{}
		h.Set("X-ERPC-Skip-Cache-Read", "true")
		h.Set("X-ERPC-Use-Upstream", "alchemy")
		req.EnrichFromHttp(h, nil, UserAgentTrackingModeSimplified)
		dir := req.Directives()
		if dir.SkipCacheRead != "" {
			t.Fatalf("expected SkipCacheRead blocked, got %q", dir.SkipCacheRead)
		}
		if dir.UseUpstream != "alchemy" {
			t.Fatalf("expected UseUpstream=alchemy (allowed), got %q", dir.UseUpstream)
		}
	})

	t.Run("blocks query params too", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		setDirectiveFilter(t, req, "!skip-cache-read")
		q := url.Values{}
		q.Set("skip-cache-read", "true")
		q.Set("use-upstream", "alchemy")
		req.EnrichFromHttp(nil, q, UserAgentTrackingModeSimplified)
		dir := req.Directives()
		if dir.SkipCacheRead != "" {
			t.Fatalf("expected SkipCacheRead blocked via query, got %q", dir.SkipCacheRead)
		}
		if dir.UseUpstream != "alchemy" {
			t.Fatalf("expected UseUpstream=alchemy via query (allowed), got %q", dir.UseUpstream)
		}
	})

	t.Run("user agent always extracted regardless of filter", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		setDirectiveFilter(t, req, "")
		h := http.Header{}
		h.Set("User-Agent", "curl/7.68.0")
		h.Set("X-ERPC-Skip-Cache-Read", "true")
		req.EnrichFromHttp(h, nil, UserAgentTrackingModeSimplified)
		if req.AgentName() == "" || req.AgentName() == "unknown" {
			t.Fatalf("expected user-agent to be extracted even when all directives blocked, got %q", req.AgentName())
		}
		dir := req.Directives()
		if dir != nil && dir.SkipCacheRead != "" {
			t.Fatalf("expected SkipCacheRead blocked, got %q", dir.SkipCacheRead)
		}
	})

	t.Run("directive defaults still apply when client directives blocked", func(t *testing.T) {
		req := NewNormalizedRequest(nil)
		retryEmpty := true
		req.ApplyDirectiveDefaults(&DirectiveDefaultsConfig{
			RetryEmpty: &retryEmpty,
		})
		setDirectiveFilter(t, req, "")
		h := http.Header{}
		h.Set("X-ERPC-Skip-Cache-Read", "true")
		req.EnrichFromHttp(h, nil, UserAgentTrackingModeSimplified)
		dir := req.Directives()
		if dir == nil {
			t.Fatal("expected directives from defaults to survive, got nil")
		}
		if !dir.RetryEmpty {
			t.Fatal("expected RetryEmpty=true from defaults even when client directives blocked")
		}
		if dir.SkipCacheRead != "" {
			t.Fatalf("expected SkipCacheRead blocked, got %q", dir.SkipCacheRead)
		}
	})
}

func newReqForUser() *NormalizedRequest {
	return NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}`))
}

func TestSetUserFromTrustedHeader(t *testing.T) {
	t.Run("sets the user id when none was resolved", func(t *testing.T) {
		req := newReqForUser()
		req.SetUserFromTrustedHeader("proj_edge_ep1")
		if u := req.User(); u == nil || u.Id != "proj_edge_ep1" {
			t.Fatalf("expected user id %q, got %+v", "proj_edge_ep1", u)
		}
	})

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		req := newReqForUser()
		req.SetUserFromTrustedHeader("  proj_edge_ep1\n")
		if u := req.User(); u == nil || u.Id != "proj_edge_ep1" {
			t.Fatalf("expected trimmed user id, got %+v", u)
		}
	})

	t.Run("is a no-op for an empty or whitespace-only value", func(t *testing.T) {
		for _, v := range []string{"", "   ", "\t\n"} {
			req := newReqForUser()
			req.SetUserFromTrustedHeader(v)
			if u := req.User(); u != nil {
				t.Fatalf("expected no user for value %q, got %+v", v, u)
			}
		}
	})

	t.Run("never derives a rate-limit budget", func(t *testing.T) {
		req := newReqForUser()
		req.SetUserFromTrustedHeader("proj_edge_ep1")
		if u := req.User(); u == nil || u.RateLimitBudget != "" {
			t.Fatalf("trusted-header user must carry no budget, got %+v", u)
		}
	})

	t.Run("auth wins — does not overwrite an already-resolved user", func(t *testing.T) {
		req := newReqForUser()
		req.SetUser(&User{Id: "auth-resolved", RateLimitBudget: "tier-1"})
		req.SetUserFromTrustedHeader("header-user")
		if u := req.User(); u == nil || u.Id != "auth-resolved" || u.RateLimitBudget != "tier-1" {
			t.Fatalf("auth-resolved user must be preserved, got %+v", u)
		}
	})
}
