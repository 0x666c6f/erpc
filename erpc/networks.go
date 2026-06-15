package erpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erpc/erpc/architecture/evm"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/internal/policy"
	"github.com/erpc/erpc/telemetry"
	"github.com/erpc/erpc/upstream"
	"github.com/erpc/erpc/util"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var networkCacheHitLogSampler = &zerolog.BasicSampler{N: 100}

const (
	maxConcurrentNetworkCacheWrites       = 100
	networkPostCompletionCoalescingWindow = 40 * time.Millisecond
	networkDeterministicNegativeCacheTTL  = 200 * time.Millisecond
	networkFailsafeTimeoutSlack           = 30 * time.Millisecond
)

func logCacheHit(lg *zerolog.Logger, resp *common.NormalizedResponse) {
	if lg == nil || lg.GetLevel() > zerolog.DebugLevel {
		return
	}
	sampled := lg.Sample(networkCacheHitLogSampler)
	if lg.GetLevel() <= zerolog.TraceLevel {
		sampled.Debug().Object("response", resp).Msg("response served from cache")
		return
	}
	sampled.Debug().Msg("response served from cache")
}

func (n *Network) getCacheWriteSem() chan struct{} {
	n.cacheWriteSemInit.Do(func() {
		if n.cacheWriteSem == nil {
			n.cacheWriteSem = make(chan struct{}, maxConcurrentNetworkCacheWrites)
		}
	})
	return n.cacheWriteSem
}

func (n *Network) observeCacheWriteQueueDepth(sem chan struct{}) {
	if sem == nil {
		return
	}
	telemetry.MetricNetworkCacheWriteQueueDepth.WithLabelValues(
		n.projectId,
		n.Label(),
	).Set(float64(len(sem)))
}

func classifyAttemptReason(consensusEnabled bool, retries, hedges int) string {
	switch {
	case consensusEnabled:
		return telemetry.AttemptReasonConsensus
	case hedges > 0:
		return telemetry.AttemptReasonHedge
	case retries > 0:
		return telemetry.AttemptReasonRetry
	default:
		return ""
	}
}

type allUpstreamsCheckResult struct {
	AllSucceeded        bool                              `json:"allSucceeded"`
	Total               int                               `json:"total"`
	Succeeded           int                               `json:"succeeded"`
	ExecutionExceptions int                               `json:"executionExceptions"`
	Failed              int                               `json:"failed"`
	Upstreams           []allUpstreamsCheckUpstreamResult `json:"upstreams"`
}

type allUpstreamsCheckUpstreamResult struct {
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

type getSortedUpstreamsForNetworkFn func(
	ctx context.Context,
	registry *upstream.UpstreamsRegistry,
	networkID string,
	method string,
) ([]common.Upstream, error)

type Network struct {
	networkId             string
	networkLabel          string
	projectId             string
	logger                *zerolog.Logger
	bootstrapOnce         sync.Once
	appCtx                context.Context
	cfg                   *common.NetworkConfig
	inFlightRequests      *sync.Map
	failsafeExecutors     []*networkExecutor
	rateLimitersRegistry  *upstream.RateLimitersRegistry
	cacheDal              common.CacheDAL
	metricsTracker        *health.Tracker
	upstreamsRegistry     *upstream.UpstreamsRegistry
	policyEngine          *policy.Engine
	initializer           *util.Initializer
	getSortedUpstreamsFn  getSortedUpstreamsForNetworkFn
	cacheWriteSem         chan struct{}
	cacheWriteSemInit     sync.Once
	negativeResultCache   *sync.Map
	postCompletionResults *sync.Map

	// servedLatest / servedFinalized are STRICT-MONOTONIC at the network level:
	// once we serve a tip of N to clients, EvmHighestLatest/FinalizedBlockNumber
	// servedTipAnchor watchdogs track when this process last SAW the served
	// value change -- purely for the advance-age stuck-tip gauge. The pick
	// itself is stateless (evm.PickServedTip); nothing here feeds back into
	// what clients receive.
	servedLatestAnchor    servedTipAnchor
	servedFinalizedAnchor servedTipAnchor

	// servedTipPartitions holds lazily-materialized per-group lanes for
	// use-upstream selectors that carve out a real sub-group. Telemetry only.
	servedTipPartitions     sync.Map // map[string]*servedTipPartition (key = "grp:<hash>")
	servedTipPartitionCount atomic.Int32

	// servedTipBlockTimeOverride, when > 0, replaces the tracker's EMA block
	// time in buildServedTipConfig. Set only from package-internal tests.
	servedTipBlockTimeOverride float64
}

type deterministicNegativeCacheEntry struct {
	err       error
	expiresAt int64
}

type postCompletionResultEntry struct {
	mu        sync.RWMutex
	resp      *common.NormalizedResponse
	expiresAt int64
	released  bool
}

func (e *postCompletionResultEntry) acquireResponse() (*common.NormalizedResponse, bool) {
	if e == nil {
		return nil, false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.released || e.resp == nil {
		return nil, false
	}

	e.resp.AddRef()
	return e.resp, true
}

func (e *postCompletionResultEntry) release() {
	if e == nil {
		return
	}

	e.mu.Lock()
	if e.released {
		e.mu.Unlock()
		return
	}
	e.released = true
	resp := e.resp
	e.resp = nil
	e.mu.Unlock()

	if resp != nil {
		resp.Release()
	}
}

type skipNetworkRateLimitKey struct{}

func defaultGetSortedUpstreamsForNetwork(
	ctx context.Context,
	registry *upstream.UpstreamsRegistry,
	networkID string,
	method string,
) ([]common.Upstream, error) {
	return registry.GetSortedUpstreams(ctx, networkID, method)
}

func upstreamMatchesFailsafeGroup(u common.Upstream, group string) bool {
	if group == "" {
		return true
	}
	cfg := u.Config()
	if cfg == nil {
		return false
	}
	patterns := []string{group}
	if !strings.Contains(group, ":") {
		patterns = append(patterns, "tier:"+group)
	}
	for _, tag := range cfg.Tags {
		for _, pattern := range patterns {
			if tag == pattern {
				return true
			}
			if matched, _ := common.WildcardMatch(pattern, tag); matched {
				return true
			}
		}
	}
	return false
}

func withSkipNetworkRateLimit(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipNetworkRateLimitKey{}, true)
}

func shouldSkipNetworkRateLimit(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v := ctx.Value(skipNetworkRateLimitKey{}); v != nil {
		if skip, ok := v.(bool); ok && skip {
			return true
		}
	}
	return false
}

func cloneRequestForAllUpstreamsCheck(ctx context.Context, source *common.NormalizedRequest) (*common.NormalizedRequest, error) {
	body, err := source.ForwardBody(ctx)
	if err != nil {
		return nil, err
	}

	cloned := common.NewNormalizedRequest(bytes.Clone(body))
	if directives := source.Directives(); directives != nil {
		clonedDirs := directives.Clone()
		// Force per-probe to bypass cache reads so the diagnostic truly
		// contacts each upstream instead of returning a cache hit from
		// EVM hook paths (e.g. handleUserMulticall3 honors SkipCacheRead).
		clonedDirs.SkipCacheRead = "true"
		cloned.SetDirectives(clonedDirs)
	}
	if source.ForwardHeaders != nil {
		cloned.ForwardHeaders = source.ForwardHeaders.Clone()
	}
	cloned.CopyHttpContextFrom(source)
	cloned.SetSkipMultiplex(true)

	return cloned, nil
}

func (n *Network) forwardAllUpstreamsCheck(
	ctx context.Context,
	req *common.NormalizedRequest,
	upsList []common.Upstream,
	method string,
) (*common.NormalizedResponse, error) {
	result := allUpstreamsCheckResult{
		AllSucceeded: true,
		Total:        len(upsList),
		Upstreams:    make([]allUpstreamsCheckUpstreamResult, 0, len(upsList)),
	}

	for _, ups := range upsList {
		entry := n.probeUpstreamForAllUpstreamsCheck(ctx, req, ups, method)

		switch {
		case entry.ExecutionException:
			result.ExecutionExceptions++
			result.Succeeded++
		case entry.Succeeded:
			result.Succeeded++
		default:
			result.Failed++
			result.AllSucceeded = false
		}
		result.Upstreams = append(result.Upstreams, entry)
	}

	if result.Total == 0 {
		// AllSucceeded over an empty fleet is misleading; signal explicitly.
		result.AllSucceeded = false
	}

	jrr, err := common.NewJsonRpcResponse(req.ID(), result, nil)
	if err != nil {
		n.logger.Error().Err(err).Msg("failed to marshal check-all-upstreams diagnostic result")
		return nil, err
	}

	resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
	resp.SetAttempts(len(upsList))
	return resp, nil
}

// probeUpstreamForAllUpstreamsCheck runs a single upstream probe for the
// CheckAllUpstreams diagnostic mode and returns the populated entry.
func (n *Network) probeUpstreamForAllUpstreamsCheck(
	ctx context.Context,
	req *common.NormalizedRequest,
	ups common.Upstream,
	method string,
) allUpstreamsCheckUpstreamResult {
	started := time.Now()
	entry := allUpstreamsCheckUpstreamResult{
		ID:     ups.Id(),
		Vendor: ups.VendorName(),
	}

	probeReq, err := cloneRequestForAllUpstreamsCheck(ctx, req)
	if err == nil {
		probeReq.SetNetwork(n)
		probeReq.SetCacheDal(n.cacheDal)

		if skipErr, isRetryable := n.checkUpstreamBlockAvailability(ctx, ups, probeReq, method); skipErr != nil {
			err = skipErr
			entry.RetryableSkip = isRetryable
		} else {
			var probeResp *common.NormalizedResponse
			probeResp, err = n.doForward(ctx, ups, probeReq, true, false)
			if probeResp != nil {
				jrr, jrrErr := probeResp.JsonRpcResponse(ctx)
				switch {
				case jrrErr != nil:
					// Don't silently swallow parse/decompression failures —
					// surface them as the upstream's failure if no other error
					// already won.
					if err == nil {
						err = jrrErr
					}
					n.logger.Warn().
						Err(jrrErr).
						Str("upstreamId", ups.Id()).
						Msg("check-all-upstreams: failed to parse upstream JSON-RPC response")
				case jrr != nil:
					entry.ResultSize = len(jrr.GetResultBytes())
					if jrr.Error != nil && err == nil {
						// jrr.Error is a *ErrJsonRpcExceptionExternal whose top-level
						// type code is not ErrCodeEndpointExecutionException. Wrap
						// revert/call exceptions into ErrEndpointExecutionException so
						// downstream HasErrorCode(...) detects them as execution
						// exceptions — matching how the rest of the proxy classifies
						// these errors and what the directive's docs promise.
						switch common.JsonRpcErrorNumber(jrr.Error.Code) {
						case common.JsonRpcErrorEvmReverted,
							common.JsonRpcErrorCallException,
							common.JsonRpcErrorTransactionRejected:
							err = common.NewErrEndpointExecutionException(jrr.Error)
						default:
							err = jrr.Error
						}
					}
				}
				probeResp.Release()
			}
		}
	}

	entry.DurationMs = time.Since(started).Milliseconds()
	entry.ExecutionException = common.HasErrorCode(err, common.ErrCodeEndpointExecutionException)
	entry.Succeeded = err == nil || entry.ExecutionException
	if err != nil {
		entry.Error = err.Error()
		entry.ErrorSummary = common.ErrorSummary(err)
		entry.ErrorFingerprint = common.ErrorFingerprint(err)
		var se common.StandardError
		if errors.As(err, &se) {
			if base := se.Base(); base != nil {
				entry.ErrorCode = string(base.Code)
			}
		}
	}

	return entry
}

// maxServedTipPartitions caps the number of materialized per-tag served-tip
// partitions per network (a backstop on top of the "must be a configured tag"
// gate). Realistic configs have 1–3 groups; beyond the cap, extra tags fall
// back to the stateless subset candidate.
const maxServedTipPartitions = 16

// servedTipPartition is one node group's telemetry lane: the stable gauge
// label (common.LaneName of the matched upstream set) and the group's
// watchdog anchors. It holds NO pick state.
type servedTipPartition struct {
	lane            string
	latestAnchor    servedTipAnchor
	finalizedAnchor servedTipAnchor
}

// servedTipAnchor tracks, per process, when the served-tip VALUE was last
// seen to change — the advance-age watchdog's clock. Telemetry only: nothing
// feeds back into the pick.
//
// The first observed value does NOT count as a change (there is nothing to
// compare against): the anchor stays unset until the process witnesses the
// value actually move.
type servedTipAnchor struct {
	seenValue   atomic.Int64
	changedAtMs atomic.Int64
}

// observe records v as the latest served value, stamping the anchor when the
// value changed since the last observation.
func (a *servedTipAnchor) observe(v int64) {
	if old := a.seenValue.Swap(v); old != v && old != 0 {
		a.changedAtMs.Store(time.Now().UnixMilli())
	}
}

// age returns time since the last observed value change, or -1 when no change
// has been observed yet (the advance-age gauge is skipped on -1).
func (a *servedTipAnchor) age() time.Duration {
	if ms := a.changedAtMs.Load(); ms > 0 {
		return time.Since(time.UnixMilli(ms))
	}
	return -1
}

// servedTipLaneAll is the lane label for the network-wide served tip.
const servedTipLaneAll = "all"

// servedTipLaneNone is the sentinel passed to clusteredServedTip for a
// stateless selector-scoped tip (glob/single-node/match-all that did not form a
// group): observeServedTipMetrics emits nothing for it, so such a subset value
// never overwrites any gauge.
const servedTipLaneNone = "\x00scoped"

// Bootstrap registers this network with the policy engine. The engine kicks
// off the slot's ticker and runs an initial synchronous eval so request-path
// reads through `policyEngine.GetOrdered` always see a populated cache.
//
// The upstream list is supplied as a closure so newly-bootstrapped upstreams
// become visible to the engine each tick without a re-register.
func (n *Network) Bootstrap(ctx context.Context) error {
	if n.policyEngine == nil {
		return nil
	}
	cfg := n.cfg.SelectionPolicy
	if cfg == nil {
		cfg = &common.SelectionPolicyConfig{}
		n.cfg.SelectionPolicy = cfg
	}
	// Defensive: callers may have set Eval but skipped SetDefaults (common
	// in tests that build Config as Go struct literals). Compile here so
	// the engine never sees a nil program.
	if cfg.CompiledProgram == nil {
		if err := cfg.SetDefaults(); err != nil {
			return fmt.Errorf("selectionPolicy SetDefaults: %w", err)
		}
	}
	upstreamsFn := func() []common.Upstream {
		ptrs := n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId)
		out := make([]common.Upstream, len(ptrs))
		for i, u := range ptrs {
			out[i] = u
		}
		return out
	}
	return n.policyEngine.RegisterNetwork(n.networkId, n.Label(), upstreamsFn, cfg)
}

// PinUpstreamOrderForTest pins the upstream ordering for this network to
// the given IDs (or alphabetical-by-id if `ids` is empty). Affects both
// the underlying registry (so cold-start reads see the pinned order) and
// the policy engine's cache when one is wired up. Test-only.
func (n *Network) PinUpstreamOrderForTest(ids ...string) {
	if n.upstreamsRegistry != nil {
		n.upstreamsRegistry.OverrideOrderForTest(n.networkId, ids...)
	}
	if n.policyEngine != nil {
		policy.OverrideOrderForTest(n.policyEngine, n.networkId, ids...)
	}
}

func (n *Network) Id() string {
	return n.networkId
}

// MetricsTracker returns the network's shared health tracker. Exposed
// so diagnostic tooling (the erpc-simulator, admin readouts) can read
// per-upstream observed metrics — the same numbers the selection
// policy's `keepHealthy` / `sortByScore` filters consume.
func (n *Network) MetricsTracker() *health.Tracker {
	return n.metricsTracker
}

// EvmBlockTime returns the network's estimated (EMA) block time, or 0 if not
// yet known. Used by the cache layer to derive realtime TTLs from block cadence.
func (n *Network) EvmBlockTime() time.Duration {
	if n.metricsTracker == nil {
		return 0
	}
	return n.metricsTracker.GetNetworkBlockTime(n.networkId)
}

// AllUpstreams returns every upstream configured on the network, in
// no particular order. Diagnostic tooling uses this to walk upstreams
// for tracker lookups without needing to know the routing order.
func (n *Network) AllUpstreams() []*upstream.Upstream {
	if n.upstreamsRegistry == nil {
		return nil
	}
	return n.upstreamsRegistry.GetNetworkUpstreams(context.Background(), n.networkId)
}

// PolicyScores returns the per-upstream `score` map produced by the
// selection-policy engine's most recent tick for `(networkID, method)`,
// or nil if the engine isn't wired up. Source of truth for "what does
// the policy rank this upstream at?" — never re-implement the PREFER_FASTEST
// weight formula client-side; read from here.
func (n *Network) PolicyScores(method string) map[string]float64 {
	if n.policyEngine == nil {
		return nil
	}
	if method == "" {
		method = "*"
	}
	return n.policyEngine.GetScores(n.networkId, method, "*")
}

// RecentPolicyDecisions returns up to `limit` most-recent policy
// engine Decisions for `(networkID, method)`, OLDEST-first. Diagnostic
// tooling uses this to render a tick-by-tick replay panel. Returns nil
// if no engine is wired up.
func (n *Network) RecentPolicyDecisions(method string, limit int) []*policy.Decision {
	if n.policyEngine == nil {
		return nil
	}
	if method == "" {
		method = "*"
	}
	return n.policyEngine.RecentDecisions(n.networkId, method, "*", limit)
}

// PolicyLastSwitchAt returns when the primary upstream last changed
// for `(networkID, method)`. Used by diagnostics to render the
// `stickyPrimary` cooldown countdown ("primary held for Xs").
func (n *Network) PolicyLastSwitchAt(method string) time.Time {
	if n.policyEngine == nil {
		return time.Time{}
	}
	if method == "" {
		method = "*"
	}
	return n.policyEngine.LastSwitchAt(n.networkId, method, "*")
}

// PolicyOrderedUpstreams returns the IDs of upstreams in the order the
// selection-policy engine currently has them ordered for the given
// method. The slot's cache is read lock-free — this is the same source
// of truth `Forward` uses to pick attempts. Returns nil if no policy
// engine is wired up or the slot hasn't ticked yet.
//
// Diagnostics tooling (the simulator, admin endpoints) uses this to
// render "position pills" that reflect the policy's real verdict —
// NOT a guess derived from per-second selection counts.
func (n *Network) PolicyOrderedUpstreams(method string) []string {
	if n.policyEngine == nil {
		return nil
	}
	if method == "" {
		method = "*"
	}
	ups := n.policyEngine.GetOrdered(n.networkId, method, "*")
	if len(ups) == 0 {
		return nil
	}
	out := make([]string, 0, len(ups))
	for _, u := range ups {
		out = append(out, u.Id())
	}
	return out
}

// SetPolicyEnginePaused gates the per-slot ticker on this network's
// selection-policy engine. While paused every slot still wakes on each
// tick but skips `tickOnce`, so the cached ordering — what `Forward`
// reads via `policyEngine.GetOrdered` — stays frozen at the last
// verdict. No-op if no policy engine is wired up (test-only networks,
// or YAML without a `selectionPolicy` block).
//
// Wired up so the eRPC simulator's pause button can stop the policy
// engine churning while traffic generation is halted. Production
// callers shouldn't need this — leave the engine running.
func (n *Network) SetPolicyEnginePaused(paused bool) {
	if n.policyEngine == nil {
		return
	}
	n.policyEngine.SetPaused(paused)
}

// SetPolicyStepLogEnabled toggles per-tick capture of the selection
// policy's chain trail. While enabled the engine's `Decision` carries
// `Output.StepLog` (the chain timeline) and DEBUG-level logs print one
// line per step + one per excluded upstream.
//
// Off by default in production (zero overhead beyond a function-call
// indirection per stdlib step). Flipped on by the simulator at boot so
// the policy-history drawer has data; production callers running with
// DEBUG-level logs may also want to enable it for incident triage.
func (n *Network) SetPolicyStepLogEnabled(enabled bool) {
	if n.policyEngine == nil {
		return
	}
	n.policyEngine.SetStepLogEnabled(enabled)
}

func (n *Network) Label() string {
	if n == nil {
		return ""
	}
	if n.networkLabel != "" {
		return n.networkLabel
	}
	return n.networkId
}

func (n *Network) ProjectId() string {
	return n.projectId
}

func (n *Network) Architecture() common.NetworkArchitecture {
	if n.cfg.Architecture == "" {
		if n.cfg.Evm != nil {
			n.cfg.Architecture = common.ArchitectureEvm
		}
	}

	return n.cfg.Architecture
}

func (n *Network) ShadowUpstreams() []*upstream.Upstream {
	return n.upstreamsRegistry.GetNetworkShadowUpstreams(n.networkId)
}

func (n *Network) Logger() *zerolog.Logger {
	return n.logger
}

// gatherEvmTipInputsForMethod builds the picker inputs from the upstreams that
// the selection policy considers ELIGIBLE for `method` — so an upstream the
// user (or an integrity guard) excluded/cordoned drops out of head tracking in
// the same place it drops out of routing. `method == "*"` yields the
// network-wide eligible set (the global served tip); a concrete method yields
// the per-method eligible set (a capability lane). Falls back to the full
// registered set on cold start / when the policy has produced no decision yet.
func (n *Network) gatherEvmTipInputsForMethod(
	ctx context.Context,
	useFinalized bool,
	method string,
) []evm.ServedTipInput {
	upstreams := n.tipCandidateUpstreams(ctx, method)
	out := make([]evm.ServedTipInput, 0, len(upstreams))
	for _, cu := range upstreams {
		u, ok := cu.(common.EvmUpstream)
		if !ok || u.EvmStatePoller() == nil {
			continue
		}
		if u.EvmSyncingState() == common.EvmSyncingStateSyncing {
			continue
		}
		var blk int64
		if useFinalized {
			blk = u.EvmEffectiveFinalizedBlock()
		} else {
			blk = u.EvmEffectiveLatestBlock()
		}
		if blk <= 0 {
			continue
		}
		out = append(out, evm.ServedTipInput{
			UpstreamID:  u.Id(),
			BlockNumber: blk,
		})
	}
	return out
}

// tipCandidateUpstreams returns the eligible upstreams that feed the served-tip
// picker for `method`, sourced from the selection policy so head tracking and
// routing share one notion of "which upstreams count". Falls back to the full
// registered set when the policy is absent or has not yet produced a decision.
func (n *Network) tipCandidateUpstreams(ctx context.Context, method string) []common.Upstream {
	var ups []common.Upstream
	if n.policyEngine != nil {
		if eligible := n.policyEngine.GetOrdered(n.networkId, method, "*"); len(eligible) > 0 {
			ups = eligible
		}
	}
	if ups == nil {
		if n.upstreamsRegistry == nil {
			// Defensive: a partially-initialized network (e.g. cache-only paths or tests
			// that don't wire a registry) has none — report no candidates so the head
			// accessors fail open (return 0) instead of dereferencing a nil registry.
			return nil
		}
		raw := n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId)
		ups = make([]common.Upstream, 0, len(raw))
		for _, u := range raw {
			ups = append(ups, u)
		}
	}

	// Selector-scoped served tip: when the request targets a subset of
	// upstreams (the use-upstream id/tag selector), the network's
	// `latest`/`finalized` must be decided AMONG that subset — otherwise a
	// more-ahead group (e.g. base flashblocks vs normal) would define the tip
	// for a request pinned to the other group, causing "block not found"
	// churn. This is stateless (no shared counter / no materialized state),
	// so an arbitrary selector value can never grow per-pod memory. No-op when
	// there is no request or no selector in ctx (background/admin callers).
	if sel := requestSelector(ctx); sel != "" {
		filtered := make([]common.Upstream, 0, len(ups))
		for _, u := range ups {
			if m, _ := common.UpstreamMatchesSelector(sel, u); m {
				filtered = append(filtered, u)
			}
		}
		return filtered
	}
	return ups
}

// requestSelector returns the use-upstream selector for the request bound to
// ctx (set at the top of Forward), or "" when there is no request/selector.
func requestSelector(ctx context.Context) string {
	if req, ok := ctx.Value(common.RequestContextKey).(*common.NormalizedRequest); ok && req != nil {
		if d := req.Directives(); d != nil {
			return d.UseUpstream
		}
	}
	return ""
}

// servedTipPartitionFor returns the lazily-materialized telemetry lane for a
// GROUP-scoped request, or nil to signal the unlabeled stateless path. The
// partition key (and its bounded/DDoS-safe gating) is computed by
// partitionKeyFor; the count is capped by maxServedTipPartitions.
func (n *Network) servedTipPartitionFor(ctx context.Context, selector string) *servedTipPartition {
	key, ids := n.partitionKeyFor(ctx, selector)
	if key == "" {
		return nil
	}
	// Fast path: already materialized.
	if p, ok := n.servedTipPartitions.Load(key); ok {
		return p.(*servedTipPartition)
	}
	// Cap backstop (also bounds a race racing past the cap).
	if n.servedTipPartitionCount.Load() >= maxServedTipPartitions {
		return nil
	}
	p := &servedTipPartition{lane: common.LaneName(ids)}
	actual, loaded := n.servedTipPartitions.LoadOrStore(key, p)
	if !loaded {
		n.servedTipPartitionCount.Add(1)
	}
	return actual.(*servedTipPartition)
}

// partitionKeyFor maps a use-upstream selector to a STABLE served-tip partition
// key, or "" when the selector must use the stateless path.
//
// A per-group monotonic tracker is materialized only for a "simple" selector (a
// single glob token, optionally negated — e.g. `flashblocks*`, `!flashblocks*`,
// or an exact tag like `family:systx`) that carves out a real SUB-group of the
// network's upstreams. The key is a hash of the MATCHED UPSTREAM SET, not the
// selector text, which makes the whole thing bounded and cross-pod safe:
//
//   - `flashblocks*`, `fl*`, `flash*`, and a `family:flashblocks` tag that all
//     resolve to the same upstreams collapse to ONE partition (dedup);
//   - prefix-enumeration / garbage selectors cannot inflate state beyond the
//     (small, topology-bounded) number of real groupings — an attacker can't
//     manufacture more distinct sets than the upstream layout allows;
//   - every pod derives the same key from the same config, so the shared
//     counter is cross-pod consistent.
//
// A group must match >=2 upstreams (single-node targeting needs no group
// counter — the per-upstream poller is already monotonic) and fewer than ALL
// (matching everything is just the network-wide tip). A per-network cap
// (maxServedTipPartitions) backstops pathological topologies. Anything else
// returns "" (stateless cluster-min over the subset).
func (n *Network) partitionKeyFor(ctx context.Context, selector string) (string, []string) {
	if !isSimpleGroupSelector(selector) || n.upstreamsRegistry == nil {
		return "", nil
	}
	all := n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId)
	if len(all) < 2 {
		return "", nil
	}
	matched := make([]string, 0, len(all))
	for _, u := range all {
		if m, _ := common.UpstreamMatchesSelector(selector, u); m {
			matched = append(matched, u.Id())
		}
	}
	if len(matched) < 2 || len(matched) >= len(all) {
		return "", nil
	}
	slices.Sort(matched)
	sum := sha256.Sum256([]byte(strings.Join(matched, "\x00")))
	return "grp:" + hex.EncodeToString(sum[:8]), matched
}

// isSimpleGroupSelector reports whether a selector is a single glob token
// (optionally negated) rather than a boolean expression — keeping automatic
// group materialization to simple patterns like `flashblocks*` / `!flashblocks*`
// and bounding the cost of resolving the matched set. Boolean combinators,
// grouping, whitespace, and embedded negation are rejected.
func isSimpleGroupSelector(selector string) bool {
	s := strings.TrimSpace(selector)
	if s == "" || len(s) > 128 {
		return false
	}
	if strings.ContainsAny(s, " \t\r\n()|&,") {
		return false
	}
	// Allow at most one negation, only at the very start (`!prefix*`).
	if i := strings.IndexByte(s, '!'); i > 0 || strings.Count(s, "!") > 1 {
		return false
	}
	return true
}

// EvmHighestLatestBlockNumber returns the served latest block for this network.
//
// In the default max mode it is the MAX effective latest block across eligible
// non-syncing upstreams. When the served tip is enabled (EvmServedTipConfig),
// it is instead the freshest block a strict MAJORITY of the eligible upstreams
// already have — so interpolated requests land on upstreams that can serve the
// advertised block. Advertising a block visible on only the single most-ahead
// upstream is what causes the "block not found" churn the majority mode avoids.
func (n *Network) EvmHighestLatestBlockNumber(ctx context.Context) int64 {
	ctx, span := common.StartDetailSpan(ctx, "Network.EvmHighestLatestBlockNumber")
	defer span.End()

	if !n.servedTipEnabledFor("latest") {
		// Max mode: evmHighestBlockMax → tipCandidateUpstreams already scopes
		// to the request's selector (if any), so the MAX is within-subset.
		return n.evmHighestBlockMax(ctx, false)
	}
	if sel := requestSelector(ctx); sel != "" {
		// Targeted request: the gather is already scoped to the selector's
		// subset, so the majority is within-group. A selector that names a
		// real configured group additionally gets its own lane (gauge labels
		// + watchdog anchor); arbitrary selectors emit no gauges.
		if p := n.servedTipPartitionFor(ctx, sel); p != nil {
			return n.servedTip(ctx, span, false, "latest", &p.latestAnchor, p.lane)
		}
		return n.servedTip(ctx, span, false, "latest", nil, servedTipLaneNone)
	}
	return n.servedTip(ctx, span, false, "latest", &n.servedLatestAnchor, "")
}

// servedTipEnabledFor reports whether the majority served tip is enabled for
// the given block tag ("latest"/"finalized") on this (EVM) network. Default is
// the max mode for every tag — see EvmServedTipConfig.
func (n *Network) servedTipEnabledFor(tag string) bool {
	return n.cfg != nil && n.cfg.Evm != nil && n.cfg.Evm.ServedTipEnabledFor(tag)
}

// evmHighestBlockMax returns the MAX effective latest/finalized block across the
// eligible, non-syncing upstreams — the default max-mode served tip used when
// the majority served tip is not enabled for this network.
func (n *Network) evmHighestBlockMax(ctx context.Context, useFinalized bool) int64 {
	var maxBlock int64
	for _, cu := range n.tipCandidateUpstreams(ctx, "*") {
		u, ok := cu.(common.EvmUpstream)
		if !ok || u.EvmStatePoller() == nil || u.EvmSyncingState() == common.EvmSyncingStateSyncing {
			continue
		}
		b := u.EvmEffectiveLatestBlock()
		if useFinalized {
			b = u.EvmEffectiveFinalizedBlock()
		}
		if b > maxBlock {
			maxBlock = b
		}
	}
	return maxBlock
}

// tryShortCircuitFutureBlock returns a truthful null response (ok=true) when
// `req` is a concrete-numbered eth_getBlockByNumber lookup whose target block is
// beyond every eligible upstream's head (at the network's emptyResultConfidence level).
// No upstream can serve such a block yet, so dispatching + hedging across all of
// them only burns latency and load before they each return empty — returning the
// null here skips that fan-out entirely.
//
// Safety: it compares against the MAX observed head across eligible upstreams
// (not the majority served tip), so it never nulls out a block the most-ahead
// upstream actually has. It is gated on served-tip being enabled for the latest
// axis — the same opt-in that makes the head trustworthy — and the synthesized
// response is returned directly from Forward, so it is never written to cache
// (the block will exist later). The post-forward empty guard remains as
// defense-in-depth for the in-flight case where an upstream advances mid-request.
func (n *Network) tryShortCircuitFutureBlock(ctx context.Context, req *common.NormalizedRequest, method string) (*common.NormalizedResponse, bool) {
	if n.cfg == nil || n.cfg.Evm == nil || !n.servedTipEnabledFor("latest") {
		return nil, false
	}
	if !strings.EqualFold(method, "eth_getBlockByNumber") {
		return nil, false
	}
	_, bn, err := evm.ExtractBlockReferenceFromRequest(ctx, req)
	if err != nil || bn <= 0 {
		// Tags ("latest"/"pending"/...), block-hash lookups, or unparseable
		// params carry no concrete future number — never short-circuit.
		return nil, false
	}
	useFinalized := n.cfg.Evm.EmptyResultConfidence == common.AvailbilityConfidenceFinalized
	maxHead := n.evmHighestBlockMax(ctx, useFinalized)
	if maxHead <= 0 || bn <= maxHead {
		// Unknown head (fail open) or block within reach of some upstream.
		return nil, false
	}
	jrr, err := common.NewJsonRpcResponse(req.ID(), nil, nil)
	if err != nil {
		return nil, false
	}
	resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
	resp.SetEvmBlockNumber(bn)
	return resp, true
}

// servedTip computes the majority served tip for one axis over the
// selection-policy-eligible upstreams — the freshest block a strict majority
// of them already has (see evm.PickServedTip) — applies the guaranteed-method
// floor, and exports the gauges. STATELESS by design: nothing is persisted
// and nothing predicted, so no inherited or rogue value can ever pin, freeze
// or poison the result (networks_served_tip_invariants_test.go pins those
// outcomes against the 2026-06 production incident).
func (n *Network) servedTip(
	ctx context.Context,
	span trace.Span,
	useFinalized bool,
	axis string,
	anchor *servedTipAnchor,
	lane string,
) int64 {
	tips := n.gatherEvmTipInputsForMethod(ctx, useFinalized, "*")
	pick := evm.PickServedTip(tips)

	// Capability guarantee (#855): the served tip must never exceed what any
	// configured guaranteed-method's supporting upstreams can serve, so a
	// request on such a method (e.g. trace_*) never resolves "latest" to a
	// block only non-supporting upstreams have.
	if pick.Tip > 0 {
		if floor := n.guaranteedMethodFloor(ctx, useFinalized); floor > 0 && floor < pick.Tip {
			pick.Tip = floor
		}
	}

	if common.IsTracingDetailed {
		span.SetAttributes(
			attribute.String("served_tip.axis", axis),
			attribute.Int64("served_tip.tip", pick.Tip),
			attribute.Int64("served_tip.freshest", pick.Freshest),
			attribute.Int("served_tip.inputs", pick.Inputs),
		)
	}

	// Watchdog anchor: when did this process last see the served value
	// change. Telemetry only — nothing feeds back into the pick.
	advanceAge := time.Duration(-1)
	if anchor != nil {
		if pick.Tip > 0 {
			anchor.observe(pick.Tip)
		}
		advanceAge = anchor.age()
	}
	n.observeServedTipMetrics(axis, lane, pick, advanceAge)
	return pick.Tip
}

// EvmHighestFinalizedBlockNumber is the finalized-axis sibling of
// EvmHighestLatestBlockNumber. Same majority semantics, applied to each
// upstream's EvmEffectiveFinalizedBlock.
func (n *Network) EvmHighestFinalizedBlockNumber(ctx context.Context) int64 {
	ctx, span := common.StartDetailSpan(ctx, "Network.EvmHighestFinalizedBlockNumber", trace.WithAttributes(
		attribute.String("network.id", n.networkId),
	))
	defer span.End()

	if !n.servedTipEnabledFor("finalized") {
		return n.evmHighestBlockMax(ctx, true)
	}
	if sel := requestSelector(ctx); sel != "" {
		// See EvmHighestLatestBlockNumber: a configured-tag selector gets its
		// own lane (telemetry labels + watchdog anchor); any other selector
		// computes the same stateless majority without emitting gauges.
		if p := n.servedTipPartitionFor(ctx, sel); p != nil {
			return n.servedTip(ctx, span, true, "finalized", &p.finalizedAnchor, p.lane)
		}
		return n.servedTip(ctx, span, true, "finalized", nil, servedTipLaneNone)
	}
	return n.servedTip(ctx, span, true, "finalized", &n.servedFinalizedAnchor, "")
}

// guaranteedMethodFloor returns the lowest majority served tip across the
// configured GuaranteedMethods' supporting (eligible) upstream sets, or 0 when
// no guaranteed methods are configured or none constrain the tip. Each method's
// supporting set is the selection-policy-eligible set for that method (which,
// via autoIgnoreUnsupportedMethods, excludes upstreams that don't support it).
func (n *Network) guaranteedMethodFloor(ctx context.Context, useFinalized bool) int64 {
	if n.cfg == nil || n.cfg.Evm == nil || n.cfg.Evm.ServedTip == nil {
		return 0
	}
	methods := n.cfg.Evm.ServedTip.GuaranteedMethods
	if len(methods) == 0 {
		return 0
	}
	eligible := n.tipCandidateUpstreams(ctx, "*")
	var floor int64
	for _, m := range methods {
		tips := make([]evm.ServedTipInput, 0, len(eligible))
		for _, cu := range eligible {
			u, ok := cu.(common.EvmUpstream)
			if !ok || u.EvmStatePoller() == nil || u.EvmSyncingState() == common.EvmSyncingStateSyncing {
				continue
			}
			// Supporting set = eligible upstreams that handle this method.
			if handle, _ := cu.ShouldHandleMethod(m); !handle {
				continue
			}
			blk := u.EvmEffectiveLatestBlock()
			if useFinalized {
				blk = u.EvmEffectiveFinalizedBlock()
			}
			if blk <= 0 {
				continue
			}
			tips = append(tips, evm.ServedTipInput{UpstreamID: u.Id(), BlockNumber: blk})
		}
		if len(tips) == 0 {
			// No supporting upstream for this method → no constraint (fall through
			// rather than pinning the tip to 0).
			continue
		}
		if t := evm.PickServedTip(tips).Tip; t > 0 && (floor == 0 || t < floor) {
			floor = t
		}
	}
	return floor
}

// observeServedTipMetrics exports the served-tip gauges. axis
// ("latest"|"finalized") is a label rather than part of the metric name to
// keep the gauge set small. lane="all" is the network-wide pick; a named lane
// is a use-upstream group's own pick; the stateless lane-none sentinel emits
// nothing (a subset value must never overwrite a gauge).
func (n *Network) observeServedTipMetrics(axis string, lane string, pick evm.ServedTipPick, advanceAge time.Duration) {
	if lane == servedTipLaneNone {
		return
	}
	laneLabel := lane
	if laneLabel == "" {
		laneLabel = servedTipLaneAll
	}
	if pick.Tip > 0 {
		telemetry.MetricNetworkServedTipBlockNumber.
			WithLabelValues(n.projectId, n.Label(), laneLabel, axis).
			Set(float64(pick.Tip))
		// Deliberate lag: how far the majority tip sits behind the single
		// freshest upstream view in the same set.
		telemetry.MetricNetworkServedTipLagBlocks.
			WithLabelValues(n.projectId, n.Label(), laneLabel, axis).
			Set(float64(pick.Freshest - pick.Tip))
	}
	// Universal stuck-tip signal: seconds since this process last saw the
	// served value change. Skipped until a first change is observed.
	if advanceAge >= 0 {
		telemetry.MetricNetworkServedTipAdvanceAgeSeconds.
			WithLabelValues(n.projectId, n.Label(), laneLabel, axis).
			Set(advanceAge.Seconds())
	}
}

func (n *Network) EvmLowestFinalizedBlockNumber(ctx context.Context) int64 {
	ctx, span := common.StartDetailSpan(ctx, "Network.EvmLowestFinalizedBlockNumber", trace.WithAttributes(
		attribute.String("network.id", n.networkId),
	))
	defer span.End()

	upstreams := n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId)
	var minBlock int64 = 0
	var initialized bool = false

	for _, u := range upstreams {
		statePoller := u.EvmStatePoller()
		if statePoller == nil {
			continue
		}

		// Check if the node is syncing - skip syncing nodes as their block numbers may be unreliable
		if u.EvmSyncingState() == common.EvmSyncingStateSyncing {
			n.logger.Debug().Str("upstreamId", u.Id()).Msg("skipping syncing upstream for lowest finalized block calculation")
			continue
		}

		// Use effective finalized block which considers blockAvailability.upper config
		upBlock := u.EvmEffectiveFinalizedBlock()
		// Skip upstreams that haven't determined finalized block yet (returning 0)
		if upBlock > 0 {
			if !initialized || upBlock < minBlock {
				minBlock = upBlock
				initialized = true
			}
		}
	}

	span.SetAttributes(attribute.Int64("lowest_finalized_block", minBlock))
	return minBlock
}

func (n *Network) EvmLeaderUpstream(ctx context.Context) common.Upstream {
	var leader common.Upstream
	var leaderLastBlock int64 = 0
	upsList := n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId)
	for _, u := range upsList {
		if statePoller := u.EvmStatePoller(); statePoller != nil {
			lastBlock := statePoller.LatestBlock()
			if lastBlock > leaderLastBlock {
				leader = u
				leaderLastBlock = lastBlock
			}
		}
	}
	return leader
}

func (n *Network) getFailsafeExecutor(ctx context.Context, req *common.NormalizedRequest) *networkExecutor {
	method, _ := req.Method()
	finality := req.Finality(ctx)

	// Iterate through executors in config order and return the first match.
	// This respects the user-defined priority order in the config file.
	for _, fe := range n.failsafeExecutors {
		mp := fe.MatchMethod()
		methodMatches := mp == "*"
		if !methodMatches {
			methodMatches, _ = common.WildcardMatch(mp, method)
		}

		fl := fe.MatchFinality()
		finalityMatches := len(fl) == 0 || slices.Contains(fl, finality)

		if methodMatches && finalityMatches {
			return fe
		}
	}

	return nil
}

func (n *Network) Forward(ctx context.Context, req *common.NormalizedRequest) (*common.NormalizedResponse, error) {
	startTime := time.Now()
	req.SetNetwork(n)
	req.SetCacheDal(n.cacheDal)

	// Apply default directives from config
	req.ApplyDirectiveDefaults(n.cfg.DirectiveDefaults)

	method, _ := req.Method()
	lg := n.logger.With().Str("method", method).Interface("id", req.ID()).Str("ptr", fmt.Sprintf("%p", req)).Logger()
	var resp *common.NormalizedResponse
	var err error
	var upstreamCalls atomic.Int64
	defer func() {
		telemetry.ObserveNetworkUpstreamCallsPerRequest(
			n.projectId,
			n.Label(),
			method,
			int(upstreamCalls.Load()),
		)
	}()

	// Start a span for network forwarding
	ctx, forwardSpan := common.StartSpan(ctx, "Network.Forward",
		trace.WithAttributes(
			attribute.String("network.id", n.networkId),
			attribute.String("request.method", method),
			attribute.String("request.finality", req.Finality(ctx).String()),
		),
	)

	// Bind the request to ctx so downstream served-tip resolution can decide
	// `latest`/`finalized` AMONG the use-upstream-selected subset (see
	// tipCandidateUpstreams / requestSelector). The failsafe executor re-sets
	// the same value below for its own readers; setting it here makes it
	// available to the earlier pre-forward / short-circuit paths too.
	ctx = context.WithValue(ctx, common.RequestContextKey, req)

	if common.IsTracingDetailed {
		forwardSpan.SetAttributes(
			attribute.String("request.id", fmt.Sprintf("%v", req.ID())),
			attribute.String("user.id", req.UserId()),
			attribute.String("agent.name", req.AgentName()),
		)
	}
	defer forwardSpan.End()

	if lg.GetLevel() == zerolog.TraceLevel {
		lg.Debug().Object("request", req).Msgf("forwarding request for network")
	} else {
		lg.Debug().Msgf("forwarding request for network")
	}

	// Static response short-circuit. Checked after method extraction and before
	// the multiplexer/cache/upstream-selection path so matching requests never
	// touch any upstream. See StaticResponseConfig for match semantics.
	if len(n.cfg.StaticResponses) > 0 {
		if resp, ok := n.tryServeStaticResponse(ctx, &lg, req, method); ok {
			forwardSpan.SetAttributes(attribute.Bool("static_response.hit", true))
			return resp, nil
		}
	}

	var mlx *Multiplexer
	checkAllUpstreams := req.ShouldCheckAllUpstreams()
	forwardSpan.SetAttributes(attribute.Bool("check_all_upstreams", checkAllUpstreams))
	if !checkAllUpstreams {
		mlx, resp, err = n.handleMultiplexing(ctx, &lg, req, startTime)
		if err != nil || resp != nil {
			// When the original request is already fulfilled by multiplexer (follower path)
			forwardSpan.SetAttributes(
				attribute.Bool("multiplexed", true),
				attribute.String("multiplexer.role", "follower"),
			)
			if err != nil {
				common.SetTraceSpanError(forwardSpan, err)
			}
			return resp, err
		}
		if mlx != nil {
			forwardSpan.SetAttributes(
				attribute.String("multiplexer.hash", mlx.hash),
				attribute.String("multiplexer.role", "leader"),
			)
			defer n.cleanupMultiplexer(mlx)
		}
	}

	if n.cacheDal != nil && !req.ShouldSkipCacheRead("") && !checkAllUpstreams {
		lg.Debug().Msgf("checking cache for request")
		resp, err := n.cacheDal.Get(ctx, req)
		if err != nil {
			lg.Debug().Err(err).Msgf("could not find response in cache")
		} else if resp != nil && !resp.IsObjectNull(ctx) {
			logCacheHit(&lg, resp)
			if mlx != nil {
				mlx.Close(ctx, resp, err)
			}
			forwardSpan.SetAttributes(attribute.Bool("cache.hit", true))
			return resp, err
		}
		forwardSpan.SetAttributes(attribute.Bool("cache.hit", false))
	}

	if negErr, negHit := n.loadDeterministicNegativeCache(ctx, req, method); negHit {
		if mlx != nil {
			mlx.Close(ctx, nil, negErr)
		}
		forwardSpan.SetAttributes(attribute.Bool("negative_cache.hit", true))
		common.SetTraceSpanError(forwardSpan, negErr)
		return nil, negErr
	}
	forwardSpan.SetAttributes(attribute.Bool("negative_cache.hit", false))

	// Get failsafe executor first to know if we need to filter upstreams by group
	failsafeExecutor := n.getFailsafeExecutor(ctx, req)
	if failsafeExecutor == nil {
		err := errors.New("no failsafe executor found for this request")
		if mlx != nil {
			mlx.Close(ctx, nil, err)
		}
		return nil, err
	}

	useUpstreamDirective := ""
	if req.Directives() != nil {
		useUpstreamDirective = req.Directives().UseUpstream
	}
	requiredCapabilities := n.requiredCapabilitiesForMethod(method)
	upstreamGroup := failsafeExecutor.MatchUpstreamGroup()

	loadUpstreams := func() ([]common.Upstream, error) {
		_, upstreamSpan := common.StartDetailSpan(ctx, "GetSortedUpstreams")
		getSortedUpstreams := n.getSortedUpstreamsFn
		if getSortedUpstreams == nil {
			getSortedUpstreams = defaultGetSortedUpstreamsForNetwork
		}
		upsList, err := getSortedUpstreams(ctx, n.upstreamsRegistry, n.networkId, method)
		upstreamSpan.SetAttributes(attribute.Int("upstreams.count", len(upsList)))
		if common.IsTracingDetailed {
			names := make([]string, len(upsList))
			for i, u := range upsList {
				names[i] = u.Id()
			}
			upstreamSpan.SetAttributes(
				attribute.String("upstreams.list", strings.Join(names, ", ")),
			)
		}
		upstreamSpan.End()

		if err != nil {
			return nil, err
		}

		// Filter upstreams by group if the failsafe executor specifies a group.
		// Skip group filtering if UseUpstream directive is set - allows targeting any upstream for debugging.
		if upstreamGroup != "" && useUpstreamDirective == "" {
			filteredUpstreams := make([]common.Upstream, 0, len(upsList))
			for _, u := range upsList {
				if upstreamMatchesFailsafeGroup(u, upstreamGroup) {
					filteredUpstreams = append(filteredUpstreams, u)
				}
			}
			lg.Debug().
				Str("upstreamGroup", upstreamGroup).
				Int("originalCount", len(upsList)).
				Int("filteredCount", len(filteredUpstreams)).
				Msgf("filtered upstreams by group for failsafe policy")
			if len(filteredUpstreams) == 0 {
				return nil, common.NewErrFailsafeConfiguration(
					fmt.Errorf("no upstreams match the configured group '%s' for failsafe policy (had %d upstreams before filtering, method=%s)",
						upstreamGroup, len(upsList), method),
					map[string]interface{}{
						"upstreamGroup":  upstreamGroup,
						"originalCount":  len(upsList),
						"method":         method,
						"failsafeMethod": failsafeExecutor.MatchMethod(),
					},
				)
			}
			upsList = filteredUpstreams

			forwardSpan.SetAttributes(
				attribute.Int("upstreams.filtered_count", len(upsList)),
				attribute.String("upstreams.filter_group", upstreamGroup),
			)
		}

		if len(requiredCapabilities) > 0 {
			filteredByCapabilities := filterUpstreamsByRequiredCapabilities(upsList, requiredCapabilities)
			if len(filteredByCapabilities) == 0 {
				return nil, common.NewErrFailsafeConfiguration(
					fmt.Errorf("no upstreams satisfy required capabilities %v for method %s", requiredCapabilities, method),
					map[string]interface{}{
						"method":               method,
						"requiredCapabilities": requiredCapabilities,
						"originalCount":        len(upsList),
					},
				)
			}
			upsList = filteredByCapabilities
			forwardSpan.SetAttributes(
				attribute.Int("upstreams.capability_filtered_count", len(upsList)),
				attribute.String("upstreams.required_capabilities", strings.Join(requiredCapabilities, ",")),
			)
		}

		return upsList, nil
	}

	var upsList []common.Upstream
	batchSelectionCache := common.BatchUpstreamSelectionCacheFromContext(ctx)
	if batchSelectionCache != nil {
		key := common.BatchUpstreamSelectionKey{
			NetworkID:     n.networkId,
			Method:        method,
			Finality:      req.Finality(ctx),
			UseUpstream:   useUpstreamDirective,
			UpstreamGroup: upstreamGroup,
		}
		var cacheHit bool
		upsList, cacheHit, err = batchSelectionCache.Resolve(key, loadUpstreams)
		forwardSpan.SetAttributes(
			attribute.Bool("upstreams.batch_selection_cache_enabled", true),
			attribute.Bool("upstreams.batch_selection_cache_hit", cacheHit),
		)
	} else {
		upsList, err = loadUpstreams()
	}

	if len(upsList) == 0 {
		err := common.NewErrNoUpstreamsFound(n.projectId, n.networkId)
		common.SetTraceSpanError(forwardSpan, err)
		if mlx != nil {
			mlx.Close(ctx, nil, err)
		}
		return nil, err
	}

	// Set upstreams on the request
	req.SetUpstreams(upsList)

	// Feed the per-network probe-bus AFTER we know the request is
	// actually going to dispatch to an upstream (i.e. not a
	// cache-hit / static-response / follower-multiplexer
	// short-circuit, all of which returned earlier). The publish is
	// non-blocking and drops on overflow — request latency is never
	// affected. The Prober (if any) samples from this feed to mirror
	// the request against currently-excluded upstreams so their
	// tracker counters get refreshed without touching real traffic.
	// No-op for networks whose policy chain doesn't include
	// `probeExcluded`.
	if n.policyEngine != nil {
		n.policyEngine.PublishRequest(n.networkId, req)
	}

	// Network-level pre-forward (executed after upstream selection) for upstream-aware logic
	if handled, resp, err := evm.HandleNetworkPreForward(ctx, n, upsList, req); handled {
		if err != nil {
			if mlx != nil {
				mlx.Close(ctx, nil, err)
			}
			return nil, err
		}
		if mlx != nil {
			mlx.Close(ctx, resp, nil)
		}
		return resp, nil
	}

	// Future-block short-circuit: a concrete block number beyond every eligible
	// upstream's head cannot be served yet — return the truthful null instead of
	// dispatching + hedging across upstreams that will all return empty. Runs
	// before rate limiting so a non-dispatched request consumes no permit.
	if resp, ok := n.tryShortCircuitFutureBlock(ctx, req, method); ok {
		forwardSpan.SetAttributes(attribute.Bool("future_block.short_circuit", true))
		if mlx != nil {
			mlx.Close(ctx, resp, nil)
		}
		return resp, nil
	}

	// 3) Check if we should handle this method on this network
	if err := n.shouldHandleMethod(req, method, upsList); err != nil {
		if mlx != nil {
			mlx.Close(ctx, nil, err)
		}
		return nil, err
	}

	// 3) Apply rate limits
	if !shouldSkipNetworkRateLimit(ctx) {
		if err := n.acquireRateLimitPermit(ctx, req); err != nil {
			if mlx != nil {
				mlx.Close(ctx, nil, err)
			}
			return nil, err
		}
	}

	// 4) Prepare the request
	if err := n.prepareRequest(ctx, req); err != nil {
		if mlx != nil {
			mlx.Close(ctx, nil, err)
		}
		return nil, err
	}

	if checkAllUpstreams {
		resp, err := n.forwardAllUpstreamsCheck(ctx, req, upsList, method)
		if err != nil {
			common.SetTraceSpanError(forwardSpan, err)
		}
		return resp, err
	}

	// 5) Iterate over upstreams and forward the request until success or fatal failure
	tryForward := func(
		u common.Upstream,
		req *common.NormalizedRequest,
		execSpanCtx context.Context,
		lg *zerolog.Logger,
		hedge int,
		attempt int,
		retry int,
	) (resp *common.NormalizedResponse, err error) {
		ctx, span := common.StartDetailSpan(execSpanCtx, "Network.TryForward")
		defer span.End()

		// hedge > 0 means failsafe spawned this attempt as a hedge (not the
		// primary). Threaded down explicitly into doForward → Upstream.Forward
		// so the per-upstream rate counters stay clean. The hedge policy lives
		// at this network layer, so this is where the signal originates.
		isHedgeAttempt := hedge > 0

		lg.Debug().Int("hedge", hedge).Int("attempt", attempt).Int("retry", retry).Msgf("trying to forward request to upstream")

		upstreamCalls.Add(1)

		resp, err = n.doForward(ctx, u, req, false, isHedgeAttempt)

		if err != nil && !common.IsNull(err) {
			// If upstream complains that the method is not supported let's dynamically add it ignoreMethods config
			if common.HasErrorCode(err, common.ErrCodeEndpointUnsupported) {
				go u.IgnoreMethod(method)
			}

			lg.Debug().Object("response", resp).Err(err).Msgf("finished forwarding request to upstream with error")
			return nil, err
		}

		lg.Debug().Object("response", resp).Msgf("finished forwarding request to upstream with success")

		return resp, err
	}
	// This is the only way to pass additional values to failsafe policy executors context
	ectx := context.WithValue(ctx, common.RequestContextKey, req)

	// Add tracing for which failsafe policy was selected
	forwardSpan.SetAttributes(
		attribute.String("failsafe.matched_method", failsafeExecutor.MatchMethod()),
		attribute.String("failsafe.matched_finalities", fmt.Sprintf("%v", failsafeExecutor.MatchFinality())),
		attribute.String("failsafe.matched_upstream_group", upstreamGroup),
	)

	// Build the per-execution upstream-loop closure. This is what the new
	// network executor invokes (potentially multiple times for retry/hedge,
	// and per-slot for consensus).
	sweepFn := func(execSpanCtx context.Context, effectiveReq *common.NormalizedRequest, oneUpstreamOnly bool) (*common.NormalizedResponse, error) {
		snap := effectiveReq.ExecState().Snapshot()
		_, execSpan := common.StartSpan(execSpanCtx, "Network.forwardAttempt",
			trace.WithAttributes(
				attribute.String("network.id", n.networkId),
				attribute.String("request.method", method),
				attribute.Int("execution.attempt", snap.Attempts),
				attribute.Int("execution.retry", snap.Retries),
				attribute.Int("execution.hedge", snap.Hedges),
			),
		)
		defer execSpan.End()

		if common.IsTracingDetailed {
			execSpan.SetAttributes(
				attribute.String("request.id", fmt.Sprintf("%v", effectiveReq.ID())),
			)
		}

		if ctxErr := execSpanCtx.Err(); ctxErr != nil {
			cause := context.Cause(execSpanCtx)
			if cause != nil {
				common.SetTraceSpanError(execSpan, cause)
				return nil, cause
			}
			common.SetTraceSpanError(execSpan, ctxErr)
			return nil, ctxErr
		}

		var bestResp *common.NormalizedResponse
		var lastErr error
		maxLoopIterations := effectiveReq.UpstreamsCount()
		if oneUpstreamOnly {
			maxLoopIterations = 1
		}
		attempted := make(map[string]struct{}, maxLoopIterations)

		for loopIteration := 0; loopIteration < maxLoopIterations; loopIteration++ {
			loopCtx, loopSpan := common.StartDetailSpan(execSpanCtx, "Network.UpstreamLoop")
			if ctxErr := loopCtx.Err(); ctxErr != nil {
				cause := context.Cause(loopCtx)
				if cause == nil {
					cause = ctxErr
				}
				common.SetTraceSpanError(loopSpan, cause)
				loopSpan.End()
				return nil, cause
			}

			u, selErr := effectiveReq.NextUpstream()
			if selErr != nil {
				loopSpan.SetAttributes(
					attribute.Bool("upstreams_exhausted", true),
					attribute.String("error", selErr.Error()),
				)
				loopSpan.End()
				break
			}

			if _, seen := attempted[u.Id()]; seen {
				effectiveReq.ConsumedUpstreams.Delete(u)
				loopSpan.SetAttributes(attribute.Bool("duplicate_selection", true))
				loopSpan.End()
				break
			}
			attempted[u.Id()] = struct{}{}

			loopSpan.SetAttributes(attribute.String("upstream.id", u.Id()))
			if common.IsTracingDetailed {
				loopSpan.SetAttributes(
					attribute.Float64("upstream.score", n.upstreamsRegistry.GetUpstreamScore(u.Id(), n.networkId, method)),
				)
			}
			if eu, ok := u.(common.EvmUpstream); ok {
				if sp := eu.EvmStatePoller(); sp != nil && !sp.IsObjectNull() {
					loopSpan.SetAttributes(
						attribute.Int64("upstream.latest_block", sp.LatestBlock()),
						attribute.Int64("upstream.finalized_block", sp.FinalizedBlock()),
					)
				}
			}

			ulg := lg.With().Str("upstreamId", u.Id()).Logger()
			ulg.Debug().
				Interface("id", effectiveReq.ID()).
				Str("ptr", fmt.Sprintf("%p", effectiveReq)).
				Str("selectedUpstream", u.Id()).
				Msg("selected upstream from list")

			if skipErr, isRetryable := n.checkUpstreamBlockAvailability(loopCtx, u, effectiveReq, method); skipErr != nil {
				n.handleBlockSkip(loopCtx, loopSpan, &ulg, u, effectiveReq, method, skipErr, isRetryable)
				loopSpan.End()
				continue
			}

			hedges := snap.Hedges
			attempts := snap.Attempts
			retries := snap.Retries
			if hedges > 0 {
				finality := effectiveReq.Finality(loopCtx)
				telemetry.CounterHandle(telemetry.MetricNetworkHedgedRequestTotal,
					n.projectId, n.Label(), u.Id(), method, fmt.Sprintf("%d", hedges),
					finality.String(), effectiveReq.UserId(), effectiveReq.AgentName(),
				).Inc()
			}
			if reason := classifyAttemptReason(failsafeExecutor.HasConsensus(), retries, hedges); reason != "" {
				telemetry.IncNetworkAttemptReason(n.projectId, n.Label(), method, reason)
			}

			r, err := tryForward(u, effectiveReq, loopCtx, &ulg, hedges, attempts, retries)
			if e := n.normalizeResponse(loopCtx, effectiveReq, r); e != nil {
				ulg.Error().Err(e).Msgf("failed to normalize response")
				err = e
			}
			effectiveReq.MarkUpstreamCompleted(loopCtx, u, r, err)

			if hedges > 0 && common.HasErrorCode(err, common.ErrCodeEndpointRequestCanceled) {
				n.recordHedgeDiscard(loopCtx, loopSpan, &ulg, u, effectiveReq, method, err, attempts, hedges)
				loopSpan.End()
				return nil, common.NewErrUpstreamHedgeCancelled(u.Id(), err)
			}
			_ = attempts

			if r != nil {
				r.SetUpstream(u)
				r.WithRequest(effectiveReq)
			}

			if err == nil && r != nil && !r.IsObjectNull() {
				emptyish := r.IsResultEmptyish()
				acceptEmpty := !emptyish ||
					(!failsafeExecutor.HasConsensus() &&
						slices.Contains(failsafeExecutor.EmptyResultAccept(), method))
				if acceptEmpty {
					st := effectiveReq.ExecState()
					st.MarkUpstreamAttemptWon(r.UpstreamId())
					s := st.Snapshot()
					r.SetAttempts(s.Attempts)
					r.SetRetries(s.Retries)
					r.SetHedges(s.Hedges)
					loopSpan.SetStatus(codes.Ok, "")
					if emptyish {
						loopSpan.SetAttributes(attribute.Bool("emptyish_accepted", true))
					}
					loopSpan.End()
					return r, nil
				}
			}

			if common.IsClientError(err) || common.HasErrorCode(err, common.ErrCodeEndpointExecutionException) {
				common.SetTraceSpanError(loopSpan, err)
				loopSpan.End()
				return nil, err
			}

			if err != nil {
				lastErr = err
				common.SetTraceSpanError(loopSpan, err)
			} else if r != nil {
				bestResp = r
				loopSpan.SetStatus(codes.Ok, "")
			}
			loopSpan.End()
		}

		if ctxErr := execSpanCtx.Err(); ctxErr != nil {
			cause := context.Cause(execSpanCtx)
			if cause == nil {
				cause = ctxErr
			}
			return nil, cause
		}

		if bestResp != nil {
			st := effectiveReq.ExecState()
			st.MarkUpstreamAttemptWon(bestResp.UpstreamId())
			s := st.Snapshot()
			bestResp.SetAttempts(s.Attempts)
			bestResp.SetRetries(s.Retries)
			bestResp.SetHedges(s.Hedges)
			return bestResp, nil
		}

		if oneUpstreamOnly && lastErr != nil {
			return nil, lastErr
		}

		s := effectiveReq.ExecState().Snapshot()
		exhaustedErr := common.NewErrUpstreamsExhausted(
			effectiveReq,
			&effectiveReq.ErrorsByUpstream,
			n.projectId,
			n.networkId,
			method,
			time.Since(startTime),
			s.Attempts,
			s.Retries,
			s.Hedges,
			len(upsList),
		)
		common.SetTraceSpanError(execSpan, exhaustedErr)
		return nil, exhaustedErr
	}

	tryOneUpstream := func(c context.Context, r *common.NormalizedRequest) (*common.NormalizedResponse, error) {
		return sweepFn(c, r, true)
	}
	runUpstreamSweep := func(c context.Context, r *common.NormalizedRequest) (*common.NormalizedResponse, error) {
		return sweepFn(c, r, false)
	}

	resp, execErr := failsafeExecutor.Run(ectx, req, tryOneUpstream, runUpstreamSweep)

	req.RLockWithTrace(ctx)
	defer req.RUnlock()

	if execErr != nil {
		// When the lifecycle ctx fires, the network executor may return plain
		// context.DeadlineExceeded with no sentinel in the Unwrap chain.
		// Substitute only when the ctx cause is OUR sentinel.
		if _, ok := execErr.(common.StandardError); !ok && errors.Is(execErr, context.DeadlineExceeded) {
			if cause := context.Cause(ectx); errors.Is(cause, common.ErrDynamicTimeoutExceeded) {
				execErr = cause
			}
		}
		// Timeout-attribution metric: emit only when this scope's policy
		// fired the timeout and the error is not retry-exhausted.
		if failsafeExecutor.HasTimeout() &&
			!common.HasErrorCode(execErr, common.ErrCodeFailsafeRetryExceeded) &&
			errors.Is(execErr, common.ErrDynamicTimeoutExceeded) &&
			!common.HasErrorCode(execErr, common.ErrCodeFailsafeTimeoutExceeded) {
			finality := req.Finality(ctx)
			telemetry.MetricNetworkTimeoutFiredTotal.WithLabelValues(
				n.projectId,
				req.NetworkLabel(),
				method,
				finality.String(),
				string(common.ScopeNetwork),
			).Inc()
		}
		// Wrap bare timeout sentinel as a typed error for downstream callers.
		translatedErr := execErr
		if _, ok := translatedErr.(common.StandardError); !ok && errors.Is(translatedErr, common.ErrDynamicTimeoutExceeded) {
			translatedErr = common.NewErrFailsafeTimeoutExceeded(common.ScopeNetwork, translatedErr, &startTime)
		}
		// Don't override consensus results with last valid response from individual upstreams
		// For example if 1 upstream gives empty response another 3 give "reverted" error,
		// we should still return reverted error, even though there was an empty response before.
		if failsafeExecutor.HasConsensus() {
			n.storeDeterministicNegativeCache(ctx, req, method, translatedErr)
			if mlx != nil {
				mlx.Close(ctx, nil, translatedErr)
			}
			// LVR is a borrowed pointer — consensus executor owns releasing all participant
			// responses (winners and losers). Just drop our reference to avoid dangling pointer.
			req.ClearLastValidResponse()
			return nil, translatedErr
		}

		lvr := req.LastValidResponse()
		if lvr != nil && !lvr.IsObjectNull() {
			// A valid response is a json-rpc response without "error" object.
			// This mechanism is needed in these two scenarios:
			//
			// 1) If error is due to empty response be generous and accept it,
			// because this means after many retries or exhausting all upstreams still no data is available.
			// We don't need to worry about wrongly replying empty responses for unfinalized data
			// because cache layer has mechanism to deal with empty and/or unfinalized data.
			//
			// 2) For pending txs we can accept the response, if after retries it is still pending.
			// This avoids failing with "retry" error, when we actually do have a response but blockNumber is null since tx is pending.
			resp = lvr
			req.SetLastUpstream(resp.Upstream())
		} else {
			n.storeDeterministicNegativeCache(ctx, req, method, translatedErr)
			if mlx != nil {
				mlx.Close(ctx, nil, translatedErr)
			}
			return nil, translatedErr
		}
	}

	if resp != nil {
		if n.cacheDal != nil {
			sem := n.getCacheWriteSem()
			select {
			case sem <- struct{}{}:
				n.observeCacheWriteQueueDepth(sem)
				// Force-materialize jrr so the goroutine reads only via atomic pointer (no locks needed).
				// TODO For other architectures we might need a different approach
				_, _ = resp.JsonRpcResponse(ctx)
				resp.AddRef()

				go (func(resp *common.NormalizedResponse, forwardSpan trace.Span) {
					defer func() {
						<-sem
						n.observeCacheWriteQueueDepth(sem)
					}()
					defer (func() {
						if rec := recover(); rec != nil {
							telemetry.MetricUnexpectedPanicTotal.WithLabelValues(
								"cache-set",
								fmt.Sprintf("network:%s method:%s", n.networkId, method),
								common.ErrorFingerprint(rec),
							).Inc()
							lg.Error().
								Interface("panic", rec).
								Str("stack", string(debug.Stack())).
								Msgf("unexpected panic on cache-set")
						}
					})()
					defer resp.DoneRef()

					timeoutCtx, timeoutCtxCancel := context.WithTimeoutCause(n.appCtx, 10*time.Second, errors.New("cache driver timeout during set"))
					defer timeoutCtxCancel()
					tracedCtx := trace.ContextWithSpanContext(timeoutCtx, forwardSpan.SpanContext())
					err := n.cacheDal.Set(tracedCtx, req, resp)
					if err != nil {
						lg.Warn().Err(err).Msgf("could not store response in cache")
					}
				})(resp, forwardSpan)
			default:
				n.observeCacheWriteQueueDepth(sem)
				telemetry.MetricNetworkCacheWriteDroppedTotal.WithLabelValues(
					n.projectId,
					n.Label(),
					method,
				).Inc()
				lg.Warn().Msg("skipping cache-set due to backpressure")
			}
		}

		// Per-request execution counters + full upstream-attempt trace.
		// req.ExecState().Apply emits the standard execution.* attrs AND the
		// upstreams.* slices (tried, outcomes, reasons, durations, won) so
		// traces answer "who, what, why" without enumerating child spans.
		req.ExecState().Apply(forwardSpan)

		n.recordHedgeRaceOutcome(ctx, req, resp, method)
	}

	isEmpty := resp == nil || resp.IsObjectNull(ctx) || resp.IsResultEmptyish(ctx)
	forwardSpan.SetAttributes(attribute.Bool("response.emptyish", isEmpty))
	if isEmpty {
		lg.Trace().Msgf("response is empty")
	}

	if execErr == nil && !isEmpty {
		n.enrichStatePoller(ctx, method, req, resp)

		// Extract block number from successful response for block availability bounds check below.
		var respBlockNumber int64
		if n.cfg.Architecture == common.ArchitectureEvm {
			if _, bn, err := evm.ExtractBlockReferenceFromResponse(ctx, resp); err == nil && bn > 0 {
				respBlockNumber = bn
			}
		}

		// If response is not empty, but at least one upstream responded empty we track in a metric.
		// Derived from ErrorsByUpstream entries with ErrEndpointMissingData code.
		req.ErrorsByUpstream.Range(func(key, value any) bool {
			upstreamErr, ok := value.(error)
			if !ok || !common.HasErrorCode(upstreamErr, common.ErrCodeEndpointMissingData) {
				return true
			}
			upstream := key.(*upstream.Upstream)
			finality := req.Finality(ctx)
			telemetry.MetricUpstreamWrongEmptyResponseTotal.WithLabelValues(
				n.projectId,
				upstream.VendorName(),
				n.Label(),
				upstream.Id(),
				method,
				finality.String(),
				req.UserId(),
				req.AgentName(),
			).Inc()

			// If the response block number is known, check if it falls outside
			// this upstream's configured block availability range.
			// If so, the empty response was expected — skip misbehavior recording.
			if respBlockNumber > 0 {
				minBound, maxBound := upstream.EvmBlockAvailabilityBounds()
				if (minBound != math.MinInt64 && respBlockNumber < minBound) ||
					(maxBound != math.MaxInt64 && respBlockNumber > maxBound) {
					return true
				}
			}

			// Wrong-empty is a misbehavior (data disagreement with other upstreams),
			// not an error. The upstream responded correctly, it just lacked data
			// that others had. Only record misbehavior, not failure. `finality` was
			// resolved a few lines up for the wrong-empty telemetry metric — reuse it
			// here so per-(method, finality) misbehavior counters stratify
			// consistently with the rest of the tracker writes.
			if upstream != nil {
				if mt := upstream.MetricsTracker(); mt != nil {
					mt.RecordUpstreamMisbehavior(upstream, method, finality)
				}
			}
			return true
		})
	}

	if resp != nil && !isEmpty && mlx != nil && atomic.LoadInt32(&mlx.followerCount) > 0 {
		n.storePostCompletionCoalescedResult(ctx, req, method, resp)
	}

	if mlx != nil {
		mlx.Close(ctx, resp, nil)
	}

	// LVR is a borrowed pointer — consensus executor owns releasing non-winner responses.
	// Just drop our reference so it doesn't outlive the response lifecycle.
	if failsafeExecutor.HasConsensus() {
		req.ClearLastValidResponse()
	}

	return resp, nil
}

func requestObservedHedgeCancellation(req *common.NormalizedRequest) bool {
	if req == nil {
		return false
	}
	observed := false
	req.ErrorsByUpstream.Range(func(_, value any) bool {
		err, ok := value.(error)
		if !ok {
			return true
		}
		if common.HasErrorCode(err, common.ErrCodeEndpointRequestCanceled) {
			observed = true
			return false
		}
		return true
	})
	return observed
}

func (n *Network) prepareRequest(ctx context.Context, nr *common.NormalizedRequest) error {
	switch n.Architecture() {
	case common.ArchitectureEvm:
		jsonRpcReq, err := nr.JsonRpcRequest(ctx)
		if err != nil {
			return common.NewErrJsonRpcExceptionInternal(
				0,
				common.JsonRpcErrorParseException,
				"failed to unmarshal json-rpc request",
				err,
				nil,
			)
		}
		evm.NormalizeHttpJsonRpc(ctx, nr, jsonRpcReq)
	default:
		return common.NewErrJsonRpcExceptionInternal(
			0,
			common.JsonRpcErrorServerSideException,
			fmt.Sprintf("unsupported architecture: %s for network: %s", n.Architecture(), n.Id()),
			nil,
			nil,
		)
	}

	return nil
}

func (n *Network) GetMethodMetrics(method string) common.TrackedMetrics {
	if method == "" {
		return nil
	}

	mt := n.metricsTracker.GetNetworkMethodMetrics(n.networkId, method)
	return mt
}

func (n *Network) Config() *common.NetworkConfig {
	return n.cfg
}

func (n *Network) Cache() common.CacheDAL {
	return n.cacheDal
}

func (n *Network) AppCtx() context.Context {
	return n.appCtx
}

func (n *Network) GetFinality(ctx context.Context, req *common.NormalizedRequest, resp *common.NormalizedResponse) common.DataFinalityState {
	ctx, span := common.StartDetailSpan(ctx, "Network.GetFinality")
	defer span.End()

	finality := common.DataFinalityStateUnknown

	if req == nil && resp == nil {
		return finality
	}

	method, _ := req.Method()
	if n.cfg.Methods != nil && n.cfg.Methods.Definitions != nil {
		if cfg, ok := n.cfg.Methods.Definitions[method]; ok {
			if cfg.Finalized {
				finality = common.DataFinalityStateFinalized
				return finality
			} else if cfg.Realtime {
				finality = common.DataFinalityStateRealtime
				return finality
			}
		}
	}

	blockRef, blockNumber, _ := evm.ExtractBlockReferenceFromRequest(ctx, req)

	// When the request alone doesn't carry a block number (e.g. tx-hash lookups
	// like eth_getTransactionReceipt, or block-hash lookups like eth_getBlockByHash),
	// try to extract it from the response body. This is critical for cache hits where
	// the response contains blockNumber but the request only has a hash.
	if blockNumber == 0 && resp != nil {
		if _, respBlockNumber, err := evm.ExtractBlockReferenceFromResponse(ctx, resp); err == nil && respBlockNumber > 0 {
			blockNumber = respBlockNumber
		}
	}

	// If we still have no block number and the request is a wildcard/hash-only ref
	// with no way to determine finality, return unknown early.
	if blockNumber == 0 && (blockRef == "" || blockRef == "*") {
		return finality // unknown
	}

	if blockRef != "" && blockRef != "*" && (blockRef[0] < '0' || blockRef[0] > '9') {
		finality = common.DataFinalityStateRealtime
		return finality
	}

	if blockNumber > 0 {
		// Try response's upstream first (direct upstream responses)
		if resp != nil {
			upstream := resp.Upstream()
			if upstream != nil {
				if ups, ok := upstream.(common.EvmUpstream); ok {
					if isFinalized, err := ups.EvmIsBlockFinalized(ctx, blockNumber, false); err == nil {
						if isFinalized {
							finality = common.DataFinalityStateFinalized
						} else {
							finality = common.DataFinalityStateUnfinalized
						}
						return finality
					}
				}
			}
		}

		// Try LastUpstream from the request (retries or subsequent attempts)
		if upstream := req.LastUpstream(); upstream != nil {
			if ups, ok := upstream.(common.EvmUpstream); ok {
				if isFinalized, err := ups.EvmIsBlockFinalized(ctx, blockNumber, false); err == nil {
					if isFinalized {
						finality = common.DataFinalityStateFinalized
					} else {
						finality = common.DataFinalityStateUnfinalized
					}
					return finality
				}
			}
		}

		// Fallback: use the network's lowest finalized block as a heuristic.
		// This is the primary path for cache hits where no upstream is available.
		if n.upstreamsRegistry != nil {
			lowestFinalized := n.EvmLowestFinalizedBlockNumber(ctx)
			if lowestFinalized > 0 {
				if blockNumber <= lowestFinalized {
					finality = common.DataFinalityStateFinalized
				} else {
					finality = common.DataFinalityStateUnfinalized
				}
				span.SetAttributes(
					attribute.Bool("used_network_finalized_heuristic", true),
					attribute.Int64("lowest_finalized", lowestFinalized),
				)
			}
		}
		// If we still can't determine, it remains unknown
	}

	return finality
}

func (n *Network) doForward(execSpanCtx context.Context, u common.Upstream, req *common.NormalizedRequest, skipCacheRead, isHedgeAttempt bool) (*common.NormalizedResponse, error) {
	switch n.cfg.Architecture {
	case common.ArchitectureEvm:
		if handled, resp, err := evm.HandleUpstreamPreForward(execSpanCtx, n, u, req, skipCacheRead); handled {
			return evm.HandleUpstreamPostForward(execSpanCtx, n, u, req, resp, err, skipCacheRead)
		}
	}

	// If not handled, then fallback to the normal forward
	resp, err := u.Forward(execSpanCtx, req, false, isHedgeAttempt)
	return evm.HandleUpstreamPostForward(execSpanCtx, n, u, req, resp, err, skipCacheRead)
}

// upstreamHasBlockAvailabilityBounds reports whether the upstream has any
// BlockAvailability bounds (lower or upper) configured. Presence of explicit
// bounds is treated as the user's intent to enforce them when no higher-priority
// explicit override has been set.
func upstreamHasBlockAvailabilityBounds(u common.Upstream) bool {
	if u == nil {
		return false
	}
	cfg := u.Config()
	if cfg == nil || cfg.Evm == nil || cfg.Evm.BlockAvailability == nil {
		return false
	}
	ba := cfg.Evm.BlockAvailability
	return ba.Lower != nil || ba.Upper != nil
}

// systemDefaultEnforceBlockAvailability returns the global system default for
// EnforceBlockAvailability for a given method, or nil if no default is set.
func systemDefaultEnforceBlockAvailability(method string) *bool {
	if common.DefaultWithBlockCacheMethods == nil {
		return nil
	}
	if dmc, ok := common.DefaultWithBlockCacheMethods[method]; ok && dmc != nil {
		return dmc.EnforceBlockAvailability
	}
	return nil
}

// resolveEnforceBlockAvailability resolves the effective enforcement flag for block availability.
// Precedence (highest to lowest):
//  1. Explicit method-level user override that differs from the system default
//     (system defaults get merged into n.cfg.Methods.Definitions during config
//     loading, so a method-level value that matches the system default is
//     treated as not-an-override).
//  2. Explicit network-level user override (network.cfg.Evm.EnforceBlockAvailability).
//  3. Per-upstream BlockAvailability bounds — configured bounds are themselves
//     an opt-in signal that overrides the method common default.
//  4. System default for this method (DefaultWithBlockCacheMethods).
//  5. Fallback: enabled.
func (n *Network) resolveEnforceBlockAvailability(method string, u common.Upstream) bool {
	sysDefault := systemDefaultEnforceBlockAvailability(method)

	// 1. Method-level user override (only when it differs from the system default).
	//    The defaults loader merges DefaultWithBlockCacheMethods into Methods.Definitions,
	//    so we cannot tell apart a user's explicit value from a merged-in default by
	//    presence alone. Comparing values is the cleanest way to distinguish — and is
	//    semantically harmless because a user explicitly setting the same value as the
	//    default has the same intent as not setting it.
	if n.cfg != nil && n.cfg.Methods != nil && n.cfg.Methods.Definitions != nil {
		if mc, ok := n.cfg.Methods.Definitions[method]; ok && mc != nil && mc.EnforceBlockAvailability != nil {
			if sysDefault == nil || *mc.EnforceBlockAvailability != *sysDefault {
				return *mc.EnforceBlockAvailability
			}
			// Matches the system default — treat as not-an-override and fall through.
		}
	}
	// 2. Explicit network-level user override
	if n.cfg != nil && n.cfg.Evm != nil && n.cfg.Evm.EnforceBlockAvailability != nil {
		return *n.cfg.Evm.EnforceBlockAvailability
	}
	// 3. Configured per-upstream bounds opt-in to enforcement, regardless of
	//    method common defaults. This ensures that users who configure
	//    BlockAvailability on an upstream actually get their bounds enforced
	//    even for methods whose system default has it off (e.g. eth_getBlockByNumber).
	if upstreamHasBlockAvailabilityBounds(u) {
		return true
	}
	// 4. System default for this method
	if sysDefault != nil {
		return *sysDefault
	}
	// 5. Fallback: enabled
	return true
}

// handleBlockSkip records telemetry when a block availability check causes an upstream to be skipped,
// and triggers an async state poller refresh for retryable cases so subsequent retries see fresh block numbers.
func (n *Network) handleBlockSkip(
	ctx context.Context,
	span trace.Span,
	ulg *zerolog.Logger,
	u common.Upstream,
	req *common.NormalizedRequest,
	method string,
	skipErr error,
	isRetryable bool,
) {
	ulg.Debug().Err(skipErr).Bool("retryable", isRetryable).Msg("skipping upstream due to block availability gating")
	span.SetAttributes(
		attribute.Bool("skipped", true),
		attribute.String("skip_reason", skipErr.Error()),
		attribute.Bool("skip_retryable", isRetryable),
	)
	finality := req.Finality(ctx)
	telemetry.MetricUpstreamErrorTotal.WithLabelValues(
		n.projectId, u.VendorName(), n.Label(), u.Id(), method,
		common.ErrorFingerprint(skipErr), string(common.SeverityInfo),
		req.CompositeType(), finality.String(),
		req.UserId(),
	).Inc()
	errToStore := skipErr
	if !isRetryable {
		errToStore = common.NewErrUpstreamRequestSkipped(skipErr, u.Id())
	}
	req.MarkUpstreamCompleted(ctx, u, nil, errToStore)

	// When the block is slightly ahead (retryable), trigger an async poll so the
	// state poller fetches the latest block number before the next retry fires.
	// Without this the retry loop sleeps for blockUnavailableDelay but the cached
	// latestBlock stays stale until the background ticker fires (often 10s+).
	// PollLatestBlockNumber respects its own debounce interval so concurrent
	// triggers from multiple upstreams/requests are coalesced safely.
	if isRetryable {
		if eu, ok := u.(common.EvmUpstream); ok {
			if sp := eu.EvmStatePoller(); sp != nil && !sp.IsObjectNull() {
				go func() {
					pollCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, _ = sp.PollLatestBlockNumber(pollCtx)
				}()
			}
		}
	}
}

func (n *Network) recordHedgeRaceOutcome(
	ctx context.Context,
	req *common.NormalizedRequest,
	resp *common.NormalizedResponse,
	method string,
) {
	if req == nil || resp == nil {
		return
	}

	hedgeWon := resp.Hedges() > 0 && resp.WinningHedge()
	hedgeObserved := resp.Hedges() > 0 || requestObservedHedgeCancellation(req)
	if !hedgeObserved {
		return
	}

	finality := req.Finality(ctx)
	upstreamID := "unknown"
	if ups := resp.Upstream(); ups != nil {
		upstreamID = ups.Id()
	}

	labels := []string{
		n.projectId,
		n.Label(),
		upstreamID,
		method,
		finality.String(),
		req.UserId(),
		req.AgentName(),
	}

	if hedgeWon {
		telemetry.CounterHandle(telemetry.MetricNetworkHedgeWonTotal, labels...).Inc()
		return
	}
	telemetry.CounterHandle(telemetry.MetricNetworkHedgeLostTotal, labels...).Inc()
}

// recordHedgeDiscard handles telemetry when a hedged request is discarded because another hedge won.
func (n *Network) recordHedgeDiscard(
	ctx context.Context,
	span trace.Span,
	ulg *zerolog.Logger,
	u common.Upstream,
	req *common.NormalizedRequest,
	method string,
	err error,
	attempts int,
	hedges int,
) {
	ulg.Debug().Err(err).Msgf("discarding hedged request to upstream")
	finality := req.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkHedgeDiscardsTotal,
		n.projectId, n.Label(), u.Id(), method, fmt.Sprintf("%d", attempts),
		fmt.Sprintf("%d", hedges), finality.String(),
		req.UserId(), req.AgentName(),
	).Inc()
	telemetry.CounterHandle(telemetry.MetricNetworkHedgeDiscardTotal,
		n.projectId, n.Label(), u.Id(), method, finality.String(),
		req.UserId(), req.AgentName(),
	).Inc()
	common.SetTraceSpanError(span, common.NewErrUpstreamHedgeCancelled(u.Id(), err))
}

// checkUpstreamBlockAvailability performs per-upstream gating for the request based on block availability.
// It is invoked just before forwarding to an upstream to avoid copying/filtering the list.
// Returns (nil, false) if upstream has the block available.
// Returns (error, isRetryable) if block is not available:
//   - isRetryable=true: block is just slightly ahead (within MaxRetryableBlockDistance), upstream may catch up
//   - isRetryable=false: block is too far ahead or below lower bound, not worth retrying this upstream
//
// This is the single point of block-availability enforcement. It runs whenever
// EnforceBlockAvailability resolves to true OR the upstream has explicit
// BlockAvailability bounds configured (the user's signal that they want bounds
// enforced regardless of per-method defaults).
//
// FAIL-OPEN BEHAVIOR: If we cannot determine block availability (e.g., state poller issues),
// we allow the request to proceed rather than blocking traffic.
func (n *Network) checkUpstreamBlockAvailability(ctx context.Context, u common.Upstream, req *common.NormalizedRequest, method string) (error, bool) {
	if n.cfg.Architecture != common.ArchitectureEvm {
		return nil, false
	}
	if !n.resolveEnforceBlockAvailability(method, u) {
		return nil, false
	}
	// Prefer the cached block number from normalization. Fall back to extracting
	// from the request (defensive: handles paths that bypass json_rpc.go's
	// normalization, and methods whose params haven't been pre-cached yet).
	var bn int64
	if v := req.EvmBlockNumber(); v != nil {
		if n64, ok := v.(int64); ok {
			bn = n64
		}
	}
	if bn <= 0 {
		if _, x, ebn := evm.ExtractBlockReferenceFromRequest(ctx, req); ebn == nil && x > 0 {
			bn = x
		}
	}
	if bn <= 0 {
		// If still unknown, skip gating (fail-open)
		return nil, false
	}
	eu, ok := u.(common.EvmUpstream)
	if !ok {
		return nil, false
	}
	available, err := eu.EvmAssertBlockAvailability(ctx, method, common.AvailbilityConfidenceBlockHead, true, bn)
	if err != nil {
		// FAIL-OPEN: Error during availability check - allow the request to proceed
		// This prevents blocking traffic due to state poller or detection issues
		n.logger.Debug().
			Err(err).
			Str("upstreamId", u.Id()).
			Int64("blockNumber", bn).
			Str("method", method).
			Msg("block availability check failed; failing open to allow request")
		return nil, false
	}
	if !available {
		// Get poller state for detailed error
		var latestBlock, finalizedBlock int64
		if sp := eu.EvmStatePoller(); sp != nil && !sp.IsObjectNull() {
			latestBlock = sp.LatestBlock()
			finalizedBlock = sp.FinalizedBlock()
		}

		blockErr := common.NewErrUpstreamBlockUnavailable(u.Id(), bn, latestBlock, finalizedBlock)

		// Determine if this is retryable based on distance
		// Upper bound issue (block ahead of latest): retryable if within max distance
		// Lower bound issue (block too old): not retryable
		if bn > latestBlock && latestBlock > 0 {
			distance := bn - latestBlock
			maxDistance := int64(128) // default
			if n.cfg.Evm != nil && n.cfg.Evm.MaxRetryableBlockDistance != nil {
				maxDistance = *n.cfg.Evm.MaxRetryableBlockDistance
			}
			isRetryable := distance <= maxDistance
			return blockErr, isRetryable
		}

		// Lower bound issue or unknown state - not retryable
		return blockErr, false
	}
	return nil, false
}

func (n *Network) handleMultiplexing(ctx context.Context, lg *zerolog.Logger, req *common.NormalizedRequest, startTime time.Time) (*Multiplexer, *common.NormalizedResponse, error) {
	method, _ := req.Method()
	if req.SkipMultiplex() {
		lg.Trace().Msg("multiplexing skipped: skipMultiplex flag set")
		return nil, nil, nil
	}
	if !n.isMultiplexingEnabledForMethod(method) {
		return nil, nil, nil
	}

	mlxHash, err := req.CacheHash()
	lg.Trace().Str("hash", mlxHash).Object("request", req).Msgf("checking if multiplexing is possible")
	if err != nil || mlxHash == "" {
		lg.Debug().Str("hash", mlxHash).Err(err).Object("request", req).Msgf("could not get multiplexing hash for request")
		return nil, nil, nil
	}

	if resp, hit := n.loadPostCompletionCoalescedResult(ctx, req, method, mlxHash); hit {
		lg.Trace().Str("hash", mlxHash).Object("response", resp).Msgf("served from post-completion coalescing window")
		return nil, resp, nil
	}

	// Try to atomically register as the leader or become a follower.
	// Loop handles the edge case where we find a closed multiplexer during cleanup.
	for {
		mlx := NewMultiplexer(mlxHash)
		vinf, loaded := n.inFlightRequests.LoadOrStore(mlxHash, mlx)

		if !loaded {
			// Our multiplexer was stored, we're the leader
			return mlx, nil, nil
		}

		// Another request is already in flight - try to be a follower
		inf := vinf.(*Multiplexer)

		// Try to register as follower under lock to prevent race with cleanup's Wait()
		inf.mu.Lock()
		if inf.closed {
			// Leader already finished and cleanup started, can't be a follower.
			// Retry LoadOrStore - the old entry should be deleted soon, allowing
			// us to properly become the new leader (with our mlx stored in the map).
			inf.mu.Unlock()
			continue
		}

		inf.copyWg.Add(1)
		atomic.AddInt32(&inf.followerCount, 1)
		inf.mu.Unlock()

		finality := req.Finality(ctx)
		telemetry.CounterHandle(telemetry.MetricNetworkMultiplexedRequests,
			n.projectId, n.Label(), method, finality.String(), req.UserId(), req.AgentName(),
		).Inc()

		lg.Debug().Str("hash", mlxHash).Msgf("found identical request initiating multiplexer")

		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			span.SetAttributes(attribute.String("multiplexer.hash", mlxHash))
		}

		resp, err := n.waitForMultiplexResult(ctx, inf, req, startTime)

		// Done() is called in waitForMultiplexResult via defer
		lg.Trace().Str("hash", mlxHash).Object("response", resp).Err(err).Msgf("multiplexed request result")

		if err != nil {
			return nil, nil, err
		}
		if resp != nil {
			return nil, resp, nil
		}

		// Shouldn't reach here normally, but if we do, retry
		lg.Warn().Str("hash", mlxHash).Msg("multiplexer follower got nil response and no error, retrying")
	}
}

func (n *Network) waitForMultiplexResult(ctx context.Context, mlx *Multiplexer, req *common.NormalizedRequest, startTime time.Time) (*common.NormalizedResponse, error) {
	ctx, span := common.StartSpan(ctx, "Network.WaitForMultiplexResult")
	defer span.End()
	span.SetAttributes(attribute.String("multiplexer.hash", mlx.hash))

	// Caller already registered with copyWg.Add(1), ensure we signal completion
	defer mlx.copyWg.Done()

	// Wait for leader to complete (or check if already done)
	select {
	case <-mlx.done:
		// Leader finished - copy the response
		// Lock protects against concurrent reads, cleanup waits for copyWg so response is valid
		mlx.mu.RLock()
		out, err := common.CopyResponseForRequest(ctx, mlx.resp, req)
		resultErr := mlx.err
		mlx.mu.RUnlock()

		if err != nil {
			return nil, err
		}
		return out, resultErr
	case <-ctx.Done():
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, common.NewErrNetworkRequestTimeout(time.Since(startTime), err)
		}
		return nil, err
	}
}

func (n *Network) loadPostCompletionCoalescedResult(ctx context.Context, req *common.NormalizedRequest, method, hash string) (*common.NormalizedResponse, bool) {
	if n == nil || n.postCompletionResults == nil || hash == "" {
		return nil, false
	}
	if !n.shouldUsePostCompletionCoalescing(ctx, req, method) {
		return nil, false
	}

	raw, ok := n.postCompletionResults.Load(hash)
	if !ok {
		return nil, false
	}
	entry, ok := raw.(*postCompletionResultEntry)
	if !ok || entry == nil {
		n.postCompletionResults.Delete(hash)
		if entry != nil {
			entry.release()
		}
		return nil, false
	}
	if time.Now().UnixNano() > atomic.LoadInt64(&entry.expiresAt) {
		n.postCompletionResults.Delete(hash)
		entry.release()
		return nil, false
	}

	resp, ok := entry.acquireResponse()
	if !ok {
		n.postCompletionResults.Delete(hash)
		entry.release()
		return nil, false
	}
	defer resp.DoneRef()

	out, err := common.CopyResponseForRequest(ctx, resp, req)
	if err != nil {
		n.postCompletionResults.Delete(hash)
		entry.release()
		return nil, false
	}
	if out == nil {
		return nil, false
	}

	finality := req.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkMultiplexedRequests,
		n.projectId, n.Label(), method, finality.String(), req.UserId(), req.AgentName(),
	).Inc()
	telemetry.CounterHandle(telemetry.MetricNetworkBurstCoalescedRequests,
		n.projectId, n.Label(), method, finality.String(), req.UserId(), req.AgentName(),
	).Inc()

	return out, true
}

func (n *Network) storePostCompletionCoalescedResult(ctx context.Context, req *common.NormalizedRequest, method string, resp *common.NormalizedResponse) {
	if n == nil || n.postCompletionResults == nil || resp == nil {
		return
	}
	if !n.shouldUsePostCompletionCoalescing(ctx, req, method) {
		return
	}
	hash, err := req.CacheHash(ctx)
	if err != nil || hash == "" {
		return
	}

	stagedResp, copyErr := common.CopyResponseForRequest(ctx, resp, req)
	if copyErr != nil || stagedResp == nil {
		return
	}

	entry := &postCompletionResultEntry{
		resp:      stagedResp,
		expiresAt: time.Now().Add(networkPostCompletionCoalescingWindow).UnixNano(),
	}

	previous, loaded := n.postCompletionResults.Swap(hash, entry)
	if loaded {
		if oldEntry, ok := previous.(*postCompletionResultEntry); ok {
			oldEntry.release()
		}
	}

	time.AfterFunc(networkPostCompletionCoalescingWindow, func() {
		raw, ok := n.postCompletionResults.Load(hash)
		if ok && raw == entry {
			n.postCompletionResults.Delete(hash)
			entry.release()
		}
	})
}

func (n *Network) shouldUsePostCompletionCoalescing(ctx context.Context, req *common.NormalizedRequest, method string) bool {
	if req == nil || method == "" {
		return false
	}
	if req.SkipCacheRead() {
		return false
	}
	if dr := req.Directives(); dr != nil && dr.UseUpstream != "" {
		return false
	}
	if !isReadMethodEligibleForNegativeCache(method) {
		return false
	}
	if isNonceSensitiveReadMethod(method) {
		return false
	}
	if hasPendingBlockReference(ctx, req) {
		return false
	}
	return true
}

func (n *Network) cleanupMultiplexer(mlx *Multiplexer) {
	// Mark as closed under lock to prevent new followers from registering
	mlx.mu.Lock()
	mlx.closed = true
	mlx.mu.Unlock()

	// Remove from map so new requests create their own multiplexer
	n.inFlightRequests.Delete(mlx.hash)

	// Wait for all followers to finish copying before releasing.
	// Wait() is safe because closed=true prevents any new Add() calls.
	// This blocks the leader briefly, but the leader has already returned
	// its response to the caller, so this only delays cleanup.
	mlx.copyWg.Wait()

	// After Wait(), no followers are accessing the response.
	mlx.mu.Lock()
	// Do NOT call Release() here: the leader returns mlx.resp to the caller, and the
	// caller will release it after writing the response. Releasing here would
	// clear the JsonRpcResponse and break the leader response path.
	mlx.resp = nil
	mlx.err = nil
	mlx.mu.Unlock()
}

func (n *Network) loadDeterministicNegativeCache(ctx context.Context, req *common.NormalizedRequest, method string) (error, bool) {
	if n == nil || n.negativeResultCache == nil {
		return nil, false
	}
	if !n.shouldUseDeterministicNegativeCache(ctx, req, method) {
		return nil, false
	}

	key, ok := n.deterministicNegativeCacheKey(ctx, req, method)
	if !ok {
		return nil, false
	}

	raw, ok := n.negativeResultCache.Load(key)
	if !ok {
		return nil, false
	}
	entry, ok := raw.(*deterministicNegativeCacheEntry)
	if !ok || entry == nil || entry.err == nil {
		n.negativeResultCache.Delete(key)
		return nil, false
	}
	if time.Now().UnixNano() > atomic.LoadInt64(&entry.expiresAt) {
		n.negativeResultCache.Delete(key)
		return nil, false
	}

	finality := req.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkDeterministicNegativeCacheHitTotal,
		n.projectId, n.Label(), method, finality.String(), req.UserId(), req.AgentName(),
	).Inc()

	return entry.err, true
}

func (n *Network) storeDeterministicNegativeCache(ctx context.Context, req *common.NormalizedRequest, method string, err error) {
	if n == nil || n.negativeResultCache == nil || err == nil {
		return
	}
	if !n.shouldUseDeterministicNegativeCache(ctx, req, method) {
		return
	}
	if !shouldStoreDeterministicNegativeError(err) {
		return
	}

	key, ok := n.deterministicNegativeCacheKey(ctx, req, method)
	if !ok {
		return
	}

	now := time.Now()
	entry := &deterministicNegativeCacheEntry{
		err:       err,
		expiresAt: now.Add(networkDeterministicNegativeCacheTTL).UnixNano(),
	}
	n.negativeResultCache.Store(key, entry)

	finality := req.Finality(ctx)
	telemetry.CounterHandle(telemetry.MetricNetworkDeterministicNegativeCacheSetTotal,
		n.projectId, n.Label(), method, finality.String(), req.UserId(), req.AgentName(),
	).Inc()

	time.AfterFunc(networkDeterministicNegativeCacheTTL, func() {
		raw, ok := n.negativeResultCache.Load(key)
		if ok && raw == entry {
			n.negativeResultCache.Delete(key)
		}
	})
}

func (n *Network) deterministicNegativeCacheKey(ctx context.Context, req *common.NormalizedRequest, method string) (string, bool) {
	hash, err := req.CacheHash(ctx)
	if err != nil || hash == "" {
		return "", false
	}
	finality := req.Finality(ctx)
	return fmt.Sprintf("%s|%s|%s", method, finality.String(), hash), true
}

func (n *Network) shouldUseDeterministicNegativeCache(ctx context.Context, req *common.NormalizedRequest, method string) bool {
	if req == nil || method == "" {
		return false
	}
	if req.SkipCacheRead() {
		return false
	}
	if dr := req.Directives(); dr != nil && dr.UseUpstream != "" {
		return false
	}
	if !isReadMethodEligibleForNegativeCache(method) {
		return false
	}
	if n != nil && n.cfg != nil && n.cfg.Methods != nil && n.cfg.Methods.Definitions != nil {
		if mc, ok := n.cfg.Methods.Definitions[method]; ok && mc != nil && mc.Stateful {
			return false
		}
	}
	if isNonceSensitiveReadMethod(method) {
		return false
	}
	if hasPendingBlockReference(ctx, req) {
		return false
	}

	finality := req.Finality(ctx)
	return finality == common.DataFinalityStateFinalized
}

func hasPendingBlockReference(ctx context.Context, req *common.NormalizedRequest) bool {
	if req == nil {
		return false
	}
	if br := req.EvmBlockRef(); br != nil {
		if s, ok := br.(string); ok && strings.EqualFold(s, "pending") {
			return true
		}
	}
	ref, _, err := evm.ExtractBlockReferenceFromRequest(ctx, req)
	return err == nil && strings.EqualFold(ref, "pending")
}

func shouldStoreDeterministicNegativeError(err error) bool {
	if err == nil {
		return false
	}
	if common.IsRetryableTowardNetwork(err) {
		return false
	}
	return common.IsClientError(err) ||
		common.HasErrorCode(err, common.ErrCodeEndpointExecutionException, common.ErrCodeEndpointUnsupported)
}

func isReadMethodEligibleForNegativeCache(method string) bool {
	if method == "" {
		return false
	}
	if evm.IsNonRetryableWriteMethod(method) || method == "eth_sendRawTransaction" {
		return false
	}
	if strings.HasPrefix(method, "eth_send") ||
		strings.HasPrefix(method, "engine_") ||
		strings.HasPrefix(method, "personal_") ||
		strings.HasPrefix(method, "wallet_") {
		return false
	}
	return true
}

func (n *Network) isMultiplexingEnabledForMethod(method string) bool {
	if n == nil || n.cfg == nil {
		return true
	}
	if method != "" && n.cfg.Methods != nil && n.cfg.Methods.Definitions != nil {
		if mc, ok := n.cfg.Methods.Definitions[method]; ok && mc != nil && mc.Multiplex != nil {
			return *mc.Multiplex
		}
	}
	return n.cfg.MultiplexingEnabled()
}

func isNonceSensitiveReadMethod(method string) bool {
	return method == "eth_getTransactionCount" || strings.HasPrefix(method, "txpool_")
}

func (n *Network) shouldHandleMethod(req *common.NormalizedRequest, method string, upsList []common.Upstream) error {
	// Check stateful methods policy
	// Methods.Definitions is guaranteed to be populated by SetDefaults() with all necessary stateful methods
	isStateful := false
	if n.cfg != nil && n.cfg.Methods != nil && n.cfg.Methods.Definitions != nil {
		if mc, ok := n.cfg.Methods.Definitions[method]; ok && mc != nil {
			isStateful = mc.Stateful
		}
	}
	if isStateful {
		// Determine targeted upstream count
		targeted := 0
		if dr := req.Directives(); dr != nil && dr.UseUpstream != "" {
			for _, u := range upsList {
				if match, _ := common.UpstreamMatchesSelector(dr.UseUpstream, u); match {
					targeted++
				}
			}
		} else {
			targeted = len(upsList)
		}
		if targeted > 1 {
			return common.NewErrNotImplemented("stateful method requires a single targeted upstream; either configure only 1 upstream or set Use-Upstream to a single upstream id")
		}
	}

	if method == "eth_accounts" || method == "eth_sign" {
		return common.NewErrNotImplemented("eth_accounts and eth_sign are not supported")
	}

	return nil
}

// filterMethodEligible returns the upstreams expected to serve `method` per
// their ignoreMethods/allowMethods config (Upstream.ShouldHandleMethod, which
// is memoized per method), plus the number dropped. The input slice is never
// mutated — selection-policy slots hand out their cached backing array as
// read-only — and is returned as-is when nothing is dropped. Matcher errors
// fail open (the upstream stays in) so a malformed pattern degrades to the
// forward-time skip rather than silently shrinking the pool.
func filterMethodEligible(ups []common.Upstream, method string) ([]common.Upstream, int) {
	eligible := ups
	dropped := 0
	for i, u := range ups {
		ok, err := u.ShouldHandleMethod(method)
		if err == nil && !ok {
			if dropped == 0 {
				// First drop: materialize a fresh slice with the survivors so far.
				eligible = make([]common.Upstream, i, len(ups))
				copy(eligible, ups[:i])
			}
			dropped++
			continue
		}
		if dropped > 0 {
			eligible = append(eligible, u)
		}
	}
	return eligible, dropped
}

func (n *Network) enrichStatePoller(ctx context.Context, method string, req *common.NormalizedRequest, resp *common.NormalizedResponse) {
	ctx, span := common.StartDetailSpan(ctx, "Network.EnrichStatePoller")
	defer span.End()

	switch n.Architecture() {
	case common.ArchitectureEvm:
		// TODO Move the logic to evm package as a post-forward hook?
		if method == "eth_getBlockByNumber" {
			// Prefer the original block reference preserved by normalization.
			// This stays "latest"/"finalized" even if params were interpolated to hex.
			var blkTag string
			if ref := req.EvmBlockRef(); ref != nil {
				if s, ok := ref.(string); ok && (s == "latest" || s == "finalized") {
					blkTag = s
				}
			}
			if lg := n.logger; lg != nil && lg.GetLevel() <= zerolog.TraceLevel {
				lg.Trace().
					Str("blkTagFromEvmBlockRef", blkTag).
					Interface("evmBlockRefRaw", req.EvmBlockRef()).
					Msg("enrichStatePoller: resolved block tag from request EvmBlockRef")
			}
			// Fallback to inspecting the request param only if we couldn't resolve from EvmBlockRef
			if blkTag == "" {
				jrq, err := req.JsonRpcRequest(ctx)
				if err != nil {
					return
				}
				jrq.RLock()
				if len(jrq.Params) > 0 {
					if s, ok := jrq.Params[0].(string); ok && (s == "latest" || s == "finalized") {
						blkTag = s
					}
				}
				jrq.RUnlock()
				if lg := n.logger; lg != nil {
					lg.Trace().
						Str("blkTagFromParams", blkTag).
						Msg("enrichStatePoller: resolved block tag from request params")
				}
			}
			if blkTag == "" {
				return
			}
			jrs, _ := resp.JsonRpcResponse(ctx)
			bnh, err := jrs.PeekStringByPath(ctx, "number")
			if err != nil {
				return
			}
			blockNumber, err := common.HexToInt64(bnh)
			if err != nil {
				return
			}
			if lg := n.logger; lg != nil {
				lg.Trace().
					Str("blkTag", blkTag).
					Int64("blockNumber", blockNumber).
					Str("method", method).
					Msg("enrichStatePoller: suggesting block number to state poller")
			}
			if ups := resp.Upstream(); ups != nil {
				if ups, ok := ups.(common.EvmUpstream); ok {
					// These methods are non-blocking and handle async updates internally
					switch blkTag {
					case "finalized":
						ups.EvmStatePoller().SuggestFinalizedBlock(blockNumber)
					case "latest":
						ups.EvmStatePoller().SuggestLatestBlock(blockNumber)
					}
				}
			}
		} else if method == "eth_blockNumber" {
			jrs, _ := resp.JsonRpcResponse(ctx)
			bnh, err := jrs.PeekStringByPath(ctx)
			if err == nil {
				blockNumber, err := common.HexToInt64(bnh)
				if err == nil {
					if ups := resp.Upstream(); ups != nil {
						if ups, ok := ups.(common.EvmUpstream); ok {
							// This method is non-blocking and handles async updates internally
							ups.EvmStatePoller().SuggestLatestBlock(blockNumber)
						}
					}
				}
			}
		}
	}
}

func (n *Network) normalizeResponse(ctx context.Context, req *common.NormalizedRequest, resp *common.NormalizedResponse) error {
	ctx, span := common.StartDetailSpan(ctx, "Network.NormalizeResponse")
	defer span.End()

	// For any JSON-RPC architecture: ensure the response ID always reflects the
	// client's original request ID, regardless of what the upstream echoed back.
	// This is especially important for proxies that normalize or multiplex IDs
	// toward upstreams, and must apply to every JSON-RPC architecture — not just
	// EVM — so non-EVM clients (Solana and future architectures) aren't left with
	// mismatched response IDs.
	if resp != nil {
		if jrr, err := resp.JsonRpcResponse(ctx); err == nil && jrr != nil {
			jrq, err := req.JsonRpcRequest(ctx)
			if err != nil {
				return err
			}
			// Prefer the verbatim request id bytes when available so that
			// large integers (>2^53), fractional ids, and other exotic
			// numeric formats round-trip without precision loss. Falls
			// back to the typed id for programmatically-constructed
			// requests where idRaw is unset.
			if rawID := jrq.IDRawBytes(); len(rawID) > 0 {
				if err := jrr.SetIDBytes(rawID); err != nil {
					return err
				}
			} else if err := jrr.SetID(jrq.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Network) acquireRateLimitPermit(ctx context.Context, req *common.NormalizedRequest) error {
	if n.cfg.RateLimitBudget == "" {
		return nil
	}

	rlb, errNetLimit := n.rateLimitersRegistry.GetBudget(n.cfg.RateLimitBudget)
	if errNetLimit != nil {
		return errNetLimit
	}
	if rlb == nil {
		return nil
	}

	method, errMethod := req.Method()
	if errMethod != nil {
		return errMethod
	}
	lg := n.logger.With().Str("method", method).Logger()

	rules, errRules := rlb.GetRulesByMethod(method)
	if errRules != nil {
		return errRules
	}
	lg.Debug().Msgf("found %d network-level rate limiters", len(rules))

	if len(rules) > 0 {
		allowed, err := rlb.TryAcquirePermit(ctx, n.projectId, req, method, "", "", "", "network")
		if err != nil {
			return err
		}
		if !allowed {
			// Blocked event already recorded in budget.TryAcquirePermit; avoid double recording here
			return common.NewErrNetworkRateLimitRuleExceeded(
				n.projectId,
				n.networkId,
				n.cfg.RateLimitBudget,
				fmt.Sprintf("method:%s", method),
			)
		}
	}

	return nil
}
