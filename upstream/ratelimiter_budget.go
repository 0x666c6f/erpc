package upstream

import (
	"context"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	pb_struct "github.com/envoyproxy/go-control-plane/envoy/extensions/common/ratelimit/v3"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/limiter"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type doLimitResult struct {
	statuses []*pb.RateLimitResponse_DescriptorStatus
	panicErr error
}

type rateLimitEvalContext struct {
	methodPattern string
	scope         string
	projectId     string
	networkLabel  string
	userLabel     string
	agentName     string
	method        string
	observe       func(string)
}

type RateLimiterBudget struct {
	logger     *zerolog.Logger
	Id         string
	Rules      []*RateLimitRule
	registry   *RateLimitersRegistry
	rulesMu    sync.RWMutex
	maxTimeout time.Duration

	// admission is a buffered semaphore that bounds the number of concurrent
	// in-flight remote (Redis) DoLimit calls per budget. It exists because the
	// underlying envoyproxy/ratelimit + radix client does not honor context
	// cancellation: a goroutine spawned in doLimitWithTimeout that has timed
	// out will continue to live until Redis finally answers (which can be
	// seconds when the connection pool is contended).
	//
	// Without this cap we observed 25k+ leaked goroutines per machine during
	// a Redis-rate-limiter contention spike (root-caused 2026-05-07 cronos
	// receipts incident), which then drove CPU/GC into a death spiral.
	//
	// When admission is full, doLimitWithTimeout fails closed immediately
	// without spawning a goroutine — increments
	// MetricRateLimiterRemoteAdmissionSheddedTotal so we can alert on it.
	//
	// Nil when no remote cache is in use (e.g. memory cache); only allocated
	// when maxTimeout > 0.
	admission chan struct{}

	// inflight gauge, kept here for fast hot-path access without a labels
	// lookup on every call. Refreshed at registration time.
	inflightGauge     prometheus.Gauge
	admissionShedded  prometheus.Counter
	durationFailopen  prometheus.Observer
	durationOK        prometheus.Observer
	durationOverlimit prometheus.Observer
}

type RateLimitRule struct {
	Config *common.RateLimitRuleConfig
}

func normalizeRateLimitMethodLabel(method string) string {
	if method == "" {
		return "*"
	}
	return method
}

func (b *RateLimiterBudget) GetRulesByMethod(method string) ([]*RateLimitRule, error) {
	b.rulesMu.RLock()
	defer b.rulesMu.RUnlock()

	rules := make([]*RateLimitRule, 0, len(b.Rules))
	for _, rule := range b.Rules {
		match, err := common.WildcardMatch(rule.Config.Method, method)
		if err != nil {
			return nil, err
		}
		if rule.Config.Method == method || match {
			rules = append(rules, rule)
		}
	}
	return rules, nil
}

// AdjustBudget updates the MaxCount for the provided rule and refreshes telemetry.
func (b *RateLimiterBudget) AdjustBudget(rule *RateLimitRule, newMaxCount uint32) error {
	if rule == nil || rule.Config == nil {
		return nil
	}
	b.rulesMu.Lock()
	defer b.rulesMu.Unlock()

	prev := rule.Config.MaxCount
	if prev == newMaxCount {
		return nil
	}
	b.logger.Warn().Str("method", rule.Config.Method).Msgf("adjusting rate limiter budget from: %d to: %d", prev, newMaxCount)
	rule.Config.MaxCount = newMaxCount
	telemetry.MetricRateLimiterBudgetMaxCount.WithLabelValues(
		b.Id,
		normalizeRateLimitMethodLabel(rule.Config.Method),
		rule.Config.ScopeString(),
	).Set(float64(newMaxCount))
	return nil
}

// AdjustBudgetByFactor reads currentMax, multiplies by factor, clamps to
// [minBudget, maxBudget], and writes the result -- all under rulesMu.
// Returns (prev, next, changed) so callers can log without holding the lock.
func (b *RateLimiterBudget) AdjustBudgetByFactor(rule *RateLimitRule, factor float64, minBudget, maxBudget int) (prev, next uint32, changed bool) {
	if rule == nil || rule.Config == nil {
		return 0, 0, false
	}
	b.rulesMu.Lock()
	defer b.rulesMu.Unlock()

	prev = rule.Config.MaxCount
	raw := math.Ceil(float64(prev) * factor)
	// Clamp to a valid uint32 range before the narrowing cast.  A large
	// increaseFactor can push the float64 product above math.MaxUint32,
	// which causes Go's float64→uint32 conversion to wrap to an arbitrary
	// small value -- the opposite of an increase.
	if raw > math.MaxUint32 {
		raw = math.MaxUint32
	} else if raw < 0 {
		raw = 0
	}
	next = uint32(raw)
	if minBudget > 0 {
		minClamped := uint32(max(0, minBudget))
		if next < minClamped {
			next = minClamped
		}
	}
	if maxBudget > 0 {
		maxClamped := uint32(max(0, maxBudget))
		if next > maxClamped {
			next = maxClamped
		}
	}

	if next == prev {
		return prev, next, false
	}

	rule.Config.MaxCount = next
	telemetry.MetricRateLimiterBudgetMaxCount.WithLabelValues(b.Id, rule.Config.Method, rule.Config.ScopeString()).Set(float64(next))
	return prev, next, true
}

// ruleResult holds the result of evaluating a single rule.
type ruleResult struct {
	rule    *RateLimitRule
	allowed bool
}

// getCache returns the current cache from the registry (thread-safe)
func (b *RateLimiterBudget) getCache() limiter.RateLimitCache {
	return b.registry.GetCache()
}

// TryAcquirePermit evaluates all matching rules for the given method using Envoy's DoLimit.
// Rules are evaluated in parallel for lower latency. Returns true if allowed, false if rate limited.
func (b *RateLimiterBudget) TryAcquirePermit(ctx context.Context, projectId string, req *common.NormalizedRequest, method string, vendor string, upstreamId string, authLabel string, origin string) (bool, error) {
	cache := b.getCache()
	if cache == nil {
		return true, nil // Fail-open when no cache is available
	}

	ctx, span := common.StartDetailSpan(ctx, "RateLimiter.TryAcquirePermit",
		trace.WithAttributes(
			attribute.String("budget", b.Id),
			attribute.String("method", method),
		),
	)
	defer span.End()

	rules, err := b.GetRulesByMethod(method)
	if err != nil {
		return false, err
	}
	if len(rules) == 0 {
		return true, nil
	}

	// Extract request metadata once
	var userLabel, agentName, networkLabel, finality, clientIP string
	if req != nil {
		userLabel = req.UserId()
		agentName = req.AgentName()
		networkLabel = req.NetworkId()
		finality = req.Finality(ctx).String()
		clientIP = req.ClientIP()
	}

	// Validate request context upfront
	for _, rule := range rules {
		if (rule.Config.PerIP || rule.Config.PerUser || rule.Config.PerNetwork) && req == nil {
			return false, fmt.Errorf("request cannot be nil when ratelimiter rule has perIP/perUser/perNetwork")
		}
	}

	// Single rule: evaluate directly without goroutine overhead
	if len(rules) == 1 {
		allowed := b.evaluateRule(ctx, projectId, rules[0], method, clientIP, userLabel, agentName, networkLabel)
		if !allowed {
			telemetry.CounterHandle(
				telemetry.MetricRateLimitsTotal,
				projectId, networkLabel, vendor, upstreamId, method, finality,
				userLabel, b.Id, rules[0].Config.ScopeString(), authLabel, origin,
			).Inc()
		}
		return allowed, nil
	}

	// Multiple rules: evaluate in parallel
	resultCh := make(chan ruleResult, len(rules))
	var blocked atomic.Bool

	for _, rule := range rules {
		go func(r *RateLimitRule) {
			// Skip if already blocked
			if blocked.Load() {
				resultCh <- ruleResult{rule: r, allowed: true}
				return
			}
			allowed := b.evaluateRule(ctx, projectId, r, method, clientIP, userLabel, agentName, networkLabel)
			if !allowed {
				blocked.Store(true)
			}
			resultCh <- ruleResult{rule: r, allowed: allowed}
		}(rule)
	}

	// Collect results
	var blockingRule *RateLimitRule
	for i := 0; i < len(rules); i++ {
		result := <-resultCh
		if !result.allowed && blockingRule == nil {
			blockingRule = result.rule
		}
	}

	if blockingRule != nil {
		telemetry.CounterHandle(
			telemetry.MetricRateLimitsTotal,
			projectId, networkLabel, vendor, upstreamId, method, finality,
			userLabel, b.Id, blockingRule.Config.ScopeString(), authLabel, origin,
		).Inc()
		return false, nil
	}
	return true, nil
}

// evaluateRule checks a single rate limit rule against the cache.
// Returns true if allowed, false if over limit.
func (b *RateLimiterBudget) evaluateRule(ctx context.Context, projectId string, rule *RateLimitRule, method, clientIP, userLabel, agentName, networkLabel string) bool {
	evalStartedAt := time.Now()
	scope := rule.Config.ScopeString()
	methodPattern := normalizeRateLimitMethodLabel(rule.Config.Method)
	observeEvaluation := func(outcome string) {
		telemetry.ObserverHandle(
			telemetry.MetricRateLimiterPermitEvaluationDuration,
			b.Id,
			methodPattern,
			scope,
			outcome,
		).Observe(time.Since(evalStartedAt).Seconds())
	}
	evalCtx := rateLimitEvalContext{
		methodPattern: methodPattern,
		scope:         scope,
		projectId:     projectId,
		networkLabel:  networkLabel,
		userLabel:     userLabel,
		agentName:     agentName,
		method:        method,
		observe:       observeEvaluation,
	}

	cache := b.getCache()
	if cache == nil {
		observeEvaluation("no_cache_fail_open")
		telemetry.IncNetworkAttemptReason(projectId, networkLabel, method, telemetry.AttemptReasonFailOpen)
		return true // Fail-open when no cache is available
	}

	// Build descriptor entries
	entries := []*pb_struct.RateLimitDescriptor_Entry{{Key: "method", Value: method}}
	if rule.Config.PerIP && clientIP != "" && clientIP != "n/a" {
		entries = append(entries, &pb_struct.RateLimitDescriptor_Entry{Key: "ip", Value: clientIP})
	}
	if rule.Config.PerUser && userLabel != "" && userLabel != "n/a" {
		entries = append(entries, &pb_struct.RateLimitDescriptor_Entry{Key: "user", Value: userLabel})
	}
	if rule.Config.PerNetwork && networkLabel != "" && networkLabel != "n/a" {
		entries = append(entries, &pb_struct.RateLimitDescriptor_Entry{Key: "network", Value: networkLabel})
	}

	rlReq := &pb.RateLimitRequest{
		Domain:      b.Id,
		Descriptors: []*pb_struct.RateLimitDescriptor{{Entries: entries}},
		HitsAddend:  1,
	}

	// Build stats key
	statsKey := b.Id + ".method_" + method + rule.statsKeySuffix()

	rlStats := b.registry.statsManager.NewStats(statsKey)
	limit := config.NewRateLimit(rule.Config.MaxCount, rule.Config.Period.Unit(), rlStats, false, false, "", nil, false)
	limits := []*config.RateLimit{limit}

	_, doSpan := common.StartSpan(ctx, "RateLimiter.DoLimit",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("budget", b.Id),
			attribute.String("method", method),
			attribute.String("scope", rule.Config.ScopeString()),
		),
	)

	var statuses []*pb.RateLimitResponse_DescriptorStatus
	var timedOut bool
	var panicErr error
	var admissionFull bool
	waitStartedAt := time.Now()
	if b.maxTimeout > 0 {
		statuses, timedOut, admissionFull, panicErr = b.doLimitWithTimeout(ctx, cache, rlReq, limits, method, userLabel, networkLabel)
	} else {
		statuses, panicErr = b.doLimitSafely(ctx, cache, rlReq, limits, method, userLabel, networkLabel)
	}
	waitDuration := time.Since(waitStartedAt)

	if admissionFull {
		return b.recordAdmissionFullDeny(
			doSpan,
			waitDuration,
			evalCtx,
		)
	}
	if timedOut {
		return b.recordFailOpen(
			doSpan,
			waitDuration,
			evalCtx,
			"timeout_fail_open",
			"limit_timeout",
			nil,
		)
	}
	if panicErr != nil {
		return b.recordFailOpen(
			doSpan,
			waitDuration,
			evalCtx,
			"panic_fail_open",
			"limit_panic",
			panicErr,
		)
	}

	outcome := "ok"
	isOverLimit := len(statuses) > 0 && statuses[0].Code == pb.RateLimitResponse_OVER_LIMIT
	if isOverLimit {
		outcome = "over_limit"
		doSpan.SetAttributes(attribute.String("result", "over_limit"))
	} else {
		doSpan.SetAttributes(attribute.String("result", "ok"))
	}
	telemetry.ObserverHandle(
		telemetry.MetricRateLimiterPermitWaitDuration,
		b.Id,
		methodPattern,
		scope,
		outcome,
	).Observe(waitDuration.Seconds())
	observeEvaluation(outcome)
	doSpan.End()

	return !isOverLimit
}

func (b *RateLimiterBudget) recordAdmissionFullDeny(
	doSpan trace.Span,
	waitDuration time.Duration,
	evalCtx rateLimitEvalContext,
) bool {
	b.finishRateLimitOutcome(doSpan, waitDuration, evalCtx, "admission_full_deny", nil)
	return false
}

func (b *RateLimiterBudget) recordFailOpen(
	doSpan trace.Span,
	waitDuration time.Duration,
	evalCtx rateLimitEvalContext,
	outcome, reason string,
	panicErr error,
) bool {
	b.finishRateLimitOutcome(doSpan, waitDuration, evalCtx, outcome, panicErr)
	telemetry.MetricRateLimiterFailopenTotal.WithLabelValues(
		evalCtx.projectId,
		evalCtx.networkLabel,
		evalCtx.userLabel,
		evalCtx.agentName,
		b.Id,
		evalCtx.method,
		reason,
	).Inc()
	telemetry.IncNetworkAttemptReason(evalCtx.projectId, evalCtx.networkLabel, evalCtx.method, telemetry.AttemptReasonFailOpen)
	return true
}

func (b *RateLimiterBudget) finishRateLimitOutcome(
	doSpan trace.Span,
	waitDuration time.Duration,
	evalCtx rateLimitEvalContext,
	outcome string,
	panicErr error,
) {
	telemetry.ObserverHandle(
		telemetry.MetricRateLimiterPermitWaitDuration,
		b.Id,
		evalCtx.methodPattern,
		evalCtx.scope,
		outcome,
	).Observe(waitDuration.Seconds())
	evalCtx.observe(outcome)
	if panicErr != nil {
		doSpan.RecordError(panicErr)
	}
	doSpan.SetAttributes(attribute.String("result", outcome))
	doSpan.End()
}

func (b *RateLimiterBudget) doLimitSafely(
	ctx context.Context,
	cache limiter.RateLimitCache,
	rlReq *pb.RateLimitRequest,
	limits []*config.RateLimit,
	method, userLabel, networkLabel string,
) (statuses []*pb.RateLimitResponse_DescriptorStatus, panicErr error) {
	defer func() {
		if rec := recover(); rec != nil {
			panicErr = fmt.Errorf("panic during rate limiter DoLimit: %v", rec)
			telemetry.MetricUnexpectedPanicTotal.WithLabelValues(
				"ratelimiter-do-limit",
				fmt.Sprintf("budget:%s", b.Id),
				common.ErrorFingerprint(rec),
			).Inc()
			if b.logger != nil {
				b.logger.Error().
					Str("budget", b.Id).
					Str("method", method).
					Str("user", userLabel).
					Str("network", networkLabel).
					Interface("panic", rec).
					Str("stack", string(debug.Stack())).
					Msg("panic recovered during rate limiter DoLimit (failing open)")
			}

			if b.registry != nil {
				b.registry.onCacheFailure(cache, panicErr)
			}
		}
	}()

	return cache.DoLimit(ctx, rlReq, limits), nil
}

// statsKeySuffix returns the pre-computed suffix for stats key.
func (r *RateLimitRule) statsKeySuffix() string {
	suffix := ""
	if r.Config.PerIP {
		suffix += ".ip"
	}
	if r.Config.PerUser {
		suffix += ".user"
	}
	if r.Config.PerNetwork {
		suffix += ".network"
	}
	return suffix
}

// doLimitWithTimeout executes doLimitSafely with a timeout.
// Returns (statuses, timedOut, admissionFull, panicErr).
// On timeout, returns (nil, true, false, nil).
// On admission saturation, returns (nil, false, true, nil).
// On panic from DoLimit, returns (nil, false, false, err).
func (b *RateLimiterBudget) doLimitWithTimeout(
	ctx context.Context,
	cache limiter.RateLimitCache,
	rlReq *pb.RateLimitRequest,
	limits []*config.RateLimit,
	method, userLabel, networkLabel string,
) ([]*pb.RateLimitResponse_DescriptorStatus, bool, bool, error) {
	start := time.Now()
	if b.admission != nil {
		select {
		case b.admission <- struct{}{}:
			if b.inflightGauge != nil {
				b.inflightGauge.Inc()
			}
		default:
			if b.admissionShedded != nil {
				b.admissionShedded.Inc()
			}
			if b.logger != nil && b.logger.GetLevel() <= zerolog.DebugLevel {
				b.logger.Debug().
					Str("budget", b.Id).
					Str("method", method).
					Int("admissionCap", cap(b.admission)).
					Msg("rate limiter remote admission full, failing closed")
			}
			return nil, false, true, nil
		}
	}
	resultCh := make(chan doLimitResult, 1)
	go func() {
		if b.admission != nil {
			defer func() {
				<-b.admission
				if b.inflightGauge != nil {
					b.inflightGauge.Dec()
				}
			}()
		}
		statuses, panicErr := b.doLimitSafely(ctx, cache, rlReq, limits, method, userLabel, networkLabel)
		resultCh <- doLimitResult{statuses: statuses, panicErr: panicErr}
	}()

	timer := time.NewTimer(b.maxTimeout)
	select {
	case result := <-resultCh:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		// Observe remote-call duration split by outcome (ok / over-limit /
		// fail-open). Without this only timeout fail-opens were recorded,
		// leaving the ok & over_limit histograms permanently empty.
		dur := time.Since(start).Seconds()
		if result.panicErr == nil {
			if len(result.statuses) > 0 && result.statuses[0].Code == pb.RateLimitResponse_OVER_LIMIT {
				if b.durationOverlimit != nil {
					b.durationOverlimit.Observe(dur)
				}
			} else if b.durationOK != nil {
				b.durationOK.Observe(dur)
			}
		} else if b.durationFailopen != nil {
			b.durationFailopen.Observe(dur)
		}
		return result.statuses, false, false, result.panicErr

	case <-timer.C:
		if b.durationFailopen != nil {
			b.durationFailopen.Observe(time.Since(start).Seconds())
		}
		// Sample the warn log; under sustained pressure this fires hundreds of
		// times per second and dwarfs the rest of the log volume.
		if b.logger != nil && b.logger.GetLevel() <= zerolog.DebugLevel {
			b.logger.Debug().
				Str("budget", b.Id).
				Str("method", method).
				Dur("timeout", b.maxTimeout).
				Msg("rate limiter timeout exceeded, failing open")
		}

		// The detached DoLimit goroutine may still finish or panic after this return.
		// That late panic is still counted via MetricUnexpectedPanicTotal and onCacheFailure,
		// but this caller has already recorded timeout_fail_open for the permit attempt.
		return nil, true, false, nil
	}
}
