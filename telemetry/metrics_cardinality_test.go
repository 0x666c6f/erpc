package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func metricLabelsByName(t *testing.T, familyName string) map[string]string {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		require.NotEmpty(t, family.Metric, "metric family %s has no samples", familyName)
		labels := make(map[string]string, len(family.Metric[0].Label))
		for _, pair := range family.Metric[0].Label {
			labels[pair.GetName()] = pair.GetValue()
		}
		return labels
	}

	t.Fatalf("metric family %s not found", familyName)
	return nil
}

func TestUpstreamRequestDurationOmitsUserLabel(t *testing.T) {
	require.NoError(t, SetHistogramBuckets(""))
	MetricUpstreamRequestDuration.Reset()

	MetricUpstreamRequestDuration.WithLabelValues(
		"project-a",
		"vendor-a",
		"evm:1",
		"upstream-a",
		"eth_call",
		"none",
		"finalized",
	).Observe(0.1)

	labels := metricLabelsByName(t, "erpc_upstream_request_duration_seconds")
	require.Equal(t, "upstream-a", labels["upstream"])
	require.Equal(t, "vendor-a", labels["vendor"])
	require.NotContains(t, labels, "user")
}

func TestNetworkRequestDurationUsesOutcomeInsteadOfHighCardinalityLabels(t *testing.T) {
	require.NoError(t, SetHistogramBuckets(""))
	MetricNetworkRequestDuration.Reset()

	MetricNetworkRequestDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"finalized",
		"cache",
	).Observe(0.1)

	labels := metricLabelsByName(t, "erpc_network_request_duration_seconds")
	require.Equal(t, "cache", labels["outcome"])
	require.NotContains(t, labels, "vendor")
	require.NotContains(t, labels, "upstream")
	require.NotContains(t, labels, "user")
}

func TestCacheDurationHistogramsOmitUserLabel(t *testing.T) {
	require.NoError(t, SetHistogramBuckets(""))

	MetricCacheSetSuccessDuration.Reset()
	MetricCacheSetErrorDuration.Reset()
	MetricCacheGetSuccessHitDuration.Reset()
	MetricCacheGetSuccessMissDuration.Reset()
	MetricCacheGetErrorDuration.Reset()

	MetricCacheSetSuccessDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"redis-connector",
		"policy-a",
		"5s",
	).Observe(0.1)
	MetricCacheSetErrorDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"redis-connector",
		"policy-a",
		"5s",
		"ContextCanceled",
	).Observe(0.1)
	MetricCacheGetSuccessHitDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"redis-connector",
		"policy-a",
		"5s",
	).Observe(0.1)
	MetricCacheGetSuccessMissDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"redis-connector",
		"policy-a",
		"5s",
	).Observe(0.1)
	MetricCacheGetErrorDuration.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_call",
		"redis-connector",
		"policy-a",
		"5s",
		"ErrRecordNotFound",
	).Observe(0.1)

	successLabels := metricLabelsByName(t, "erpc_cache_set_success_duration_seconds")
	require.Equal(t, "redis-connector", successLabels["connector"])
	require.NotContains(t, successLabels, "user")

	setErrorLabels := metricLabelsByName(t, "erpc_cache_set_error_duration_seconds")
	require.Equal(t, "ContextCanceled", setErrorLabels["error"])
	require.NotContains(t, setErrorLabels, "user")

	hitLabels := metricLabelsByName(t, "erpc_cache_get_success_hit_duration_seconds")
	require.Equal(t, "policy-a", hitLabels["policy"])
	require.NotContains(t, hitLabels, "user")

	missLabels := metricLabelsByName(t, "erpc_cache_get_success_miss_duration_seconds")
	require.Equal(t, "5s", missLabels["ttl"])
	require.NotContains(t, missLabels, "user")

	getErrorLabels := metricLabelsByName(t, "erpc_cache_get_error_duration_seconds")
	require.Equal(t, "ErrRecordNotFound", getErrorLabels["error"])
	require.NotContains(t, getErrorLabels, "user")
}

func TestNetworkEvmGetLogsRangeRequestedOmitsUserLabel(t *testing.T) {
	MetricNetworkEvmGetLogsRangeRequested.Reset()

	MetricNetworkEvmGetLogsRangeRequested.WithLabelValues(
		"project-a",
		"evm:1",
		"eth_getLogs",
		"finalized",
	).Observe(16)

	labels := metricLabelsByName(t, "erpc_network_evm_get_logs_range_requested")
	require.Equal(t, "eth_getLogs", labels["category"])
	require.Equal(t, "finalized", labels["finality"])
	require.NotContains(t, labels, "user")
}
