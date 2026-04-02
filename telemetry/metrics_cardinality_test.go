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
