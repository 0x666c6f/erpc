package evm

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLargeFinalizedGetLogsCacheGuards(t *testing.T) {
	assert.True(t, isLargeFinalizedGetLogsPayload("eth_getLogs", common.DataFinalityStateFinalized, largeFinalizedGetLogsEnvelopeBypassBytes))
	assert.False(t, isLargeFinalizedGetLogsPayload("eth_call", common.DataFinalityStateFinalized, largeFinalizedGetLogsEnvelopeBypassBytes))
	assert.False(t, isLargeFinalizedGetLogsPayload("eth_getLogs", common.DataFinalityStateUnfinalized, largeFinalizedGetLogsEnvelopeBypassBytes))

	assert.True(t, shouldSkipOversizedFinalizedGetLogsPostgresWrite(
		"eth_getLogs",
		common.DataFinalityStateFinalized,
		largeFinalizedGetLogsPostgresSkipBytes,
		&data.PostgreSQLConnector{},
	))
	assert.False(t, shouldSkipOversizedFinalizedGetLogsPostgresWrite(
		"eth_getLogs",
		common.DataFinalityStateFinalized,
		largeFinalizedGetLogsPostgresSkipBytes-1,
		&data.PostgreSQLConnector{},
	))
	assert.False(t, shouldSkipOversizedFinalizedGetLogsPostgresWrite(
		"eth_getLogs",
		common.DataFinalityStateFinalized,
		largeFinalizedGetLogsPostgresSkipBytes,
		&data.RedisConnector{},
	))
}

func TestEvmStatePoller_TriggerLatestPollAsync_DedupesInFlightWork(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	poller := &EvmStatePoller{
		appCtx: context.Background(),
		latestPollAsyncFn: func(ctx context.Context) (int64, error) {
			calls.Add(1)
			started <- struct{}{}
			<-release
			return 12, nil
		},
	}

	require.True(t, poller.TriggerLatestPollAsync(time.Second))
	require.False(t, poller.TriggerLatestPollAsync(time.Second))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("latest async poll did not start")
	}

	close(release)
	require.Eventually(t, func() bool {
		return !poller.latestPollTriggerInFlight.Load()
	}, time.Second, 10*time.Millisecond)

	require.True(t, poller.TriggerLatestPollAsync(time.Second))
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestEvmStatePoller_TriggerFinalizedPollAsync_DedupesInFlightWork(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	poller := &EvmStatePoller{
		appCtx: context.Background(),
		finalizedPollAsyncFn: func(ctx context.Context) (int64, error) {
			calls.Add(1)
			started <- struct{}{}
			<-release
			return 9, nil
		},
	}

	require.True(t, poller.TriggerFinalizedPollAsync(time.Second))
	require.False(t, poller.TriggerFinalizedPollAsync(time.Second))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("finalized async poll did not start")
	}

	close(release)
	require.Eventually(t, func() bool {
		return !poller.finalizedPollTriggerInFlight.Load()
	}, time.Second, 10*time.Millisecond)

	require.True(t, poller.TriggerFinalizedPollAsync(time.Second))
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 10*time.Millisecond)
}
