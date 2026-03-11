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
	// 1 MiB avoids extra envelope copies; 4 MiB skips the heaviest finalized Postgres blob writes.
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
	started := make(chan int32, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})

	poller := &EvmStatePoller{
		appCtx: context.Background(),
		latestPollAsyncFn: func(ctx context.Context) (int64, error) {
			call := calls.Add(1)
			started <- call
			if call == 1 {
				<-firstRelease
			} else {
				<-secondRelease
			}
			return 12, nil
		},
	}

	require.True(t, poller.TriggerLatestPollAsync(time.Second))
	require.False(t, poller.TriggerLatestPollAsync(time.Second))

	select {
	case call := <-started:
		require.Equal(t, int32(1), call)
	case <-time.After(time.Second):
		t.Fatal("latest async poll did not start")
	}

	close(firstRelease)
	require.Eventually(t, func() bool {
		return !poller.latestPollTriggerInFlight.Load()
	}, time.Second, 10*time.Millisecond)

	require.True(t, poller.TriggerLatestPollAsync(time.Second))
	select {
	case call := <-started:
		require.Equal(t, int32(2), call)
	case <-time.After(time.Second):
		t.Fatal("second latest async poll did not start")
	}
	require.False(t, poller.TriggerLatestPollAsync(time.Second))
	assert.True(t, poller.latestPollTriggerInFlight.Load())
	close(secondRelease)
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestEvmStatePoller_TriggerFinalizedPollAsync_DedupesInFlightWork(t *testing.T) {
	var calls atomic.Int32
	started := make(chan int32, 2)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})

	poller := &EvmStatePoller{
		appCtx: context.Background(),
		finalizedPollAsyncFn: func(ctx context.Context) (int64, error) {
			call := calls.Add(1)
			started <- call
			if call == 1 {
				<-firstRelease
			} else {
				<-secondRelease
			}
			return 9, nil
		},
	}

	require.True(t, poller.TriggerFinalizedPollAsync(time.Second))
	require.False(t, poller.TriggerFinalizedPollAsync(time.Second))

	select {
	case call := <-started:
		require.Equal(t, int32(1), call)
	case <-time.After(time.Second):
		t.Fatal("finalized async poll did not start")
	}

	close(firstRelease)
	require.Eventually(t, func() bool {
		return !poller.finalizedPollTriggerInFlight.Load()
	}, time.Second, 10*time.Millisecond)

	require.True(t, poller.TriggerFinalizedPollAsync(time.Second))
	select {
	case call := <-started:
		require.Equal(t, int32(2), call)
	case <-time.After(time.Second):
		t.Fatal("second finalized async poll did not start")
	}
	require.False(t, poller.TriggerFinalizedPollAsync(time.Second))
	assert.True(t, poller.finalizedPollTriggerInFlight.Load())
	close(secondRelease)
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 10*time.Millisecond)
}
