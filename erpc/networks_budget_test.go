package erpc

import (
	"context"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyNetworkMethodClass(t *testing.T) {
	assert.Equal(t, networkMethodClassGetLogs, classifyNetworkMethodClass("eth_getLogs"))
	assert.Equal(t, networkMethodClassEthCall, classifyNetworkMethodClass("eth_call"))
	assert.Equal(t, networkMethodClassDefault, classifyNetworkMethodClass("eth_blockNumber"))
}

func TestDeriveMethodClassBudgetsUsesDedicatedGetLogsCapacity(t *testing.T) {
	cfg := &common.NetworkConfig{
		Evm: &common.EvmNetworkConfig{
			GetLogsSplitConcurrency:      7,
			GetLogsCacheChunkConcurrency: 11,
		},
	}

	budgets := deriveMethodClassBudgets(cfg)
	require.NotNil(t, budgets[networkMethodClassGetLogs])
	require.NotNil(t, budgets[networkMethodClassEthCall])
	require.NotNil(t, budgets[networkMethodClassDefault])
	assert.GreaterOrEqual(t, cap(budgets[networkMethodClassGetLogs]), 44)
	assert.GreaterOrEqual(t, cap(budgets[networkMethodClassEthCall]), 8)
	assert.GreaterOrEqual(t, cap(budgets[networkMethodClassDefault]), 16)
}

func TestNetworkMethodClassPermitsIsolateGetLogsFromEthCall(t *testing.T) {
	n := &Network{
		methodClassSems: map[networkMethodClass]chan struct{}{
			networkMethodClassGetLogs: make(chan struct{}, 1),
			networkMethodClassEthCall: make(chan struct{}, 1),
			networkMethodClassDefault: make(chan struct{}, 1),
		},
	}

	require.NoError(t, acquireBoundedPermit(context.Background(), n.getMethodClassSem("eth_getLogs")))
	defer releaseBoundedPermit(n.getMethodClassSem("eth_getLogs"))

	otherClassCtx, otherClassCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer otherClassCancel()
	require.NoError(t, acquireBoundedPermit(otherClassCtx, n.getMethodClassSem("eth_call")))
	releaseBoundedPermit(n.getMethodClassSem("eth_call"))

	sameClassCtx, sameClassCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer sameClassCancel()
	err := acquireBoundedPermit(sameClassCtx, n.getMethodClassSem("eth_getLogs"))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestNetworkGetLogsCacheSemaphoresUseDedicatedPools(t *testing.T) {
	n := &Network{
		cacheWriteSem:        make(chan struct{}, 5),
		getLogsCacheReadSem:  make(chan struct{}, 2),
		getLogsCacheWriteSem: make(chan struct{}, 3),
	}

	assert.Equal(t, n.getLogsCacheReadSem, n.getCacheReadSem("eth_getLogs"))
	assert.Nil(t, n.getCacheReadSem("eth_call"))
	assert.Equal(t, n.getLogsCacheWriteSem, n.getAsyncCacheWriteSem("eth_getLogs"))
	assert.Equal(t, n.cacheWriteSem, n.getAsyncCacheWriteSem("eth_call"))
}

func TestShouldForceCacheMaterialization(t *testing.T) {
	assert.False(t, shouldForceCacheMaterialization("eth_getLogs"))
	assert.True(t, shouldForceCacheMaterialization("eth_call"))
	assert.True(t, shouldForceCacheMaterialization("eth_blockNumber"))
}
