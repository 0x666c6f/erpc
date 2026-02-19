package common

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchUpstreamSelectionCache_ContextRoundTrip(t *testing.T) {
	cache := NewBatchUpstreamSelectionCache()
	ctx := WithBatchUpstreamSelectionCache(context.Background(), cache)

	require.NotNil(t, BatchUpstreamSelectionCacheFromContext(ctx))
	assert.Nil(t, BatchUpstreamSelectionCacheFromContext(context.Background()))
}

func TestBatchUpstreamSelectionCache_ResolveConcurrentSingleLoaderCall(t *testing.T) {
	cache := NewBatchUpstreamSelectionCache()
	key := BatchUpstreamSelectionKey{
		NetworkID: "evm:1",
		Method:    "eth_getBalance",
		Finality:  DataFinalityStateUnfinalized,
	}
	mockUpstream := &mockUpstreamForSelection{id: "upstream-a"}

	var loaderCalls atomic.Int32

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ups, hit, err := cache.Resolve(key, func() ([]Upstream, error) {
				loaderCalls.Add(1)
				return []Upstream{mockUpstream}, nil
			})
			require.NoError(t, err)
			require.Len(t, ups, 1)
			assert.Equal(t, "upstream-a", ups[0].Id())
			_ = hit
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), loaderCalls.Load())
}
