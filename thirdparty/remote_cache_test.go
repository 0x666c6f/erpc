package thirdparty

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var remoteCacheLogLevelMu sync.Mutex

func TestRemoteDataCacheRedactsCacheKeyInRefreshFailureLogs(t *testing.T) {
	remoteCacheLogLevelMu.Lock()
	prevLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	t.Cleanup(func() {
		zerolog.SetGlobalLevel(prevLevel)
		remoteCacheLogLevelMu.Unlock()
	})

	var logs bytes.Buffer
	logger := zerolog.New(&logs).Level(zerolog.WarnLevel)
	cache := NewRemoteDataCache[string]("quicknode")
	cacheKey := "https://example.quiknode.pro/raw-provider-secret/evm?apiKey=provider-api-key"
	fetchCalled := make(chan struct{})

	cache.TriggerAsyncRefresh(&logger, cacheKey, func(ctx context.Context) (string, error) {
		close(fetchCalled)
		return "", errors.New("refresh failed")
	})

	select {
	case <-fetchCalled:
	case <-time.After(time.Second):
		t.Fatal("fetcher was not called")
	}

	require.Eventually(t, func() bool {
		cache.refreshMu.Lock()
		defer cache.refreshMu.Unlock()
		return len(cache.inflight) == 0
	}, time.Second, 10*time.Millisecond)

	text := logs.String()
	assert.Contains(t, text, "vendor remote-data refresh failed")
	assert.NotContains(t, text, cacheKey)
	assert.NotContains(t, text, "raw-provider-secret")
	assert.NotContains(t, text, "provider-api-key")
	assert.Contains(t, text, "redacted=")
}
