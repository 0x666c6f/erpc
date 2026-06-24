package thirdparty

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteDataCacheRedactsCacheKeyInRefreshFailureLogs(t *testing.T) {
	prevLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	defer zerolog.SetGlobalLevel(prevLevel)

	var logs bytes.Buffer
	logger := zerolog.New(&logs)
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
		return strings.Contains(logs.String(), "vendor remote-data refresh failed")
	}, time.Second, 10*time.Millisecond)

	text := logs.String()
	assert.NotContains(t, text, cacheKey)
	assert.NotContains(t, text, "raw-provider-secret")
	assert.NotContains(t, text, "provider-api-key")
	assert.Contains(t, text, "redacted=")
}
