package thirdparty

import (
	"context"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestQuicknodeFilterParams(t *testing.T) {
	vendor := CreateQuicknodeVendor().(*QuicknodeVendor)

	t.Run("extracts both tag IDs and labels from interface arrays", func(t *testing.T) {
		settings := common.VendorSettings{
			"apiKey":    "***",
			"tagIds":    []interface{}{123, 456},
			"tagLabels": []interface{}{"production", "staging"},
		}
		result := vendor.extractFilterParams(settings)
		assert.Equal(t, []int{123, 456}, result.TagIDs)
		assert.Equal(t, []string{"production", "staging"}, result.TagLabels)
	})

	t.Run("extracts both tag IDs and labels from concrete types", func(t *testing.T) {
		settings := common.VendorSettings{
			"apiKey":    "***",
			"tagIds":    []int{123, 456},
			"tagLabels": []string{"production", "staging"},
		}
		result := vendor.extractFilterParams(settings)
		assert.Equal(t, []int{123, 456}, result.TagIDs)
		assert.Equal(t, []string{"production", "staging"}, result.TagLabels)
	})

	t.Run("handles empty settings", func(t *testing.T) {
		settings := common.VendorSettings{"apiKey": "***"}
		result := vendor.extractFilterParams(settings)
		assert.Empty(t, result.TagIDs)
		assert.Empty(t, result.TagLabels)
	})
}

func TestQuicknodeGenerateConfigs(t *testing.T) {
	vendor := CreateQuicknodeVendor().(*QuicknodeVendor)
	settings := common.VendorSettings{
		"apiKey":          "test-key",
		"recheckInterval": time.Hour,
	}
	cacheKey := vendor.getCacheKey("test-key", &QuicknodeFilterParams{}, settings)
	vendor.cache.snapshot.Store(&remoteCacheSnapshot[[]*QuicknodeEndpoint]{
		values: map[string][]*QuicknodeEndpoint{cacheKey: {
			{
				ID:      "589650",
				HttpUrl: "https://example-hyperevm.quiknode.pro/secret/evm",
				ChainID: 999,
			},
		}},
		fetchedAt: map[string]time.Time{cacheKey: time.Now()},
	})

	upstream := &common.UpstreamConfig{
		Id:   "quicknode-evm:999",
		Type: common.UpstreamTypeEvm,
		Evm: &common.EvmUpstreamConfig{
			ChainId: 999,
		},
	}

	logger := zerolog.Nop()
	configs, err := vendor.GenerateConfigs(context.Background(), &logger, upstream, settings)

	assert.NoError(t, err)
	assert.Len(t, configs, 1)
	assert.Equal(t, "https://example-hyperevm.quiknode.pro/secret/evm/nanoreth", configs[0].Endpoint)
}

func TestQuicknodeCacheKeyReusesEndpointSnapshotAcrossNetworks(t *testing.T) {
	vendor := CreateQuicknodeVendor().(*QuicknodeVendor)
	settings := common.VendorSettings{
		"apiKey":          "test-key",
		"recheckInterval": time.Hour,
	}
	cacheKey := vendor.getCacheKey("test-key", &QuicknodeFilterParams{}, settings)
	vendor.cache.snapshot.Store(&remoteCacheSnapshot[[]*QuicknodeEndpoint]{
		values: map[string][]*QuicknodeEndpoint{cacheKey: {
			{ID: "1", HttpUrl: "https://mainnet.quiknode.pro/secret", ChainID: 1},
			{ID: "10", HttpUrl: "https://optimism.quiknode.pro/secret", ChainID: 10},
		}},
		fetchedAt: map[string]time.Time{cacheKey: time.Now()},
	})

	logger := zerolog.Nop()
	mainnetSupported, err := vendor.SupportsNetwork(context.Background(), &logger, settings, "evm:1")
	assert.NoError(t, err)
	assert.True(t, mainnetSupported)

	optimismSupported, err := vendor.SupportsNetwork(context.Background(), &logger, settings, "evm:10")
	assert.NoError(t, err)
	assert.True(t, optimismSupported)
}
