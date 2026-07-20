package thirdparty

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedAlchemyVendorWithDefaultNetworks() *AlchemyVendor {
	vendor := CreateAlchemyVendor().(*AlchemyVendor)
	vendor.cache.snapshot.Store(&remoteCacheSnapshot[map[int64]string]{
		values:    map[string]map[int64]string{alchemyApiUrl: defaultAlchemyNetworkSubdomains},
		fetchedAt: map[string]time.Time{alchemyApiUrl: time.Now()},
	})
	return vendor
}

func TestAlchemyVendor_DefaultNetworkMappingIncludesRecentEvmChains(t *testing.T) {
	testCases := map[int64]string{
		25:      "cronos-mainnet",
		338:     "cronos-testnet",
		196:     "xlayer-mainnet",
		1952:    "xlayer-testnet",
		1672:    "pharos-mainnet",
		4153:    "rise-mainnet",
		4217:    "tempo-mainnet",
		4326:    "megaeth-mainnet",
		42018:   "mythos-mainnet",
		46630:   "robinhood-testnet",
		51014:   "risa-testnet",
		737373:  "katana-bokuto",
		202601:  "ronin-saigon",
		685689:  "gensyn-mainnet",
		99999:   "adi-testnet",
		36900:   "adi-mainnet",
		747474:  "katana-mainnet",
		688689:  "pharos-atlantic",
		5734951: "jovay-mainnet",
		2019775: "jovay-testnet",
	}

	for chainID, expectedSubdomain := range testCases {
		subdomain, ok := defaultAlchemyNetworkSubdomains[chainID]
		require.Truef(t, ok, "expected chain %d to exist in fallback map", chainID)
		require.Equalf(t, expectedSubdomain, subdomain, "unexpected subdomain for chain %d", chainID)
	}
}

func TestAlchemyVendor_SupportsNetwork_UsesFallbackMappings(t *testing.T) {
	logger := zerolog.New(io.Discard)
	vendor := seedAlchemyVendorWithDefaultNetworks()

	for _, chainID := range []int64{4153, 99999, 202601, 737373} {
		t.Run(fmt.Sprintf("evm:%d", chainID), func(t *testing.T) {
			supported, err := vendor.SupportsNetwork(
				context.Background(),
				&logger,
				common.VendorSettings{
					"recheckInterval": 24 * time.Hour,
				},
				fmt.Sprintf("evm:%d", chainID),
			)
			require.NoError(t, err)
			require.True(t, supported)
		})
	}
}

func TestAlchemyVendor_GenerateConfigs_UsesFallbackSubdomains(t *testing.T) {
	logger := zerolog.New(io.Discard)
	vendor := seedAlchemyVendorWithDefaultNetworks()

	testCases := []struct {
		chainID          int64
		expectedEndpoint string
	}{
		{
			chainID:          4153,
			expectedEndpoint: "https://rise-mainnet.g.alchemy.com/v2/demo",
		},
		{
			chainID:          36900,
			expectedEndpoint: "https://adi-mainnet.g.alchemy.com/v2/demo",
		},
		{
			chainID:          202601,
			expectedEndpoint: "https://ronin-saigon.g.alchemy.com/v2/demo",
		},
		{
			chainID:          737373,
			expectedEndpoint: "https://katana-bokuto.g.alchemy.com/v2/demo",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("evm:%d", tc.chainID), func(t *testing.T) {
			cfgs, err := vendor.GenerateConfigs(
				context.Background(),
				&logger,
				&common.UpstreamConfig{
					Evm: &common.EvmUpstreamConfig{
						ChainId: tc.chainID,
					},
				},
				common.VendorSettings{
					"apiKey":          "demo",
					"recheckInterval": 24 * time.Hour,
				},
			)
			require.NoError(t, err)
			require.Len(t, cfgs, 1)
			require.Equal(t, tc.expectedEndpoint, cfgs[0].Endpoint)
		})
	}
}

func TestAlchemyVendor_SupportsNetwork_UnsupportedChainReturnsFalse(t *testing.T) {
	logger := zerolog.New(io.Discard)
	vendor := seedAlchemyVendorWithDefaultNetworks()

	supported, err := vendor.SupportsNetwork(
		context.Background(),
		&logger,
		common.VendorSettings{
			"recheckInterval": 24 * time.Hour,
		},
		"evm:999999998",
	)
	require.NoError(t, err)
	require.False(t, supported)
}

func TestAlchemyVendor_GenerateConfigs_UnsupportedChainReturnsError(t *testing.T) {
	logger := zerolog.New(io.Discard)
	vendor := seedAlchemyVendorWithDefaultNetworks()

	_, err := vendor.GenerateConfigs(
		context.Background(),
		&logger,
		&common.UpstreamConfig{
			Evm: &common.EvmUpstreamConfig{
				ChainId: 999999998,
			},
		},
		common.VendorSettings{
			"apiKey":          "demo",
			"recheckInterval": 24 * time.Hour,
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported network chain ID")
}

func TestAlchemyVendor_MergeAlchemyNetworkSubdomains_PreservesDefaultsAndOverridesWithApi(t *testing.T) {
	merged := mergeAlchemyNetworkSubdomains(map[int64]string{
		1:      "eth-mainnet-override",
		84531:  "",
		84532:  "base-sepolia",
		999001: "new-custom-chain",
	})

	require.Equal(t, "eth-mainnet-override", merged[1])
	require.Equal(t, "katana-mainnet", merged[747474])
	require.Equal(t, "new-custom-chain", merged[999001])
}

func TestAlchemyVendor_Code3_MissingDataIsRetryable(t *testing.T) {
	v := CreateAlchemyVendor()

	makeErr := func(msg string) error {
		jrr, err := common.NewJsonRpcResponse(1, nil, common.NewErrJsonRpcExceptionExternal(3, msg, ""))
		require.NoError(t, err)
		return v.GetVendorSpecificErrorIfAny(nil, &http.Response{StatusCode: 400}, jrr, map[string]interface{}{})
	}

	// Code 3 with a data-availability message must be classified as missing
	// data (retryable toward other upstreams), not as an execution revert.
	for _, msg := range []string{"Unknown block", "block not found with number 0x1234abc"} {
		err := makeErr(msg)
		require.Error(t, err)
		assert.True(t, common.HasErrorCode(err, common.ErrCodeEndpointMissingData), "expected missing-data for %q, got %v", msg, err)
		assert.True(t, common.IsRetryableTowardNetwork(err), "missing-data must stay network-retryable for %q", msg)
	}

	// A genuine execution revert on code 3 keeps the existing classification.
	err := makeErr("execution reverted")
	require.Error(t, err)
	assert.True(t, common.HasErrorCode(err, common.ErrCodeEndpointExecutionException), "expected execution exception, got %v", err)
	assert.False(t, common.IsRetryableTowardNetwork(err))
}
