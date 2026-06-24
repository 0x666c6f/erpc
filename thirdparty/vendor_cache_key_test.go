package thirdparty

import (
	"strings"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVendorCapabilityCacheKey_IsolatesProviderAndSecret(t *testing.T) {
	settings := common.VendorSettings{
		vendorSettingProviderID: "provider-a",
	}

	key := vendorCapabilityCacheKey("quicknode", settings, secretCachePart("apiKey", "secret-value"))

	assert.Contains(t, key, "quicknode")
	assert.Contains(t, key, "provider=provider-a")
	assert.NotContains(t, key, "secret-value")
	assert.NotContains(t, key, "apiKey=secret-value")
	require.Contains(t, key, "|key=")
	assert.Len(t, strings.Split(key, "|key=")[1], 16)
}

func TestVendorCapabilityCacheKey_DifferentInputsDoNotCollide(t *testing.T) {
	baseSettings := common.VendorSettings{vendorSettingProviderID: "provider-a"}

	base := vendorCapabilityCacheKey("repository", baseSettings, cachePart("repositoryUrl", "https://example.com/a"))
	differentProvider := vendorCapabilityCacheKey("repository", common.VendorSettings{vendorSettingProviderID: "provider-b"}, cachePart("repositoryUrl", "https://example.com/a"))
	differentPart := vendorCapabilityCacheKey("repository", baseSettings, cachePart("repositoryUrl", "https://example.com/b"))

	assert.NotEqual(t, base, differentProvider)
	assert.NotEqual(t, base, differentPart)
}

func TestSecretCachePart_UsesStableOpaqueToken(t *testing.T) {
	first := secretCachePart("apiKey", "secret-a")
	second := secretCachePart("apiKey", "secret-a")
	other := secretCachePart("apiKey", "secret-b")

	assert.Equal(t, first, second)
	assert.NotEqual(t, first, other)
	assert.Contains(t, first, "apiKey=secret:")
	assert.NotContains(t, first, "secret-a")
}
