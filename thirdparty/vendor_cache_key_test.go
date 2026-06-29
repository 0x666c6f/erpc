package thirdparty

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVendorCapabilityCacheKey_IsolatesProviderAndSecret(t *testing.T) {
	key := vendorCapabilityCacheKey("quicknode", "provider-a", secretCachePart("apiKey", "secret-value"))

	assert.Contains(t, key, "quicknode")
	assert.Contains(t, key, "provider=provider-a")
	assert.NotContains(t, key, "secret-value")
	assert.NotContains(t, key, "apiKey=secret-value")
	require.Contains(t, key, "|key=")
	assert.Len(t, strings.Split(key, "|key=")[1], 16)
}

func TestVendorCapabilityCacheKey_DifferentInputsDoNotCollide(t *testing.T) {
	base := vendorCapabilityCacheKey("repository", "provider-a", cachePart("repositoryUrl", "https://example.com/a"))
	differentProvider := vendorCapabilityCacheKey("repository", "provider-b", cachePart("repositoryUrl", "https://example.com/a"))
	differentPart := vendorCapabilityCacheKey("repository", "provider-a", cachePart("repositoryUrl", "https://example.com/b"))

	assert.NotEqual(t, base, differentProvider)
	assert.NotEqual(t, base, differentPart)
}

func TestSecretCachePart_UsesStableOpaqueToken(t *testing.T) {
	first := secretCachePart("apiKey", "secret-a")
	second := secretCachePart("apiKey", "secret-a")
	other := secretCachePart("apiKey", "secret-b")
	otherName := secretCachePart("otherKey", "secret-a")

	assert.Equal(t, first, second)
	assert.NotEqual(t, first, other)
	assert.NotEqual(t, first, otherName)
	assert.Contains(t, first, "apiKey=secret:")
	assert.NotContains(t, first, "secret-a")
}
