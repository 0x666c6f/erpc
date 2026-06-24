package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGrpcUpstreamConfigRedactsHeaders(t *testing.T) {
	cfg := &UpstreamConfig{
		Id:       "grpc-secure",
		Endpoint: "grpc+bds://example.internal:443/private/path?token=endpoint-secret",
		Grpc: &GrpcUpstreamConfig{
			Headers: map[string]string{
				"authorization": "Bearer grpc-secret-token",
				"x-api-key":     "provider-api-key",
			},
		},
	}

	jsonBytes, err := json.Marshal(cfg)
	require.NoError(t, err)
	assertGrpcConfigSecretsRedacted(t, string(jsonBytes))

	sonicBytes, err := SonicCfg.Marshal(cfg)
	require.NoError(t, err)
	assertGrpcConfigSecretsRedacted(t, string(sonicBytes))

	yamlBytes, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	assertGrpcConfigSecretsRedacted(t, string(yamlBytes))
}

func assertGrpcConfigSecretsRedacted(t *testing.T, text string) {
	t.Helper()

	assert.NotContains(t, text, "grpc-secret-token")
	assert.NotContains(t, text, "provider-api-key")
	assert.NotContains(t, text, "endpoint-secret")
	assert.Contains(t, text, "REDACTED")
	assert.Contains(t, text, "redacted=")
}
