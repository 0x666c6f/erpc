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


func TestUpstreamConfigRedactsWebsocketEndpoint(t *testing.T) {
	cfg := &UpstreamConfig{Id: "websocket-secure", Endpoint: "https://example.internal/rpc?token=http-secret", WebsocketEndpoint: "wss://example.internal/private/ws?token=websocket-secret"}
	jsonBytes, err := json.Marshal(cfg); require.NoError(t, err)
	sonicBytes, err := SonicCfg.Marshal(cfg); require.NoError(t, err)
	yamlBytes, err := yaml.Marshal(cfg); require.NoError(t, err)
	for _, text := range []string{string(jsonBytes), string(sonicBytes), string(yamlBytes)} {
		assert.NotContains(t, text, "http-secret")
		assert.NotContains(t, text, "websocket-secret")
		assert.Contains(t, text, "redacted=")
	}
}
