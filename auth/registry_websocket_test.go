package auth

import (
	"context"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestAuthRegistryAuthenticateWebsocketRequiresExplicitAccess(t *testing.T) {
	t.Parallel()
	logger := zerolog.Nop()
	registry, err := NewAuthRegistry(context.Background(), &logger, "test", &common.AuthConfig{
		Strategies: []*common.AuthStrategyConfig{
			{
				Type:   common.AuthTypeSecret,
				Secret: &common.SecretStrategyConfig{Id: "http-only", Value: "denied-key"},
			},
			{
				Type:           common.AuthTypeSecret,
				AllowWebsocket: true,
				Secret:         &common.SecretStrategyConfig{Id: "websocket", Value: "allowed-key"},
			},
		},
	}, nil)
	require.NoError(t, err)

	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`))

	_, err = registry.AuthenticateWebsocket(context.Background(), req, "eth_subscribe", &AuthPayload{
		Type: common.AuthTypeSecret, Secret: &SecretPayload{Value: "denied-key"},
	})
	require.Error(t, err)
	require.True(t, common.HasErrorCode(err, common.ErrCodeAuthUnauthorized))

	user, err := registry.AuthenticateWebsocket(context.Background(), req, "eth_subscribe", &AuthPayload{
		Type: common.AuthTypeSecret, Secret: &SecretPayload{Value: "allowed-key"},
	})
	require.NoError(t, err)
	require.Equal(t, "websocket", user.Id)
}
