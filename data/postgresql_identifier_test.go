package data

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestNewPostgreSQLTableIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		wantSQL    string
		wantSchema string
		wantName   string
	}{
		{
			name:     "unqualified",
			table:    "erpc_json_rpc_cache",
			wantSQL:  `"erpc_json_rpc_cache"`,
			wantName: "erpc_json_rpc_cache",
		},
		{
			name:       "schema qualified",
			table:      "myschema.cache",
			wantSQL:    `"myschema"."cache"`,
			wantSchema: "myschema",
			wantName:   "cache",
		},
		{
			name:       "matches postgres unquoted case folding",
			table:      "MySchema.Cache",
			wantSQL:    `"myschema"."cache"`,
			wantSchema: "myschema",
			wantName:   "cache",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newPostgreSQLTableIdentifier(tt.table)
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, got.sql)
			require.Equal(t, tt.wantSchema, got.schema)
			require.Equal(t, tt.wantName, got.name)
		})
	}
}

func TestNewPostgreSQLConnectorRejectsInvalidTableIdentifier(t *testing.T) {
	logger := zerolog.New(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &common.PostgreSQLConnectorConfig{
		Table:         "; DROP TABLE foo; --",
		ConnectionUri: "postgres://user:pass@127.0.0.1:9876/bogusdb?sslmode=disable",
		InitTimeout:   common.Duration(time.Second),
		GetTimeout:    common.Duration(time.Second),
		SetTimeout:    common.Duration(time.Second),
		MinConns:      1,
		MaxConns:      2,
	}

	connector, err := NewPostgreSQLConnector(ctx, &logger, "test-invalid-table", cfg)
	require.Error(t, err)
	require.Nil(t, connector)
	require.ErrorContains(t, err, "postgres table identifier")
}
