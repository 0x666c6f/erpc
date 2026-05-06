package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQLConnectorConfigValidateTableIdentifier(t *testing.T) {
	baseConfig := func(table string) *PostgreSQLConnectorConfig {
		return &PostgreSQLConnectorConfig{
			Table:         table,
			ConnectionUri: "postgres://user:pass@localhost/db?sslmode=disable",
			InitTimeout:   Duration(time.Second),
			GetTimeout:    Duration(time.Second),
			SetTimeout:    Duration(time.Second),
			MinConns:      1,
			MaxConns:      2,
		}
	}

	for _, table := range []string{
		"erpc_json_rpc_cache",
		"myschema.cache",
		"_schema.table_1",
		"CamelCase",
	} {
		t.Run("valid_"+table, func(t *testing.T) {
			require.NoError(t, baseConfig(table).Validate())
		})
	}

	for _, table := range []string{
		"; DROP TABLE foo; --",
		"schema.table.extra",
		"bad-name",
		"1table",
		".table",
		"schema.",
		"schema.*",
		"table name",
	} {
		t.Run("invalid_"+table, func(t *testing.T) {
			err := baseConfig(table).Validate()
			require.Error(t, err)
			require.ErrorContains(t, err, "database.*.connector.postgresql.table is invalid")
		})
	}
}
