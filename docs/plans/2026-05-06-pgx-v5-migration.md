# pgx v5 Migration Audit

**Date:** 2026-05-06
**Status:** Implemented (PLA-1392, 2026-07-08)

Scope: `github.com/jackc/pgx/v4` in `data/postgresql.go`.

Current usage is concentrated in the PostgreSQL connector:

- Imports: `github.com/jackc/pgx/v4` and `github.com/jackc/pgx/v4/pgxpool`.
- Pool lifecycle: `pgxpool.ParseConfig`, `pgxpool.ConnectConfig`, `Pool.Acquire`, `Config.ConnString()`.
- Queries: `Exec`, `QueryRow`, `Query`, `Rows.Next`, `Rows.Scan`, `Rows.Err`, `CommandTag.RowsAffected`.
- Transactions and locking: `Pool.Begin`, `pgx.Tx`, `QueryRow`, `Commit`, `Rollback`.
- Pubsub: `pg_notify`, `LISTEN`, `Conn().WaitForNotification`.
- Error mapping: `pgx.ErrNoRows`.

Migration notes:

- Update imports to `github.com/jackc/pgx/v5` and `github.com/jackc/pgx/v5/pgxpool`.
- Replace `pgxpool.ConnectConfig` with `pgxpool.NewWithConfig`; pgx v5 renamed pool constructors and pools always connect lazily.
- Re-check startup behavior because current initialization expects the first connection attempt to happen during `NewPostgreSQLConnector`.
- Keep `pgxpool.ParseConfig` and `Config.ConnString()`; both remain in v5 docs.
- Validate `pgx.ErrNoRows` handling; v5 keeps it and wraps `sql.ErrNoRows`.
- No direct `pgtype`, `pgconn`, or `pgproto3` code usage exists outside indirect dependencies, so most v5 type-system churn should not affect repo code.
- Run the real Postgres connector tests because listener reconnects, advisory-lock transactions, and startup readiness are the highest-risk paths.
- Reference: https://github.com/jackc/pgx/blob/master/CHANGELOG.md

Suggested follow-up:

- Dedicated pgx v5 PR that updates the imports, adapts pool creation, and adds/updates connector tests for lazy pool startup.
- Validation: `go test ./data -run 'TestPostgre' -count=1`, `go test ./data -run 'TestPostgreSQLDistributedLocking' -count=1`, and full `make test-fast` if runtime allows.

Implementation outcome (PLA-1392):

- `go.mod`: `github.com/jackc/pgx/v4` and direct `github.com/jackc/pgconn` v1 replaced by `github.com/jackc/pgx/v5` (v5.10.0); the v1-era `pgtype`, `pgio`, and `pgproto3/v2` indirects dropped, `puddle/v2` added.
- One correction to the audit: `data/postgresql.go` (PgError SQLSTATE matching in `isPostgresConnectionError`) and two test files did import `pgconn` v1 directly; all moved to `github.com/jackc/pgx/v5/pgconn`.
- Pool creation goes through a new `connectPostgreSQLPool` helper (`pgxpool.NewWithConfig` + eager `Ping`) so `connectTask`, readonly replicas, and the listener pool keep v4's fail-fast dial semantics — without the Ping, a reconnect after schema setup would publish a dead pool as ready.
- `err == pgx.ErrNoRows` comparisons switched to `errors.Is`, since v5 may wrap the sentinel.
- `newRDSBeforeConnect` needed only the import bump: v5 keeps the `func(context.Context, *pgx.ConnConfig) error` hook shape and `User`/`Password` are promoted from the embedded `pgconn.Config`.
- Validated with the real-Postgres container tests (`TestPostgreConnectorInitialization` both readiness cases, `TestPostgreSQLDistributedLocking` all five locking scenarios) plus the `data`/`auth` unit suites and `make test-fast`.
