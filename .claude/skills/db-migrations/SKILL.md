---
name: db-migrations
description: Rules for database schemas in this repository — every schema change is a numbered migration file in migrations/<database>/, applied on startup, and every database has integration tests for its migrations and queries. Use whenever adding or changing a table, column, index or keyspace, adding a new database, or writing a store that talks to a database.
---

# Database migrations

Schemas live **only** in migration files. Go code never contains `CREATE TABLE` / `ALTER TABLE`; it applies the migrations on startup.

## Layout

```
migrations/
  migrations.go              go:embed for every database directory
  cassandra/0001_create_links.cql
  cassandra/0002_create_links_by_user.cql
  postgres/0001_create_users.sql
  postgres/0002_create_api_keys.sql
  clickhouse/0001_create_clicks.sql
```

- One directory per database, one `embed.FS` variable per directory in `migrations/migrations.go`.
- File name: `NNNN_description.ext`: four-digit version starting at `0001`, consecutive, lowercase `snake_case` description, `.sql` for PostgreSQL, `.cql` for Cassandra. `internal/migrate.Load` rejects anything else.
- Statements end with `;` at the end of a line. `--` comment lines are allowed and encouraged: say *why* the table or index exists.

## Changing a schema

1. **Never edit or delete a migration that has been committed.** Databases that already applied it would never see the change, and fresh databases would diverge from existing ones.
2. Add a new file with the next version, e.g. `migrations/postgres/0003_add_users_name.sql`.
3. Update the store code and its integration tests in the same commit.
4. Update the "Data Model" section of `README.md` if the model changes.

## Database-specific rules

**PostgreSQL** (`internal/storage/postgres`):
- Each migration runs in one transaction together with its `schema_migrations` row, so it is applied completely or not at all.
- Concurrent starts are serialized with `pg_advisory_lock`.
- Prefer additive changes (new nullable column, new table, `CREATE INDEX CONCURRENTLY` in its own migration). Renames and drops need a multi-step migration: add new → backfill → switch code → drop old in a later release.

**Cassandra** (`internal/storage/cassandra`):
- DDL is not transactional: a migration interrupted halfway is re-run from the start. **Every statement must be idempotent**: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE ... ADD` only for columns that cannot exist yet.
- The keyspace itself is created in code, because its replication factor depends on the deployment. Everything inside the keyspace comes from migrations.
- Design tables per query (query-first modeling); a new query usually means a new table, not a new index.

**ClickHouse** (`internal/storage/clickhouse`):
- DDL is not transactional either: every statement must be idempotent (`IF NOT EXISTS`).
- A materialized view only processes rows inserted after it is created. A migration that adds a view over existing data must backfill it explicitly (`INSERT INTO target SELECT ... FROM source`), and say so in a comment.
- `SummingMergeTree` collapses rows only during background merges: queries must aggregate (`sum(clicks)`), never read rows as final.

## A new database

1. Add `migrations/<db>/0001_....ext` and an `embed.FS` in `migrations/migrations.go`.
2. Add the directory to `TestRepositoryMigrations` in `internal/migrate/migrate_test.go`.
3. Write a runner in the store package following the existing ones: load with `migrate.Load`, read applied versions from `schema_migrations`, apply `migrate.Pending`, record each version.
4. Add the service to `docker-compose.yml` with a healthcheck.
5. Write the integration tests below.

## Required integration tests

Integration tests use the `//go:build integration` tag, run against the `docker-compose.yml` services (`make up`, then `make test-integration`), and follow the `go-table-tests` skill. Every database package must have:

1. **`TestMigrations`** on a **fresh** database object (a new schema via `search_path` in PostgreSQL, a new keyspace in Cassandra), dropped in `t.Cleanup`. Steps:
   - apply on empty: every expected table exists and `schema_migrations` lists every version;
   - apply again: no error, the same versions, and data written in the first step is still there.
2. **A test for every query the store runs**: each `Store` method, its not-found and conflict paths, and anything the database must guarantee (uniqueness under concurrent writes, transactions rolling back, pagination without gaps or duplicates).
3. Unique keys or IDs per run (random suffixes), since rows from earlier runs stay in the shared test database.

## Checklist before finishing

- [ ] No `CREATE`/`ALTER` statements in Go code outside migration runners and test cleanup.
- [ ] New schema changes are new files; no committed migration was modified.
- [ ] Cassandra statements are idempotent.
- [ ] `TestMigrations` and query tests exist for every database touched.
- [ ] `make test-integration` and `make coverage` pass.
