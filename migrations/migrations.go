// Package migrations embeds the schema migrations of every database, so the
// service binary carries its own schema. See the db-migrations skill.
package migrations

import "embed"

// Cassandra holds cassandra/NNNN_name.cql files.
//
//go:embed cassandra/*.cql
var Cassandra embed.FS

// ClickHouse holds clickhouse/NNNN_name.sql files.
//
//go:embed clickhouse/*.sql
var ClickHouse embed.FS

// Postgres holds postgres/NNNN_name.sql files.
//
//go:embed postgres/*.sql
var Postgres embed.FS
