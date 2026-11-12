// Package postgres is an account.Store backed by PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pristupaanastasia/shortener/internal/account"
	"github.com/pristupaanastasia/shortener/internal/migrate"
	"github.com/pristupaanastasia/shortener/migrations"
)

// uniqueViolation is the PostgreSQL error code for a UNIQUE conflict.
const uniqueViolation = "23505"

// Accounts implements account.Store.
type Accounts struct {
	pool *pgxpool.Pool
}

// Open connects to PostgreSQL and applies pending migrations.
func Open(ctx context.Context, dsn string) (*Accounts, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := applyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Accounts{pool: pool}, nil
}

// migrationLock is the pg_advisory_lock key that serializes migrations when
// several instances start at once.
const migrationLock = 0x5f0e7e

// applyMigrations runs the migrations from migrations/postgres that are not
// recorded in schema_migrations, each in its own transaction together with
// its schema_migrations row.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	all, err := migrate.Load(migrations.Postgres, "postgres")
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLock); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLock) }()

	_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version int PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	applied := map[int]bool{}
	for _, v := range versions {
		applied[v] = true
	}

	for _, m := range migrate.Pending(all, applied) {
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			for _, stmt := range m.Statements {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", m.Name, err)
		}
	}
	return nil
}

// Close closes the connection pool.
func (a *Accounts) Close() {
	a.pool.Close()
}

// CreateUser implements account.Store.
func (a *Accounts) CreateUser(ctx context.Context, u account.User, keyHash []byte) error {
	return pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO users (id, email, created_at) VALUES ($1, $2, $3)`,
			u.ID, u.Email, u.CreatedAt)
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == uniqueViolation {
			return account.ErrEmailTaken
		}
		if err != nil {
			return fmt.Errorf("insert user: %w", err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO api_keys (key_hash, user_id, created_at) VALUES ($1, $2, $3)`,
			keyHash, u.ID, u.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert api key: %w", err)
		}
		return nil
	})
}

// UserByKeyHash implements account.Store.
func (a *Accounts) UserByKeyHash(ctx context.Context, keyHash []byte) (account.User, error) {
	var u account.User
	err := a.pool.QueryRow(ctx, `
		SELECT u.id::text, u.email, u.created_at
		FROM api_keys k JOIN users u ON u.id = k.user_id
		WHERE k.key_hash = $1 AND k.revoked_at IS NULL`, keyHash,
	).Scan(&u.ID, &u.Email, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return account.User{}, account.ErrNotFound
	}
	if err != nil {
		return account.User{}, fmt.Errorf("select user by key: %w", err)
	}
	return u, nil
}
