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
)

const schema = `
CREATE TABLE IF NOT EXISTS users (
	id         uuid PRIMARY KEY,
	email      text NOT NULL UNIQUE,
	created_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS api_keys (
	key_hash   bytea PRIMARY KEY,
	user_id    uuid NOT NULL REFERENCES users (id),
	created_at timestamptz NOT NULL,
	revoked_at timestamptz
);`

// uniqueViolation is the PostgreSQL error code for a UNIQUE conflict.
const uniqueViolation = "23505"

// Accounts implements account.Store.
type Accounts struct {
	pool *pgxpool.Pool
}

// Open connects to PostgreSQL and creates the tables if they do not exist.
func Open(ctx context.Context, dsn string) (*Accounts, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Accounts{pool: pool}, nil
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
