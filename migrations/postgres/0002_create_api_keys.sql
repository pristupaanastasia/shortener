-- Only the SHA-256 of a key is stored, never the key itself.
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash   bytea PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES users (id),
    created_at timestamptz NOT NULL,
    revoked_at timestamptz
);
