-- Redirects, get, update and delete: lookup by key.
CREATE TABLE IF NOT EXISTS links (
    key         text PRIMARY KEY,
    long_url    text,
    user_id     uuid,        -- null for anonymous links
    created_at  timestamp,
    expires_at  timestamp,   -- null: never expires
    deleted_at  timestamp    -- soft delete
);
