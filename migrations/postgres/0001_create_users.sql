CREATE TABLE IF NOT EXISTS users (
    id         uuid PRIMARY KEY,
    email      text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL
);
