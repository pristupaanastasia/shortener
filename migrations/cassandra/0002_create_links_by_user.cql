-- "List my links": one partition per user, newest first.
CREATE TABLE IF NOT EXISTS links_by_user (
    user_id     uuid,
    created_at  timestamp,
    key         text,
    long_url    text,
    expires_at  timestamp,
    PRIMARY KEY ((user_id), created_at, key)
) WITH CLUSTERING ORDER BY (created_at DESC, key ASC);
