-- Raw click events, one row per redirect. Sorted by link and time, so the
-- per-link queries read a narrow range. Raw rows are kept for 90 days;
-- the aggregates below are kept forever.
CREATE TABLE IF NOT EXISTS clicks (
    key      String,
    ts       DateTime64(3, 'UTC'),
    country  LowCardinality(String),
    device   LowCardinality(String),
    referrer String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (key, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;
