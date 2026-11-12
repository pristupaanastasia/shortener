-- Clicks per link per day. SummingMergeTree adds up rows with the same key
-- during background merges, so reads must still use sum(clicks).
CREATE TABLE IF NOT EXISTS clicks_daily (
    key    String,
    day    Date,
    clicks UInt64
) ENGINE = SummingMergeTree
ORDER BY (key, day);

-- Filled on every insert into clicks. It does not backfill existing rows.
CREATE MATERIALIZED VIEW IF NOT EXISTS clicks_daily_mv TO clicks_daily AS
SELECT key, toDate(ts) AS day, count() AS clicks
FROM clicks
GROUP BY key, day;
