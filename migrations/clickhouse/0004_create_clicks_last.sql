-- The time of the latest click per link, for the stats `updated_at`. It is
-- derived from clicks but, unlike clicks, has no TTL: after 90 days the raw
-- rows are gone while the aggregates (and this) remain.
CREATE TABLE IF NOT EXISTS clicks_last (
    key     String,
    last_ts SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW IF NOT EXISTS clicks_last_mv TO clicks_last AS
SELECT key, max(ts) AS last_ts
FROM clicks
GROUP BY key;

-- The view only sees new inserts, so backfill existing rows. Re-running it
-- is harmless: the maximum of maximums does not change.
INSERT INTO clicks_last
SELECT key, max(ts) AS last_ts
FROM clicks
GROUP BY key;
