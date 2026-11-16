-- Expired links for the cleanup worker: Cassandra cannot query links by
-- expires_at without a full scan. One partition per expiry day and bucket:
-- at 100M links/day, a day alone would be a ~6 GB partition, 256 buckets keep
-- each at ~20 MB. The table is a hint: the worker re-reads links, so a row
-- left behind by a change of expires_at is skipped, not acted on.
CREATE TABLE IF NOT EXISTS links_by_expiry (
    day         date,
    bucket      int,
    expires_at  timestamp,
    key         text,
    PRIMARY KEY ((day, bucket), expires_at, key)
);
