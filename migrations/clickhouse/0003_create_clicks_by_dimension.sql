-- Clicks per link per day per dimension value (country, device, referrer):
-- one table instead of three, with the dimension as part of the key.
CREATE TABLE IF NOT EXISTS clicks_by_dimension (
    key       String,
    day       Date,
    dimension LowCardinality(String),
    value     String,
    clicks    UInt64
) ENGINE = SummingMergeTree
ORDER BY (key, dimension, value, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS clicks_by_dimension_mv TO clicks_by_dimension AS
SELECT key, toDate(ts) AS day, dim.1 AS dimension, dim.2 AS value, count() AS clicks
FROM clicks
ARRAY JOIN [('country', country), ('device', device), ('referrer', referrer)] AS dim
GROUP BY key, day, dimension, value;
