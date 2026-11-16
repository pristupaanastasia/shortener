-- Cleanup worker state. The lease keeps concurrent workers from repeating
-- work; its row expires by TTL if the holder dies.
CREATE TABLE IF NOT EXISTS cleanup_lease (
    name   text PRIMARY KEY,
    owner  text
);

-- The next expiry day to clean up: every earlier day is done.
CREATE TABLE IF NOT EXISTS cleanup_checkpoint (
    name  text PRIMARY KEY,
    day   date
);
