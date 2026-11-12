# URL Shortener

A learning project: system design of a URL shortening service (like bit.ly / TinyURL) and its implementation in Go.

## Functional Requirements

### Core

1. **Create a short link.** A user submits a long URL and gets a short one (`sho.rt/abc123`).
2. **Redirect.** `GET /{key}` redirects to the original URL with **301 Moved Permanently** and a short browser-only cache: `Cache-Control: private, max-age=N`.
   - `N = min(90, seconds until expires_at)`, so a browser never caches a link longer than it lives.
   - `private` forbids CDNs and proxies from caching the redirect.
   - 301 is SEO-friendly and saves repeated round-trips, while the short `max-age` keeps analytics, editing, deletion and expiration accurate: changes reach clients within at most 90 seconds.

### Extended

3. **Custom alias.** Optionally, a user can choose their own key (`sho.rt/sale2026`).
   - Aliases are unique: if one is already taken, the service returns `409 Conflict`.
   - Aliases and generated keys share the same namespace.
   - Validation: 4–30 characters, allowed characters `[a-zA-Z0-9_-]`, reserved words (`api`, `admin`, `login`, ...) are forbidden.
4. **Link expiration.** Optionally, an `expires_at` can be set.
   - For an expired link, the service returns `410 Gone`.
   - Expired records are removed by a background job or via TTL at the database level.
   - The cache TTL never exceeds `expires_at`.
5. **Click analytics.** Track click count, time, country, device and referrer.
   - Collection is asynchronous: the redirect does not wait for analytics to be written.
   - Events are sent to a queue (Kafka), aggregated and stored in an analytics store (ClickHouse).
6. **Accounts.**
   - Links can be created anonymously.
   - Registered users can see their links and their statistics.
   - Authentication via API key or JWT.
   - Rate limiting, stricter for anonymous users (per IP).
7. **Deletion and editing.** The owner can delete a link or change its target URL.
   - Anonymous links cannot be modified.
   - On edit or deletion, the server-side cache entry is invalidated immediately; browsers pick up the change once their `max-age` (≤ 90s) expires.
   - Deletion is soft: keys are never reused.
8. **Shortening the same URL twice** creates a new key (no deduplication), so each link has its own owner, statistics and expiration.

### Out of Scope

- Link previews, QR codes
- Phishing and malware URL checks
- Team accounts and roles

## Non-Functional Requirements

1. **High availability.** Redirects must keep working even if some nodes fail (target: 99.99%). A broken redirect breaks every place the link was published.
2. **Low latency.** Redirect p99 < **100 ms**. Link creation can be slower (p99 < 500 ms).
3. **Consistency.**
   - Redirects: eventual consistency is acceptable. A new or edited link may take a few seconds to propagate.
   - Key and alias uniqueness: strong. Two links must never get the same key.
4. **Durability.** Once a link is created, it is not lost until it expires or is deleted.
5. **Unpredictable keys.** Keys must not be sequential, so links cannot be enumerated by guessing neighboring keys.
6. **Scalability.** The system scales horizontally to handle read-heavy traffic (100:1) and growing storage.

## Capacity Estimation

### Assumptions

- **100M** new links per day
- Read:write ratio **100:1**
- Links are stored for **5 years**
- One record ≈ **500 bytes** (long URL, key, user_id, timestamps)
- One analytics event ≈ **200 bytes** (key, timestamp, IP, user agent, referrer)
- One day ≈ **10⁵ seconds** (actually 86,400; rounded for easier math)
- Peak traffic ≈ **3×** average

### Throughput

| | Average | Peak |
|---|---|---|
| Write | 100M / 10⁵ s = **1K RPS** | **3K RPS** |
| Read | 1K × 100 = **100K RPS** | **300K RPS** |
| Analytics events | **100K/s** | **300K/s** |

### Storage

- Records over 5 years: 100M × 365 × 5 ≈ **200B**
- Size: 200B × 500 B = **100 TB** (~**300 TB** with 3× replication)
- Daily growth: 100M × 500 B = **50 GB/day**

### Key Length

Keys use base62 (`[a-zA-Z0-9]`) and must cover ≥ 200B links:

| Length | Combinations | Enough? |
|---|---|---|
| 6 | 62⁶ ≈ 57B | no |
| 7 | 62⁷ ≈ 3.5T | yes, ~17× headroom |

**Key length: 7 characters.**

### Cache

- 80/20 rule: 20% of links serve 80% of traffic.
- Upper bound: 10B reads/day × 20% × 500 B = **1 TB**.
- In practice much less, since most reads hit the same hot keys: tens to hundreds of GB (a small Redis cluster).

### Bandwidth

- Write: 1K RPS × 500 B = **0.5 MB/s**
- Read: 100K RPS × 500 B = **50 MB/s**

The network is not a bottleneck.

### Analytics

- Stream: 100K events/s × 200 B = **20 MB/s**
- Raw data: 10B events × 200 B = **2 TB/day** (~200 GB/day with ~10× ClickHouse compression)
- Raw events are kept for a limited period (e.g. 90 days); after that, only aggregates.

### Design Implications

1. **Read-heavy (100:1):** a cache in front of the database and read replicas are required.
2. **100 TB of data:** a single database node is not enough, so sharding is needed. A key-value model (Cassandra, DynamoDB) or sharded Postgres fits well.
3. **3K write RPS:** modest, but key generation must be collision-free and must not become a bottleneck.
4. **Analytics outgrows the main data (2 TB vs 50 GB per day):** it gets a separate pipeline (Kafka → ClickHouse), isolated from the main database.

## API

### Conventions

- **REST + JSON.**
- **Two path spaces:** `sho.rt/{key}` for redirects (root of the domain, so links stay short) and `sho.rt/api/v1/...` for link management. This is why `api` is a reserved alias. In production, they can live on separate domains (`sho.rt` and `api.sho.rt`) and scale independently.
- **Versioning** via the path (`/v1/`). Breaking changes go to `/v2/`.
- **Authentication:** `Authorization: Bearer <api_key>`. Optional for link creation (anonymous), required for everything else.
- **Error format:**
  ```json
  {
    "error": {
      "code": "alias_taken",
      "message": "Alias 'sale2026' is already in use"
    }
  }
  ```
  `code` is for programs, `message` is for humans.
- **Rate limiting:** every API response reports the client's quota, so clients can slow down before hitting the limit:
  ```
  X-RateLimit-Limit: 100          # requests allowed per window
  X-RateLimit-Remaining: 42       # requests left in the current window
  X-RateLimit-Reset: 1762516800   # unix time when the window resets
  ```
  When the limit is exceeded, the service returns `429 Too Many Requests` with `Retry-After: <seconds>`.

### Endpoints

| Method | Path | Auth | Requirement |
|---|---|---|---|
| POST | `/api/v1/links` | optional | 1, 3, 4, 8 |
| GET | `/{key}` | none | 2 |
| GET | `/api/v1/links/{key}` | required | 6 |
| GET | `/api/v1/links` | required | 6 |
| PATCH | `/api/v1/links/{key}` | required | 7 |
| DELETE | `/api/v1/links/{key}` | required | 7 |
| GET | `/api/v1/links/{key}/stats` | required | 5 |

User registration and API key management are standard auth and are not detailed here.

#### Create a link

```
POST /api/v1/links
Authorization: Bearer <api_key>     (optional)
Idempotency-Key: <uuid>             (optional)
```
```json
{
  "long_url": "https://example.com/very/long/url",
  "alias": "sale2026",
  "expires_at": "2027-01-01T00:00:00Z"
}
```
`alias` and `expires_at` are optional.

**201 Created**
```json
{
  "key": "sale2026",
  "short_url": "https://sho.rt/sale2026",
  "long_url": "https://example.com/very/long/url",
  "expires_at": "2027-01-01T00:00:00Z",
  "created_at": "2026-11-07T12:00:00Z"
}
```

| Code | When |
|---|---|
| 400 | invalid URL or alias, or `expires_at` in the past |
| 401 | invalid API key |
| 409 | alias already taken |
| 429 | rate limit exceeded |

`Idempotency-Key`: since URLs are not deduplicated, a retried request (e.g. after a timeout) would create a second link. The server stores the key (e.g. for 24 hours) and returns the original response on retries.

#### Redirect

```
GET /{key}
```

**301 Moved Permanently**
```
Location: https://example.com/very/long/url
Cache-Control: private, max-age=90
```

| Code | When |
|---|---|
| 404 | key not found |
| 410 | link expired or deleted |

Errors are rendered as HTML pages, since this endpoint is opened in a browser.

#### Get a link

```
GET /api/v1/links/{key}
Authorization: Bearer <api_key>
```

**200 OK:** the same object as on creation.

| Code | When |
|---|---|
| 401 | missing API key |
| 404 | link not found **or owned by another user** |

Returning 404 instead of 403 for other users' links avoids revealing that the link exists.

#### List my links

```
GET /api/v1/links?limit=50&cursor=<opaque>
Authorization: Bearer <api_key>
```

**200 OK**
```json
{
  "items": [ { "...": "link" } ],
  "next_cursor": "eyJjcmVhdGVkX2F0Ijo..."
}
```

Cursor pagination instead of offset: `OFFSET N` makes the database scan and discard N rows and shifts when new links are inserted. A cursor (encoded `created_at + key` of the last item) becomes `WHERE (created_at, key) < (...) LIMIT 50`, which is fast at any depth and stable.

#### Update a link

```
PATCH /api/v1/links/{key}
Authorization: Bearer <api_key>
```
```json
{
  "long_url": "https://example.com/new",
  "expires_at": null
}
```
All fields are optional; `"expires_at": null` makes the link permanent. The key itself cannot be changed: it has already been shared.

**200 OK:** the updated object.

| Code | When |
|---|---|
| 400 | invalid fields |
| 401 | missing API key |
| 404 | link not found or owned by another user |

#### Delete a link

```
DELETE /api/v1/links/{key}
Authorization: Bearer <api_key>
```

**204 No Content.** Soft delete: the redirect returns 410 afterwards. Repeated DELETE also returns 204 (idempotent).

#### Link statistics

```
GET /api/v1/links/{key}/stats?from=2026-11-01&to=2026-11-07&granularity=day
Authorization: Bearer <api_key>
```

**200 OK**
```json
{
  "total_clicks": 15230,
  "timeseries": [
    { "date": "2026-11-01", "clicks": 2100 },
    { "date": "2026-11-02", "clicks": 1980 }
  ],
  "by_country":  [ { "value": "DE", "clicks": 5100 } ],
  "by_device":   [ { "value": "mobile", "clicks": 9800 } ],
  "by_referrer": [ { "value": "twitter.com", "clicks": 4200 } ],
  "updated_at": "2026-11-07T11:58:00Z"
}
```

`updated_at` shows data freshness: analytics is asynchronous (Kafka → ClickHouse) and lags by minutes.

## Data Model

### Access Patterns

| Query | Frequency | Lookup |
|---|---|---|
| Redirect | 100K RPS | by `key` |
| Create | 1K RPS | insert + `key` uniqueness check |
| Get / Update / Delete | rare | by `key` |
| List my links | rare | by `user_id`, sorted by `created_at` |
| Statistics | rare | by `key` + time range, aggregates |

### Storage Choice

| Data | Storage | Why |
|---|---|---|
| Links | **Cassandra** | 100 TB scale, key-based access, built-in sharding and replication |
| Users and API keys | **PostgreSQL** | small dataset, needs transactions and `UNIQUE(email)` |
| Cache, rate limits, idempotency keys | **Redis** | hot data in memory, native TTL |
| Analytics | **ClickHouse** | aggregations over billions of rows |

**Why Cassandra for links:**

- Reads by partition key: the key's hash points directly to the right node.
- Horizontal scaling out of the box: add nodes and data is rebalanced automatically.
- Replication (RF=3) with no single master: losing a node does not stop the system.
- Tunable consistency per query (`ONE`, `QUORUM`, `ALL`) matches our requirements: eventual for redirects, stronger where needed.
- Data lives on disk, so 100 TB is affordable.

Redis is used for what it does best: caching hot links, rate limiting counters and idempotency keys. Losing cached data is fine, since it can be reloaded from Cassandra.

**Alternative:** sharded PostgreSQL (manually or via Citus) also works. It offers `UNIQUE` constraints and transactions, but sharding, failover and resharding must be operated manually.

### Schema

```sql
-- redirect, get, update, delete
CREATE TABLE links (
    key         text PRIMARY KEY,
    long_url    text,
    user_id     uuid,        -- null for anonymous links
    created_at  timestamp,
    expires_at  timestamp,   -- null = never expires
    deleted_at  timestamp    -- soft delete
);

-- list my links with cursor pagination
CREATE TABLE links_by_user (
    user_id     uuid,
    created_at  timestamp,
    key         text,
    long_url    text,
    expires_at  timestamp,
    PRIMARY KEY ((user_id), created_at, key)
) WITH CLUSTERING ORDER BY (created_at DESC, key ASC);
```

- `links_by_user` uses `user_id` as the **partition key** (all of a user's links live on one node) and `created_at, key` as the **clustering key** (rows are pre-sorted, which is exactly the API cursor).
- This is **query-first modeling**: one table per query. Each link is written to both tables (denormalization is normal in Cassandra).

### Cassandra Pitfalls

1. **No `UNIQUE` constraint.** An insert with an existing key silently overwrites the old row.
   - Custom aliases use a Lightweight Transaction: `INSERT ... IF NOT EXISTS` (Paxos-based, ~4× slower than a regular write; acceptable at 1–3K write RPS).
   - Generated keys use the same check: they are random, so collisions are possible (see [Deep Dive: Key Generation](#deep-dive-key-generation)).
2. **No queries by arbitrary columns.** `WHERE user_id = ?` on `links` would hit every node, which is why `links_by_user` exists.
3. **Conditional batches are limited to one partition of one table.** `INSERT ... IF NOT EXISTS` into `links` cannot be batched with the `links_by_user` insert, so the two writes are sequential: the LWT into `links` decides whether the key is taken, then `links_by_user` is written with a plain insert. Verified by an integration test: without `IF NOT EXISTS`, 20 of 20 concurrent inserts of the same key "succeed", silently overwriting each other.
4. **Native TTL is not used.** After a row expires, the redirect would return 404 instead of 410, and the alias would become free, so old links could point to someone else's content. Instead, `expires_at` is checked on read, and expired links are archived by a background job.

## High-Level Design

```
                         ┌──────────┐
                         │ Clients  │  browsers, mobile apps, API clients
                         └────┬─────┘
                              │
                     ┌────────▼────────┐
                     │  Load Balancer  │
                     └───┬─────────┬───┘
            sho.rt/{key} │         │ sho.rt/api/v1/*
                         │   ┌─────▼───────┐       ┌────────────┐
                         │   │ API Gateway │──────▶│ PostgreSQL │ users, api keys
                         │   │ auth, rate  │       └────────────┘
                         │   │ limit       │──┐
                         │   └──┬───────┬──┘  │
                         │      │       │     │
              ┌──────────▼──┐ ┌─▼─────────┐ ┌─▼────────────┐
              │  Redirect   │ │   Link    │ │    Stats     │
              │  Service    │ │  Service  │ │   Service    │
              └──┬───────┬──┘ └─┬───┬───┬─┘ └──────┬───────┘
                 │       │      │   │   │          │
                 │  ┌────▼──────▼┐  │ ┌─▼───────┐  │
                 │  │   Redis    │  │ │   Key   │  │
                 │  │   cache    │  │ │Generator│  │
                 │  └────┬───────┘  │ └─────────┘  │
                 │       │ miss     │              │
                 │  ┌────▼──────────▼┐             │
                 │  │   Cassandra    │◀── Cleanup  │
                 │  │ links,         │    Worker   │
                 │  │ links_by_user  │             │
                 │  └────────────────┘             │
                 │                                 │
              ┌──▼─────┐   ┌───────────┐   ┌───────▼────┐
              │ Kafka  │──▶│ Analytics │──▶│ ClickHouse │
              │ clicks │   │ Consumers │   └────────────┘
              └────────┘   └───────────┘
```

### Components

| Component | Responsibility |
|---|---|
| **Load Balancer** | distributes traffic, terminates TLS, routes redirects and API separately |
| **API Gateway** | API key auth, rate limiting (counters in Redis), idempotency |
| **Redirect Service** | `GET /{key}` only: the most loaded and the simplest service |
| **Link Service** | create, read, update and delete links |
| **Key Generator** | generates 7-character keys (a library inside the Link Service, not a separate deployment) |
| **Stats Service** | serves statistics from ClickHouse |
| **Redis** | link cache, rate limit counters, idempotency keys |
| **Cassandra** | primary link storage |
| **PostgreSQL** | users and API keys |
| **Kafka** | click event queue |
| **Analytics Consumers** | enrich events (country from IP, device from User-Agent) and batch-insert into ClickHouse |
| **Cleanup Worker** | archives expired links in the background |

All services are **stateless**: state lives only in the storage layer, so services scale horizontally by adding instances behind the load balancer.

**Why Redirect and Link are separate services:**

- **Different load:** 100K vs 1K RPS, so they scale independently.
- **Failure isolation:** bugs or overload in the API must not break redirects, which millions of people rely on.
- **Simplicity:** minimal code in the Redirect Service means fewer reasons to fail.

### Flow 1: Redirect (hot path)

1. **Load Balancer** routes `GET sho.rt/abc1234` directly to the Redirect Service, bypassing the API Gateway (no auth needed, every extra hop adds latency).
2. **Cache lookup**, two levels:
   - **L1:** an in-process LRU (100K entries, TTL 10s). A viral link is served from memory on every instance instead of hammering a single Redis shard (hot key);
   - **L2:** Redis `GET link:abc1234`;
   - **hit** (most requests): go to step 4; **miss**: go to step 3.
3. **Cassandra** `SELECT ... WHERE key = 'abc1234'` with consistency `QUORUM`. With `QUORUM` writes, `W + R > N` guarantees the read sees the latest write; with `ONE`, a miss could load a stale replica into the cache for an hour. Misses are a small fraction of traffic, so the extra cost is affordable.
   - Concurrent misses for the same key on one instance are collapsed into one Cassandra read (`singleflight`), which prevents a cache stampede when a hot key expires.
   - The result is cached in Redis with `TTL = 1h + random(0, 10m)`. The jitter keeps entries that were cached together from expiring together (cache avalanche). Expired and deleted links are cached too: the service checks `expires_at`/`deleted_at` on every read, so 410 is served from cache.
   - **Negative caching:** a missing key is cached as "not found" for 1 minute. Otherwise a bot enumerating random keys would bypass the cache and flood Cassandra. Creating a link drops its negative entry, so a new alias resolves immediately (other instances' L1 may serve 404 for up to 10s).
   - **Redis failures are treated as misses**, never as errors. The Redis client uses tight timeouts and minimal retries: with library defaults, an unavailable Redis added ~5 s to every request (measured in tests).
4. **Checks and response:**
   - `deleted_at` set or `expires_at` in the past: **410**;
   - not found: **404**;
   - otherwise: **301** + `Location` + `Cache-Control: private, max-age=N`.
5. **Click event to Kafka**, asynchronously. The response does not wait for Kafka: the producer buffers events in memory and sends them in batches. If Kafka is unavailable, the event is dropped: redirects are more important than analytics.

**Latency** (measured locally): L1 hit ~0.3 ms, cache miss to Cassandra ~5 ms, Redis down ~10 ms on the first miss, then L1 again. Well within the 100 ms budget.

**Known limitations:**

- A Redis that hangs (accepts connections but does not respond) costs up to the 50 ms timeout per operation. A circuit breaker would skip Redis entirely after repeated failures.
- While Redis is down, every miss logs a warning; at production traffic, these logs need rate limiting.

### Flow 2: Create a link

1. **Load Balancer → API Gateway:**
   - API key check (PostgreSQL, cached in Redis);
   - rate limit: `INCR` a counter in Redis, return 429 when exceeded;
   - if `Idempotency-Key` is present: `SET NX` in Redis; if the key already exists, return the stored response.
2. **Link Service:**
   - URL validation: `http`/`https` scheme, length, not pointing to our own domain (prevents redirect loops);
   - **with alias:** `INSERT ... IF NOT EXISTS` (LWT); 409 if the row already exists;
   - **without alias:** generate a key and insert it with `IF NOT EXISTS`; on collision, retry with a new key.
3. **Cassandra:** insert into `links` with `IF NOT EXISTS` (this is what guarantees key uniqueness), then, for links with an owner, a plain insert into `links_by_user`. Both with consistency `QUORUM` (2 of 3 replicas) to guarantee durability. They cannot be combined into one batch: Cassandra rejects conditional batches that span tables.
4. Respond **201**.

The cache is **not warmed** on creation: most links never become hot, and the first click populates the cache.

### Flow 3: Update and delete

1. API Gateway: auth, rate limit.
2. Link Service checks that the link's `user_id` matches the API key owner; otherwise 404.
3. Update both Cassandra tables with `QUORUM`.
4. Invalidate the cache with a **delayed double delete**: `DEL link:abc1234` in Redis and L1, and again after ~1 s. The second delete removes a stale value written back by a concurrent reader that loaded it from Cassandra before the update. Other instances' L1 entries expire within 10 s.
5. Browsers pick up the change within ≤ 90 seconds (`max-age`).

This is the **cache-aside** pattern: the application reads the cache, falls back to the database on a miss and invalidates the cache on writes.

### Flow 4: Analytics

1. The Redirect Service publishes `{key, timestamp, ip, user_agent, referrer}` to the Kafka topic `clicks`, **partitioned by `key`** (all clicks of a link go to the same partition).
2. **Analytics Consumers** read in batches:
   - IP → country (local GeoIP database);
   - User-Agent → device type;
   - **the IP is then discarded**: it is personal data and is not stored without a reason (GDPR).
3. Events are inserted into ClickHouse in batches of ~10K rows (ClickHouse handles large inserts well and small ones poorly).
4. **Materialized views** in ClickHouse maintain daily and per-country aggregates; the Stats Service reads those instead of raw events.

Kafka acts as a **buffer**: if ClickHouse is slow or down, events accumulate in Kafka (retained e.g. for 7 days) and are written later. Nothing is lost and redirects are unaffected.

### Flow 5: Expired links

The **Cleanup Worker** runs daily, finds links with `expires_at` in the past and moves them to cheap cold storage (e.g. S3). A **tombstone row** (key + `deleted_at`) stays in Cassandra, so:

- the redirect returns 410, not 404;
- the key or alias is never reused.

### Consistency Between Stores (Dual Writes)

| Where | Problem | Solution |
|---|---|---|
| `links` + `links_by_user` | two tables; the `links` insert is an LWT, and Cassandra does not allow conditional batches across tables | LWT into `links` first, then an idempotent insert into `links_by_user`; if the second write fails, the link still works and the request is retried (with `Idempotency-Key`) |
| Create → Redis | none: the cache is populated lazily | — |
| Clicks → Kafka | no database write involved; losing an event is acceptable | async fire-and-forget |
| Update/delete → `DEL` in Redis | real dual write: if `DEL` fails, the cache serves the old URL until TTL | retry with backoff + cache TTL capped at 1 hour (or CDC-based invalidation) |

**Transactional outbox is not needed here.** It solves a different problem: an event that **must not be lost** and is produced by a database write (e.g. "link created" for a phishing scanner or billing). If such an event appears, it would be written to an `outbox` table and relayed to Kafka (since the `links` insert is an LWT, it cannot share a batch with it either). With Cassandra, CDC is usually preferred over an outbox table, since processed and deleted rows leave tombstones that slow down reads.

### Open Questions for the Deep Dive

1. **Key Generator:** how to generate unique, unpredictable keys across many servers without per-request coordination? See [Deep Dive: Key Generation](#deep-dive-key-generation).
2. **Cache:** hot keys (a viral link), cache stampede, the race between invalidation and a concurrent read.
3. **Rate limiting:** which algorithm, and where to keep counters.
4. **Multi-region:** what happens if a whole data center goes down?

## Deep Dive: Key Generation

### Requirements

1. **Unique:** two links never share a key.
2. **7 characters**, base62.
3. **Unpredictable:** neighboring links cannot be enumerated.
4. **Fast:** 3K RPS at peak.
5. **No per-request coordination** between Link Service instances.

### Chosen Approach: Salted Hash + LWT + Retry

```go
func generateKey(longURL string, attempt int) string {
    // the salt makes shortening the same URL twice produce different keys (requirement 8)
    input := fmt.Sprintf("%s|%d|%d", longURL, time.Now().UnixNano(), attempt)
    sum := sha256.Sum256([]byte(input))
    n := binary.BigEndian.Uint64(sum[:8]) % 3_521_614_606_208 // 62^7
    return toBase62(n, 7)
}
```

Insertion:

```
attempt = 0
loop:
    key = generateKey(url, attempt)
    INSERT INTO links ... IF NOT EXISTS
    if not applied: attempt++, retry (up to ~5 attempts)
```

**Pros:**

- Simple: one function, no counters or extra infrastructure.
- Unpredictable: a SHA-256 of a nanosecond-salted input cannot be guessed.
- No coordination between instances.
- LWT is needed anyway for custom aliases, so the uniqueness check adds no new mechanism.

**Cons:**

- **Collisions grow over time:** the chance that a new key is taken equals `used / total`, ~1% after a year (36B / 3.5T) and ~6% after 5 years (200B / 3.5T). Each collision costs an extra LWT round-trip.
- **Every insert is an LWT** (~4× more expensive than a regular write). Acceptable at 1–3K write RPS.
- Because of the salt, the hash no longer depends only on the URL, so this is effectively a random key: `crypto/rand` would work equally well. The hash is just a source of randomness.

**Why not hash only the URL:** the same URL would always get the same key (breaks requirement 8), and truncating the hash to 7 characters still produces collisions, so a uniqueness check is required anyway.

### Collision with Custom Aliases

Aliases and generated keys share one namespace. A user may pick `hello12`, which is a valid generated key. A plain Cassandra `INSERT` would silently overwrite it, so **every** insert (alias or generated) uses `IF NOT EXISTS`.

### Alternatives Considered

| Approach | Unique | Unpredictable | Coordination | Check on write |
|---|---|---|---|---|
| **Salted hash / random (chosen)** | collisions up to ~6% | yes | none | required, with retries |
| Plain counter + base62 | yes | no | per request | not needed |
| Counter ranges + Feistel permutation | yes | yes | once per ~17 min | safety-net LWT only |
| Pre-generated keys (KGS) | yes | yes | per batch | not needed |
| Snowflake ID | yes | partially | none | — (11 characters in base62, too long) |

**Counter ranges + Feistel permutation** is the main alternative if write volume grows and per-insert LWTs or retries become a problem:

- Each instance reserves a range of IDs from a counter in PostgreSQL (`UPDATE key_counter SET next_id = next_id + 1000000 RETURNING next_id`) and hands them out from memory. At 3K RPS across 3 instances, each instance fetches a new range every ~17 minutes. A lost range after a crash is harmless given the key space.
- Before base62 encoding, the counter passes through a 40-bit Feistel network with secret round keys. Feistel is a bijection for any round function, so keys stay unique while looking random. 2⁴⁰ ≈ 1.1T fits into 7 base62 characters (62⁷ ≈ 3.5T) and covers the 200B links needed.

**Pre-generated keys (KGS)** produce the same result as counter ranges, but require storing billions of keys and tracking used and unused ones in a separate service.
