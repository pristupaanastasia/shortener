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
   - Generated keys skip this check if the key generator guarantees uniqueness.
2. **No queries by arbitrary columns.** `WHERE user_id = ?` on `links` would hit every node, which is why `links_by_user` exists.
3. **Native TTL is not used.** After a row expires, the redirect would return 404 instead of 410, and the alias would become free, so old links could point to someone else's content. Instead, `expires_at` is checked on read, and expired links are archived by a background job.
