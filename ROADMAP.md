# Roadmap

Each milestone produces a working service. Work in order; the user reviews each milestone before the next one starts. See `README.md` for the design.

## M1: In-Memory MVP

- [x] `cmd/shortener`: HTTP server with config from env (`ADDR`, `BASE_URL`) and graceful shutdown on SIGINT/SIGTERM
- [x] `internal/keygen`: salted SHA-256 → 7-character base62 key
- [x] `internal/storage`: `LinkStore` interface + in-memory implementation with an atomic "insert if not exists"
- [x] `internal/link`: service: URL validation, alias validation (4–30 chars, `[a-zA-Z0-9_-]`, reserved words), generate-and-retry on collision
- [x] `POST /api/v1/links` with `long_url`, optional `alias` and `expires_at`
- [x] `GET /{key}`: 301 + `Cache-Control: private, max-age=N` (`N = min(90, seconds until expiry)`), 404, 410
- [x] JSON error format from `README.md`
- [x] `GET /healthz`

**Done when:** `make check` passes; tests cover keygen, validation, collision retry and every status code of both endpoints; `make run` + curl creates a link and follows the redirect.

## M2: Cassandra Storage

- [ ] `docker-compose.yml` with Cassandra; schema from `README.md` applied on startup
- [ ] Cassandra `LinkStore`: `links` + `links_by_user` in a logged batch, `IF NOT EXISTS`, `QUORUM` writes and reads
- [ ] Store is selected by config (`STORAGE=memory|cassandra`)
- [ ] Integration tests (`integration` build tag) against the compose Cassandra

**Done when:** `make check` passes; `make up` + integration tests pass; the service works end to end with `STORAGE=cassandra`.

## M3: Caching

- [ ] Redis in `docker-compose.yml` (`allkeys-lfu`)
- [ ] Redirect read path: L1 in-process LRU (TTL 10s) → Redis (TTL 1h + jitter) → store
- [ ] `singleflight` on cache misses
- [ ] Negative caching of missing keys (1–5 min)
- [ ] Invalidation on update/delete with delayed double delete

**Done when:** `make check` passes; tests show that concurrent misses for one key hit the store once, and that a deleted link stops resolving after invalidation.

## M4: Accounts and Link Management

- [ ] PostgreSQL in `docker-compose.yml`: users and API keys
- [ ] `Authorization: Bearer <api_key>` middleware (optional for create, required otherwise)
- [ ] Rate limiting in Redis, `X-RateLimit-*` headers, 429 + `Retry-After`
- [ ] `Idempotency-Key` support for `POST /api/v1/links`
- [ ] `GET`, `PATCH`, `DELETE /api/v1/links/{key}` (404 for other users' links, soft delete)
- [ ] `GET /api/v1/links` with cursor pagination from `links_by_user`

**Done when:** `make check` passes; tests cover auth, ownership, rate limit headers, idempotent retries and pagination across pages.

## M5: Analytics

- [ ] Kafka and ClickHouse in `docker-compose.yml`
- [ ] Redirect Service publishes click events asynchronously (non-blocking, drop on failure)
- [ ] Consumer: enrich (country, device), drop IP, batch-insert into ClickHouse
- [ ] ClickHouse materialized views for daily / per-country / per-device / per-referrer aggregates
- [ ] `GET /api/v1/links/{key}/stats` with `updated_at`

**Done when:** `make check` passes; clicking a link N times shows N clicks in `/stats` within a minute; redirects keep working when Kafka is stopped.

## M6: Load Testing and Observability

- [ ] Prometheus metrics: request rate, latency histograms, cache hit ratio per level
- [ ] Load test script (k6 or vegeta) for create and redirect
- [ ] Report: measured throughput and p99 latency, the first bottleneck found and how it was addressed

**Done when:** the load test runs with one command and the report is added to `README.md`.
