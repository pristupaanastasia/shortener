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

- [x] `docker-compose.yml` with Cassandra; schema from `README.md` applied on startup
- [x] Cassandra `LinkStore`: `IF NOT EXISTS` into `links`, then `links_by_user` (a conditional batch cannot span tables, see `README.md`), `QUORUM` writes and reads
- [x] Store is selected by config (`STORAGE=memory|cassandra`)
- [x] Integration tests (`integration` build tag) against the compose Cassandra

**Done when:** `make check` passes; `make up` + integration tests pass; the service works end to end with `STORAGE=cassandra`.

## M3: Caching

- [x] Redis in `docker-compose.yml` (`allkeys-lfu`)
- [x] Redirect read path: L1 in-process LRU (TTL 10s) → Redis (TTL 1h + jitter) → store
- [x] `singleflight` on cache misses
- [x] Negative caching of missing keys (1–5 min)
- [x] Invalidation with delayed double delete (`cache.Store.Invalidate`; there is no update/delete yet, it is wired up in M4)

**Done when:** `make check` passes; tests show that concurrent misses for one key hit the store once, and that a deleted link stops resolving after invalidation.

## M4: Accounts and Link Management

- [x] PostgreSQL in `docker-compose.yml`: users and API keys
- [x] `Authorization: Bearer <api_key>` middleware (optional for create, required otherwise)
- [x] Rate limiting in Redis, `X-RateLimit-*` headers, 429 + `Retry-After`
- [x] `Idempotency-Key` support for `POST /api/v1/links`
- [x] `GET`, `PATCH`, `DELETE /api/v1/links/{key}` (404 for other users' links, soft delete); PATCH and DELETE invalidate the cache
- [x] `GET /api/v1/links` with cursor pagination from `links_by_user`

**Done when:** `make check` passes; tests cover auth, ownership, rate limit headers, idempotent retries and pagination across pages.

## Test Format Refactoring

- [x] Rewrite all existing tests in the table-driven format from the `go-table-tests` skill (cases in a slice, `for range` + `t.Run`; `steps` slices for stateful scenarios)

**Done when:** every `Test...` function follows the skill; the set of checked behaviors is unchanged (no assertion is dropped); `make check` and `make test-integration` pass.

## M5: Analytics

- [x] Kafka and ClickHouse in `docker-compose.yml`
- [x] Redirect Service publishes click events asynchronously (non-blocking, drop on failure)
- [x] Consumer: enrich (country, device), batch-insert into ClickHouse (no IP enters the pipeline: the country comes from an edge header, see `README.md`)
- [x] ClickHouse materialized views for daily / per-country / per-device / per-referrer aggregates
- [x] `GET /api/v1/links/{key}/stats` with `updated_at`

**Done when:** `make check` passes; clicking a link N times shows N clicks in `/stats` within a minute; redirects keep working when Kafka is stopped.

## M6: Load Testing and Observability

- [x] Prometheus metrics: request rate, latency histograms, cache hit ratio per level
- [x] Load test script for create and redirect (`make loadtest`: vegeta, natively; k6 in Docker was the first bottleneck)
- [x] Report: measured throughput and p99 latency, the first bottleneck found and how it was addressed

**Done when:** the load test runs with one command and the report is added to `README.md` ("Load Test Results").

## M7: Write-Path Optimizations

The first bottleneck in the code found by the M6 load test: two Redis round trips per create. Baseline at 10K creates/s: rate limiter 12.8% of service CPU, negative-cache `DEL` 8.6%, Cassandra LWT 4.0%; 1.54 cores, create p99 34 ms.

- [x] Skip the negative-cache `DEL` on create for generated keys (a random key is practically never looked up before it exists; aliases still drop their negative entry)
- [x] Approximate rate limiting: count in memory and flush increments to Redis in batches (`RATE_LIMIT_SYNC_INTERVAL`; 0 keeps the exact per-request limiter); document the over-admission bound
- [x] Measure again with the same load and update the "Load Test Results" section in `README.md`

**Done when:** `make check`, `make coverage` and `make loadtest` pass; the CPU profile at 10K creates/s shows the rate limiter and the `DEL` reduced, with before/after numbers in `README.md`.

## M8: Kubernetes

- [x] `Dockerfile`: multi-stage build, distroless non-root image with `shortener` and `consumer`
- [x] `k8s/base` (kustomize): shortener Deployment (replicas, readiness/liveness probes, resource requests/limits, graceful shutdown), Service, consumer Deployment, HorizontalPodAutoscaler, PodDisruptionBudget, config in a ConfigMap and secrets in a Secret
- [x] `k8s/overlays/local`: a minikube profile `shortener`, dependencies from `make up` reached via `host.minikube.internal`
- [x] `make k8s-up` / `k8s-deploy` / `k8s-smoke` / `k8s-down`; every kubectl call uses an explicit `--context`, never the current one
- [x] The smoke test can run against an already running service (`SMOKE_URL`), and `make k8s-smoke` runs it through a port-forward

**Done when:** `make k8s-up k8s-deploy k8s-smoke` brings the service up in minikube and the smoke test passes against it, including click statistics; a rolling restart under load loses no requests (`make k8s-rollout`).
