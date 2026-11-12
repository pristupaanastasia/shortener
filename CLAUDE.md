# CLAUDE.md

URL shortener in Go, built as a learning project for system design and agentic development.

## Key Files

- `README.md`: the design document (requirements, capacity, API, data model, architecture). It is the source of truth: implement what it describes. If the implementation needs to diverge, update `README.md` in the same change and explain why.
- `ROADMAP.md`: milestones with tasks and acceptance criteria. Work on one milestone at a time, in order.

## Commands

- `make check`: lint (`gofmt`, `go vet`) + tests with the race detector. Must pass before every commit.
- `make test`: tests only.
- `make test-integration`: unit + integration tests (`integration` build tag); needs `make up`. Run it before committing changes to storage, caches or queues.
- `make run`: start the service locally.
- `make coverage`: unit + integration tests with a total coverage minimum of 80% (`COVERAGE_MIN`); needs `make up`.
- `make smoke` / `make smoke-full`: build and start the real service, walk through the main user journey with curl, check a clean shutdown; in memory, or against the `make up` stack.
- `make hooks`: enable the git `pre-push` hook: lint + `make coverage` + `make smoke-full` for every pushed commit, so pushing requires `make up`. Run once per clone.
- CI (`.github/workflows/ci.yml`) runs the same checks on every push and pull request.
- `make up` / `make down`: start/stop infrastructure from `docker-compose.yml` (from M2 on).

## Project Layout

```
cmd/shortener/      main: config, wiring, graceful shutdown
migrations/<db>/    versioned schema migrations, embedded and applied on startup
internal/
  httpapi/          HTTP handlers, routing, middleware
  link/             domain: Link type, errors, Store interface, validation, service
  keygen/           key generation (salted hash + base62)
  storage/<impl>/   link.Store implementations (memory, cassandra, ...)
  cache/            Redis and in-process caches
  migrate/          migration loader shared by the database runners
```

New packages go under `internal/`. Keep `cmd/` thin.

## Code Conventions

- Go standard library first: `net/http` with Go 1.22+ routing patterns, `log/slog`, `context`. Add a dependency only when the stdlib has no reasonable equivalent.
- Pass `context.Context` as the first argument to anything doing I/O.
- Wrap errors with context: `fmt.Errorf("get link %q: %w", key, err)`. Use sentinel errors (`ErrNotFound`, `ErrAliasTaken`) for cases handlers map to HTTP codes.
- Depend on interfaces at package boundaries, declared by the consumer (e.g. `link.Store`), so storage and caches can be swapped and faked in tests.
- No global state; configuration comes from environment variables, parsed in `main`.
- Database schemas live only in `migrations/<db>/`; never edit a committed migration, add a new one. Every database has integration tests for its migrations and queries (the `db-migrations` skill in `.claude/skills/` has the rules).
- Tests: always table-driven, cases in a slice run with `for range` + `t.Run` (the `go-table-tests` skill in `.claude/skills/` has the exact format). `httptest` for handlers, an in-memory store for unit tests. Integration tests that need Docker use the `integration` build tag.

## Workflow

- One branch per milestone: `milestone/m<N>-<short-name>`, branched from `main`.
- Small commits, each passing `make check`. Commit messages in English, imperative mood.
- When a milestone is done, check its boxes in `ROADMAP.md` and stop: the user reviews every milestone before the next one starts. Do not merge into `main` yourself.
- If blocked (unclear requirement, failing infrastructure, a design decision not covered by `README.md`), stop and describe the problem instead of guessing.

## Communication

- Talk to the user in Russian. Code, comments, commit messages and docs are in English.
- The user is learning: when doing something non-obvious (an agentic technique or a design trade-off), briefly explain why.
