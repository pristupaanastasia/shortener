.PHONY: build run test test-integration coverage smoke smoke-full lint check up down hooks

build:
	go build -o bin/shortener ./cmd/shortener

run:
	go run ./cmd/shortener

test:
	go test -race -count=1 ./...

# Needs the infrastructure from `make up`.
test-integration:
	go test -race -count=1 -tags integration ./...

# Unit + integration tests with a total coverage gate (COVERAGE_MIN, default
# 80%). Needs the infrastructure from `make up`.
coverage:
	./scripts/coverage.sh

# Start the real service and walk through the main user journey with curl.
smoke:
	./scripts/smoke.sh

# The same against the full stack from `make up`.
smoke-full:
	STORAGE=cassandra REDIS_ADDR=127.0.0.1:6379 \
	POSTGRES_DSN="postgres://shortener:shortener@127.0.0.1:5432/shortener?sslmode=disable" \
	./scripts/smoke.sh

# gofmt -l prints unformatted files; fail if there are any.
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "unformatted files:"; echo "$$unformatted"; exit 1; fi
	go vet -tags integration ./...

# Everything that must pass before a commit.
check: lint test

up:
	docker compose up -d --wait

down:
	docker compose down

# Use the versioned git hooks from .githooks (pre-push: lint + tests).
hooks:
	git config core.hooksPath .githooks
