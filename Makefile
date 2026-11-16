.PHONY: build run test test-integration coverage smoke smoke-full loadtest lint check up down hooks k8s-up k8s-deploy k8s-smoke k8s-rollout k8s-down k8s-destroy

build:
	go build -o bin/shortener ./cmd/shortener
	go build -o bin/consumer ./cmd/consumer

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
	KAFKA_BROKERS=127.0.0.1:9092 \
	CLICKHOUSE_DSN="clickhouse://shortener:shortener@127.0.0.1:9000/shortener" \
	./scripts/smoke.sh

# Load test against the `make up` stack; fails if the latency requirements
# are not met. Tune with REDIRECT_RATE, CREATE_RATE, DURATION, LINKS.
loadtest:
	./scripts/loadtest.sh

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

# Local Kubernetes (minikube profile "shortener"); the dependencies come from
# `make up`. See scripts/k8s.sh.
k8s-up:
	./scripts/k8s.sh up

k8s-deploy:
	./scripts/k8s.sh deploy

k8s-smoke:
	./scripts/k8s.sh smoke

k8s-rollout:
	./scripts/k8s.sh rollout

k8s-down:
	./scripts/k8s.sh down

k8s-destroy:
	./scripts/k8s.sh destroy
