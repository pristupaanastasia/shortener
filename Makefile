.PHONY: build run test lint check up down

build:
	go build -o bin/shortener ./cmd/shortener

run:
	go run ./cmd/shortener

test:
	go test -race -count=1 ./...

# gofmt -l prints unformatted files; fail if there are any.
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then echo "unformatted files:"; echo "$$unformatted"; exit 1; fi
	go vet ./...

# Everything that must pass before a commit.
check: lint test

up:
	docker compose up -d --wait

down:
	docker compose down
