#!/usr/bin/env bash
# Builds and starts the real service, walks through the main user journey
# with curl, then checks for a clean shutdown. Storage is configured by the
# usual environment variables (STORAGE, REDIS_ADDR, POSTGRES_DSN, ...); with
# none set, everything runs in memory. With KAFKA_BROKERS and CLICKHOUSE_DSN
# set, the analytics consumer runs too and click statistics are checked.
set -euo pipefail

port="${SMOKE_PORT:-18181}"
base="http://127.0.0.1:$port"
work="$(mktemp -d)"
log="$work/server.log"

analytics=false
if [ -n "${KAFKA_BROKERS:-}" ] && [ -n "${CLICKHOUSE_DSN:-}" ]; then
	analytics=true
fi

# With SMOKE_COVERDIR set, binaries are built with coverage instrumentation
# and write their coverage there on exit (used by scripts/coverage.sh).
build=(go build)
if [ -n "${SMOKE_COVERDIR:-}" ]; then
	# atomic matches the mode `go test -race` uses, so the data can be merged.
	build=(go build -cover -covermode=atomic -coverpkg=./...)
	export GOCOVERDIR="$SMOKE_COVERDIR"
fi

"${build[@]}" -o "$work/shortener" ./cmd/shortener
ADDR=":$port" BASE_URL="$base" "$work/shortener" >"$log" 2>&1 &
pid=$!
consumer_pid=""
if $analytics; then
	"${build[@]}" -o "$work/consumer" ./cmd/consumer
	# A fresh consumer group per run, so old events do not delay this one.
	KAFKA_GROUP="smoke-$(date +%s)$RANDOM" FLUSH_INTERVAL=200ms "$work/consumer" >"$work/consumer.log" 2>&1 &
	consumer_pid=$!
fi
trap 'kill "$pid" $consumer_pid 2>/dev/null || true; rm -rf "$work"' EXIT

fail() {
	echo "smoke: FAIL: $*"
	echo "--- server log"
	cat "$log"
	if [ -f "$work/consumer.log" ]; then
		echo "--- consumer log"
		cat "$work/consumer.log"
	fi
	exit 1
}

# expect NAME WANT GOT
expect() {
	[ "$3" = "$2" ] || fail "$1: got '$3', want '$2'; last response body: $(cat "$work/body" 2>/dev/null)"
	echo "smoke: ok   $1"
}

for _ in $(seq 1 60); do
	curl -sf "$base/healthz" >/dev/null 2>&1 && break
	kill -0 "$pid" 2>/dev/null || fail "server exited during startup"
	sleep 0.5
done
curl -sf "$base/healthz" >/dev/null || fail "server is not healthy after 30s"

run="$(date +%s)$RANDOM"
alias="smoke$run"
# The body of the last request is kept for failure messages.
status() { curl -s -o "$work/body" -w '%{http_code}' "$@"; }
location() { curl -s -o "$work/body" -w '%{redirect_url}' "$@"; }

body="$(curl -s -X POST "$base/api/v1/users" -d "{\"email\":\"smoke-$run@example.com\"}")"
key="$(printf '%s' "$body" | sed -n 's/.*"api_key":"\([^"]*\)".*/\1/p')"
expect "register returns an api key" "sk_" "${key:0:3}"
auth=(-H "Authorization: Bearer $key")

# JSON bodies are built outside $(...): bash 3.2 (macOS) brace-expands
# "{a,b}" inside a command substitution in double quotes, splitting the body.
create_body="{\"long_url\":\"https://example.com/v1\",\"alias\":\"$alias\"}"
expect "create link" 201 "$(status -X POST "$base/api/v1/links" "${auth[@]}" -d "$create_body")"
expect "redirect status" 301 "$(status "$base/$alias")"
expect "redirect target" "https://example.com/v1" "$(location "$base/$alias")"
expect "patch link" 200 "$(status -X PATCH "$base/api/v1/links/$alias" "${auth[@]}" -d '{"long_url":"https://example.com/v2"}')"
expect "redirect follows the patch" "https://example.com/v2" "$(location "$base/$alias")"
listed="$(curl -s "$base/api/v1/links" "${auth[@]}" | grep -c "\"key\":\"$alias\"" || true)"
expect "link is listed" 1 "$listed"
if $analytics; then
	# Three redirects above: each one is a click event. The pipeline is
	# asynchronous (Kafka -> consumer -> ClickHouse), so poll.
	clicks=""
	for _ in $(seq 1 120); do
		clicks="$(curl -s "$base/api/v1/links/$alias/stats" "${auth[@]}" | sed -n 's/.*"total_clicks":\([0-9]*\).*/\1/p')"
		[ "$clicks" = 3 ] && break
		sleep 0.5
	done
	expect "stats count the 3 clicks within a minute" 3 "$clicks"
fi
expect "anonymous create" 201 "$(status -X POST "$base/api/v1/links" -d '{"long_url":"https://example.com/anon"}')"
expect "list without key" 401 "$(status "$base/api/v1/links")"
expect "delete link" 204 "$(status -X DELETE "$base/api/v1/links/$alias" "${auth[@]}")"
expect "redirect after delete" 410 "$(status "$base/$alias")"
expect "unknown key" 404 "$(status "$base/nosuchkey$run")"

kill -INT "$pid"
code=0
wait "$pid" || code=$?
expect "graceful shutdown exit code" 0 "$code"
if $analytics; then
	kill -INT "$consumer_pid"
	code=0
	wait "$consumer_pid" || code=$?
	expect "consumer graceful shutdown exit code" 0 "$code"
	if grep -q '"level":"ERROR"' "$work/consumer.log"; then
		fail "errors in the consumer log"
	fi
fi
trap 'rm -rf "$work"' EXIT
grep -q '"msg":"shutting down"' "$log" || fail "no shutdown message in the log"
if grep -q '"level":"ERROR"' "$log"; then
	fail "errors in the server log"
fi
echo "smoke: ok   no errors in the server log"
