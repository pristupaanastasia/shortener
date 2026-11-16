#!/usr/bin/env bash
# Builds and starts the real service, walks through the main user journey
# with curl, then checks for a clean shutdown. Storage is configured by the
# usual environment variables (STORAGE, REDIS_ADDR, POSTGRES_DSN, ...); with
# none set, everything runs in memory. With KAFKA_BROKERS and CLICKHOUSE_DSN
# set, the analytics consumer runs too and click statistics are checked.
#
# With SMOKE_URL set, nothing is built or started: the journey runs against
# that already running service (e.g. a port-forward into Kubernetes), and the
# shutdown checks are skipped. SMOKE_ANALYTICS=1 checks click statistics and
# SMOKE_METRICS_URL the metrics endpoint in that mode.
#
# With STORAGE=cassandra and ARCHIVE_ENDPOINT set, the cleanup worker runs
# too: a link that expires during the run must be archived and tombstoned.
set -euo pipefail

port="${SMOKE_PORT:-18181}"
base="${SMOKE_URL:-http://127.0.0.1:$port}"
metrics_url="${SMOKE_METRICS_URL:-http://127.0.0.1:${SMOKE_METRICS_PORT:-$((port + 1))}}"
work="$(mktemp -d)"
log="$work/server.log"
external=false
[ -n "${SMOKE_URL:-}" ] && external=true

analytics=false
if $external; then
	[ -n "${SMOKE_ANALYTICS:-}" ] && analytics=true
elif [ -n "${KAFKA_BROKERS:-}" ] && [ -n "${CLICKHOUSE_DSN:-}" ]; then
	analytics=true
	# A topic of its own: the shared one may hold millions of events (e.g.
	# after a load test), and concurrent runs must not see each other's clicks.
	export KAFKA_TOPIC="smoke-$(date +%s)$RANDOM"
fi

# With SMOKE_COVERDIR set, binaries are built with coverage instrumentation
# and write their coverage there on exit (used by scripts/coverage.sh).
build=(go build)
if [ -n "${SMOKE_COVERDIR:-}" ]; then
	# atomic matches the mode `go test -race` uses, so the data can be merged.
	build=(go build -cover -covermode=atomic -coverpkg=./...)
	export GOCOVERDIR="$SMOKE_COVERDIR"
fi

cleanup=false
if ! $external && [ "${STORAGE:-}" = cassandra ] && [ -n "${ARCHIVE_ENDPOINT:-}" ]; then
	cleanup=true
	# A keyspace and an archive prefix of their own: the worker archives
	# every expired link it finds, and must not take development links.
	export CASSANDRA_KEYSPACE=shortener_smoke ARCHIVE_PREFIX="smoke/"
fi

pid=""
consumer_pid=""
if ! $external; then
	"${build[@]}" -o "$work/shortener" ./cmd/shortener
	# Several smoke runs per minute from 127.0.0.1 (the pre-push hook, CI,
	# concurrent runs) would exceed the default anonymous limit of 10/min.
	RATE_LIMIT_ANONYMOUS=1000 METRICS_ADDR="${metrics_url#http://}" ADDR=":$port" BASE_URL="$base" "$work/shortener" >"$log" 2>&1 &
	pid=$!
	if $cleanup; then
		"${build[@]}" -o "$work/cleanup" ./cmd/cleanup
	fi
	if $analytics; then
		"${build[@]}" -o "$work/consumer" ./cmd/consumer
		KAFKA_GROUP=smoke FLUSH_INTERVAL=200ms "$work/consumer" >"$work/consumer.log" 2>&1 &
		consumer_pid=$!
	fi
fi
# Delete the per-run topic too, so smoke runs do not pile up topics.
cleanup() {
	[ -n "$pid" ] && kill "$pid" $consumer_pid 2>/dev/null || true
	if $analytics && ! $external; then
		go run ./scripts/deltopics -brokers "$KAFKA_BROKERS" "$KAFKA_TOPIC" >/dev/null 2>&1 || true
	fi
	rm -rf "$work"
}
trap cleanup EXIT

fail() {
	echo "smoke: FAIL: $*"
	if [ -f "$log" ]; then
		echo "--- server log"
		cat "$log"
	fi
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
	[ -z "$pid" ] || kill -0 "$pid" 2>/dev/null || fail "server exited during startup"
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

if $cleanup; then
	# Created first, so it has expired by the time the worker runs.
	expiring="smokeexp$run"
	expires_at="$(date -u -v+3S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)"
	expiring_body="{\"long_url\":\"https://example.com/expiring\",\"alias\":\"$expiring\",\"expires_at\":\"$expires_at\"}"
	expect "create an expiring link" 201 "$(status -X POST "$base/api/v1/links" "${auth[@]}" -d "$expiring_body")"
fi

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
if $cleanup; then
	for _ in $(seq 1 20); do
		[ "$(status "$base/$expiring")" = 410 ] && break # expired
		sleep 0.5
	done
	# Another worker (a concurrent smoke run) may hold the lease: retry.
	archived=false
	for _ in $(seq 1 20); do
		CLEANUP_LOOKBACK_DAYS=1 "$work/cleanup" >>"$work/cleanup.log" 2>&1 || fail "cleanup failed: $(cat "$work/cleanup.log")"
		if go run ./scripts/archived "$expiring" "$expires_at"; then
			archived=true
			break
		fi
		sleep 1
	done
	expect "cleanup archives the expired link" true "$archived"
	expect "expired link still answers 410" 410 "$(status "$base/$expiring")"
	listed="$(curl -s "$base/api/v1/links" "${auth[@]}" | grep -c "\"key\":\"$expiring\"" || true)"
	expect "archived link left the listing" 0 "$listed"
	reuse_body="{\"long_url\":\"https://example.com/reuse\",\"alias\":\"$expiring\"}"
	expect "archived alias cannot be taken again" 409 "$(status -X POST "$base/api/v1/links" "${auth[@]}" -d "$reuse_body")"
fi
if ! $external || [ -n "${SMOKE_METRICS_URL:-}" ]; then
	redirects="$(curl -s "$metrics_url/metrics" | grep -c '^http_requests_total{code="301",method="GET",route="GET /{key}"}' || true)"
	expect "metrics count redirects by route pattern" 1 "$redirects"
fi

if $external; then
	exit 0 # the service is not ours to stop
fi
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
pid=""
consumer_pid=""
grep -q '"msg":"shutting down"' "$log" || fail "no shutdown message in the log"
if grep -q '"level":"ERROR"' "$log"; then
	fail "errors in the server log"
fi
echo "smoke: ok   no errors in the server log"
