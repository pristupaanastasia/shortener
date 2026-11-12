#!/usr/bin/env bash
# Starts the service on the full stack from `make up` and runs the k6 load
# test (loadtest/load.js) in Docker. Prints the k6 summary and the cache hit
# counters. Tune with REDIRECT_RATE, CREATE_RATE, DURATION and LINKS.
set -euo pipefail

port="${LOADTEST_PORT:-18700}"
metrics_port=$((port + 1))
work="$(mktemp -d)"
go build -o "$work/shortener" ./cmd/shortener

# Rate limits are raised: the load generator is a single client.
STORAGE=cassandra REDIS_ADDR=127.0.0.1:6379 \
POSTGRES_DSN="postgres://shortener:shortener@127.0.0.1:5432/shortener?sslmode=disable" \
KAFKA_BROKERS=127.0.0.1:9092 \
RATE_LIMIT_ANONYMOUS=100000000 RATE_LIMIT_USER=100000000 \
ADDR=":$port" METRICS_ADDR=":$metrics_port" BASE_URL="http://127.0.0.1:$port" \
	"$work/shortener" >"$work/server.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; rm -rf "$work"' EXIT
for _ in $(seq 1 60); do
	curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && break
	sleep 0.5
done

# host.docker.internal reaches the host from Docker Desktop; on Linux, host
# networking does the same.
target="http://host.docker.internal:$port"
net=()
if [ "$(uname)" = Linux ]; then
	target="http://127.0.0.1:$port"
	net=(--network host)
fi
# Sample the service's CPU once a second while k6 runs, to tell whether the
# service or the load generator is the limit (100% = one core).
( while kill -0 "$pid" 2>/dev/null; do ps -o %cpu= -p "$pid"; sleep 1; done ) >"$work/cpu.txt" &
sampler=$!

code=0
# ${net[@]+...}: bash 3.2 (macOS) treats an empty array as unset under set -u.
docker run --rm ${net[@]+"${net[@]}"} -v "$PWD/loadtest:/loadtest:ro" \
	-e BASE_URL="$target" -e REDIRECT_RATE -e CREATE_RATE -e DURATION -e LINKS \
	grafana/k6:latest run --quiet /loadtest/load.js || code=$?

kill "$sampler" 2>/dev/null || true
echo "--- service CPU while loaded (100% = one core, $(getconf _NPROCESSORS_ONLN) cores):"
awk '$1 > 5 { n++; s += $1; if ($1 > m) m = $1 } END { if (n) printf "avg %.0f%%, max %.0f%%\n", s / n, m; else print "idle" }' "$work/cpu.txt"
echo "--- cache lookups by level"
curl -s "http://127.0.0.1:$metrics_port/metrics" | grep '^cache_lookups_total' || true
echo "--- errors in the server log: $(grep -c '"level":"ERROR"' "$work/server.log" || true)"
exit "$code"
