#!/usr/bin/env bash
# Load test: starts the service on the full stack from `make up` and runs
# redirects (80% of traffic on 20% of links, like the 80/20 assumption in
# README) and link creation at the same time with vegeta, natively. Checks the
# latency requirements (redirect p99 < 100 ms, create p99 < 500 ms, > 99%
# success) and exits non-zero if they are not met.
#
# Tune with REDIRECT_RATE, CREATE_RATE (per second), DURATION and LINKS; the
# limits with REDIRECT_P99_MS and CREATE_P99_MS. With PROFILE=1, a 10s CPU profile is saved to $PROFILE_OUT (default cpu.pprof).
#
# vegeta runs natively, not in Docker: k6 in Docker Desktop capped at ~23k
# requests/s and added ~15 ms to p99 through the virtual network.
set -euo pipefail

redirect_rate="${REDIRECT_RATE:-10000}"
create_rate="${CREATE_RATE:-1000}"
duration="${DURATION:-20s}"
links="${LINKS:-1000}"
port="${LOADTEST_PORT:-18700}"
metrics_port=$((port + 1))
base="http://127.0.0.1:$port"
vegeta=(go run github.com/tsenart/vegeta/v12@v12.13.0)

work="$(mktemp -d)"
"${vegeta[@]}" -version >/dev/null # download and build once, before timing anything
go build -o "$work/shortener" ./cmd/shortener

# Rate limits are raised: the load generator is a single client.
STORAGE=cassandra REDIS_ADDR=127.0.0.1:6379 \
POSTGRES_DSN="postgres://shortener:shortener@127.0.0.1:5432/shortener?sslmode=disable" \
KAFKA_BROKERS=127.0.0.1:9092 \
RATE_LIMIT_ANONYMOUS=100000000 RATE_LIMIT_USER=100000000 \
ADDR=":$port" METRICS_ADDR=":$metrics_port" BASE_URL="$base" \
	"$work/shortener" >"$work/server.log" 2>&1 &
pid=$!
trap 'kill -INT "$pid" 2>/dev/null || true; rm -rf "$work"' EXIT
for _ in $(seq 1 60); do
	curl -sf "$base/healthz" >/dev/null 2>&1 && break
	sleep 0.5
done

echo "setup: creating $links links"
key="$(curl -s -X POST "$base/api/v1/users" -d "{\"email\":\"load-$(date +%s)$RANDOM@example.com\"}" | sed -n 's/.*"api_key":"\([^"]*\)".*/\1/p')"
for i in $(seq 1 "$links"); do
	body="{\"long_url\":\"https://example.com/$i\"}"
	curl -s -X POST "$base/api/v1/links" -H "Authorization: Bearer $key" -d "$body" |
		sed -n 's/.*"key":"\([^"]*\)".*/\1/p'
done >"$work/keys.txt"
python3 - "$work" "$base" <<'PY'
import random, sys
work, base = sys.argv[1], sys.argv[2]
keys = open(f"{work}/keys.txt").read().split()
hot = keys[: max(1, len(keys) // 5)]
with open(f"{work}/redirects.txt", "w") as f:
    for _ in range(20000):
        k = random.choice(hot) if random.random() < 0.8 else random.choice(keys)
        f.write(f"GET {base}/{k}\n\n")
PY
echo '{"long_url":"https://example.com/load"}' >"$work/create.json"
printf 'POST %s/api/v1/links\nAuthorization: Bearer %s\n@%s\n' "$base" "$key" "$work/create.json" >"$work/creates.txt"

# Sample the service's CPU once a second (100% = one core).
( while kill -0 "$pid" 2>/dev/null; do ps -o %cpu= -p "$pid"; sleep 1; done ) >"$work/cpu.txt" &
sampler=$!
if [ -n "${PROFILE:-}" ]; then
	( sleep 3; curl -s "http://127.0.0.1:$metrics_port/debug/pprof/profile?seconds=10" >"${PROFILE_OUT:-cpu.pprof}" ) &
fi

echo "load: $redirect_rate redirects/s and $create_rate creates/s for $duration"
# Redirects are not followed: we measure the shortener, not the target site.
"${vegeta[@]}" attack -targets="$work/redirects.txt" -rate="$redirect_rate" -duration="$duration" \
	-redirects=-1 -max-workers=2000 >"$work/redirects.bin" &
r=$!
"${vegeta[@]}" attack -targets="$work/creates.txt" -rate="$create_rate" -duration="$duration" \
	-max-workers=2000 >"$work/creates.bin" &
c=$!
wait "$r" "$c"
kill "$sampler" 2>/dev/null || true
wait "$sampler" 2>/dev/null || true

code=0
for scenario in "redirects:${REDIRECT_P99_MS:-100}" "creates:${CREATE_P99_MS:-500}"; do
	name="${scenario%%:*}"
	max_p99_ms="${scenario##*:}"
	echo "--- $name"
	"${vegeta[@]}" report -type=text <"$work/$name.bin" | grep -E '^(Requests|Latencies|Success|Status)'
	"${vegeta[@]}" report -type=json <"$work/$name.bin" >"$work/$name.json"
	python3 - "$work/$name.json" "$max_p99_ms" <<'PY' || code=1
import json, sys
r = json.load(open(sys.argv[1]))
p99_ms, limit = r["latencies"]["99th"] / 1e6, float(sys.argv[2])
ok = p99_ms < limit and r["success"] > 0.99
print(f"{'ok  ' if ok else 'FAIL'} p99 {p99_ms:.1f} ms (limit {limit:.0f} ms), success {r['success'] * 100:.2f}% (min 99%)")
sys.exit(0 if ok else 1)
PY
done

echo "--- service CPU while loaded (100% = one core, $(getconf _NPROCESSORS_ONLN) cores)"
awk '$1 > 5 { n++; s += $1; if ($1 > m) m = $1 } END { if (n) printf "avg %.0f%%, max %.0f%%\n", s / n, m; else print "idle" }' "$work/cpu.txt"
echo "--- cache lookups by level"
curl -s "http://127.0.0.1:$metrics_port/metrics" | grep '^cache_lookups_total' || true
echo "--- errors in the server log: $(grep -c '"level":"ERROR"' "$work/server.log" || true)"
exit "$code"
