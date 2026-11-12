#!/usr/bin/env bash
# Builds and starts the real service, walks through the main user journey
# with curl, then checks for a clean shutdown. Storage is configured by the
# usual environment variables (STORAGE, REDIS_ADDR, POSTGRES_DSN, ...); with
# none set, everything runs in memory.
set -euo pipefail

port="${SMOKE_PORT:-18181}"
base="http://127.0.0.1:$port"
work="$(mktemp -d)"
log="$work/server.log"

go build -o "$work/shortener" ./cmd/shortener
ADDR=":$port" BASE_URL="$base" "$work/shortener" >"$log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; rm -rf "$work"' EXIT

fail() {
	echo "smoke: FAIL: $*"
	echo "--- server log"
	cat "$log"
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
expect "anonymous create" 201 "$(status -X POST "$base/api/v1/links" -d '{"long_url":"https://example.com/anon"}')"
expect "list without key" 401 "$(status "$base/api/v1/links")"
expect "delete link" 204 "$(status -X DELETE "$base/api/v1/links/$alias" "${auth[@]}")"
expect "redirect after delete" 410 "$(status "$base/$alias")"
expect "unknown key" 404 "$(status "$base/nosuchkey$run")"

kill -INT "$pid"
code=0
wait "$pid" || code=$?
trap 'rm -rf "$work"' EXIT
expect "graceful shutdown exit code" 0 "$code"
grep -q '"msg":"shutting down"' "$log" || fail "no shutdown message in the log"
if grep -q '"level":"ERROR"' "$log"; then
	fail "errors in the server log"
fi
echo "smoke: ok   no errors in the server log"
