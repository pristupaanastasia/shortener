#!/bin/sh
# Measures total coverage of unit tests, integration tests and the smoke test
# of the real binaries, and fails if it is below COVERAGE_MIN percent. Needs
# the infrastructure from `make up`: without integration tests, storage
# packages would report 0%.
set -e
min="${COVERAGE_MIN:-80}"
profile="${COVERAGE_PROFILE:-coverage.out}"
raw="$(mktemp -d)"
trap 'rm -rf "$raw"' EXIT
mkdir -p "$raw/tests" "$raw/smoke"

# -coverpkg counts statements covered by tests of any package (e.g. the
# memory store exercised by HTTP tests), not only the package's own tests.
# Output is quiet on success but shown in full on failure: without it, CI
# would report only "Error 1" with no failing test name.
if ! out="$(go test -race -count=1 -tags integration -cover -coverpkg=./... ./... -args -test.gocoverdir="$raw/tests" 2>&1)"; then
	echo "$out"
	echo "coverage: tests failed"
	exit 1
fi

# The smoke test runs binaries built with -cover; they write coverage on a
# clean exit. This covers main() and wiring that unit tests cannot reach.
if ! out="$(SMOKE_COVERDIR="$raw/smoke" make -s smoke-full 2>&1)"; then
	echo "$out"
	echo "coverage: smoke test failed"
	exit 1
fi

go tool covdata textfmt -i="$raw/tests,$raw/smoke" -o "$profile"
total="$(go tool cover -func="$profile" | awk '/^total:/ { sub(/%/, "", $NF); print $NF }')"
echo "coverage: total ${total}% (minimum ${min}%)"
if awk -v t="$total" -v m="$min" 'BEGIN { exit !(t < m) }'; then
	echo "coverage: below ${min}%; least covered functions:"
	go tool cover -func="$profile" | grep -v '^total:' | sort -k3 -n | head -10
	exit 1
fi
