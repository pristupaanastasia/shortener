#!/bin/sh
# Runs unit and integration tests with coverage across all packages and fails
# if the total is below COVERAGE_MIN percent. Needs the infrastructure from
# `make up`: without integration tests, storage packages would report 0%.
set -e
min="${COVERAGE_MIN:-80}"
profile="${COVERAGE_PROFILE:-coverage.out}"

# -coverpkg counts statements covered by tests of any package (e.g. the
# memory store exercised by HTTP tests), not only the package's own tests.
# The output is kept quiet on success but shown in full on failure: without
# it, CI would report only "Error 1" with no failing test name.
if ! out="$(go test -race -count=1 -tags integration -coverpkg=./... -coverprofile="$profile" ./... 2>&1)"; then
	echo "$out"
	echo "coverage: tests failed"
	exit 1
fi

total="$(go tool cover -func="$profile" | awk '/^total:/ { sub(/%/, "", $NF); print $NF }')"
echo "coverage: total ${total}% (minimum ${min}%)"
if awk -v t="$total" -v m="$min" 'BEGIN { exit !(t < m) }'; then
	echo "coverage: below ${min}%; least covered functions:"
	go tool cover -func="$profile" | grep -v '^total:' | sort -k3 -n | head -10
	exit 1
fi
