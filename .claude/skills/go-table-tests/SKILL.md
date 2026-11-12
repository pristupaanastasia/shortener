---
name: go-table-tests
description: Required format for Go tests in this repository — every test declares its cases in a slice of structs and runs them in a `for range` loop with `t.Run`. Use whenever writing, adding to, or modifying any `*_test.go` file, including integration tests.
---

# Go table tests

Every test function in this repository follows one shape: **cases are data in a slice, executed by a `for range` loop**. No assertions outside the loop.

## Basic form

```go
func TestValidateAlias(t *testing.T) {
	tests := []struct {
		name    string
		alias   string
		wantErr error
	}{
		{name: "valid", alias: "sale2026", wantErr: nil},
		{name: "too short", alias: "abc", wantErr: ErrInvalidAlias},
		{name: "reserved", alias: "Admin", wantErr: ErrInvalidAlias},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAlias(tt.alias)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("validateAlias(%q) = %v, want %v", tt.alias, err, tt.wantErr)
			}
		})
	}
}
```

Rules:

1. The slice is named `tests` and the loop variable `tt`.
2. Every case has a `name` field: unique, lowercase, describing the case (`"other user"`, `"expiry in past"`), not `"case 1"`.
3. Inputs and expected outputs are fields (`want...`, `wantErr`, `wantStatus`). The loop body contains the logic once; cases contain only data.
4. Each case runs in `t.Run(tt.name, ...)`, so a failure names the case and `go test -run 'TestX/case_name'` reruns it alone.
5. Setup shared by all cases (a store, a handler, a fixed clock) goes before the slice or into a helper marked `t.Helper()`. Per-case setup goes into a field, e.g. `setup func(t *testing.T, s *memory.Store)`.
6. Error messages say what was called, what came back and what was expected: `t.Errorf("Get(%q) = %v, want %v", ...)`.

## Scenarios with shared state

When steps depend on each other (create → delete → delete again → redirect returns 410), the cases are **steps** in a slice, run in order against shared state. Stop at the first failed step, since later steps would fail for the wrong reason:

```go
func TestDeleteFlow(t *testing.T) {
	h, key := newHandlerWithUser(t)

	steps := []struct {
		name       string
		req        request
		wantStatus int
	}{
		{name: "create", req: request{method: "POST", path: "/api/v1/links", body: `{"long_url":"https://example.com","alias":"deleteme"}`, apiKey: key}, wantStatus: http.StatusCreated},
		{name: "delete", req: request{method: "DELETE", path: "/api/v1/links/deleteme", apiKey: key}, wantStatus: http.StatusNoContent},
		{name: "delete again is idempotent", req: request{method: "DELETE", path: "/api/v1/links/deleteme", apiKey: key}, wantStatus: http.StatusNoContent},
		{name: "redirect is gone", req: request{method: "GET", path: "/deleteme"}, wantStatus: http.StatusGone},
	}
	for _, step := range steps {
		ok := t.Run(step.name, func(t *testing.T) {
			if rec := send(h, step.req); rec.Code != step.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, step.wantStatus, rec.Body)
			}
		})
		if !ok {
			break
		}
	}
}
```

Here the slice is `steps` and the loop variable `step`, so a reader sees at once that order matters. Do not use `t.Parallel()` in step tests.

## Concurrency, timing and other special checks

Parameterize them the same way: the slice holds the parameters (number of goroutines, delays, which dependency is down), the loop body runs the scenario.

```go
tests := []struct {
	name       string
	goroutines int
	wantReads  int32
}{
	{name: "single caller", goroutines: 1, wantReads: 1},
	{name: "100 concurrent misses share one read", goroutines: 100, wantReads: 1},
}
```

## Checklist before finishing

- [ ] Every `Test...` function has a `tests` (or `steps`) slice and a `for range` loop.
- [ ] No `t.Error`/`t.Fatal` outside the loop, except in setup helpers.
- [ ] Every case has a unique, descriptive `name`.
- [ ] `make check` passes (and `make test-integration` for `//go:build integration` files).
