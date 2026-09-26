# metric-rollup

Downsamples a time series into fixed windows, so a dashboard reads rollups instead of scanning every sample, and neighbouring windows can be merged when the query range grows.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/rollupctl --file <path> windows

## Public interface

`rollup.New(window time.Duration) *Roller` builds a roller.
- `(*Roller).Add(at time.Time, value float64) error` files a sample into its window.
- `(*Roller).Window(start time.Time) (Window, bool)` returns a single bucket.
- `(*Roller).Windows() []Window` returns buckets in time order.
- `(*Roller).Range(from, to time.Time) []Window` returns the buckets whose start lies in `[from, to)`, in time order; empty, equal, or reversed bounds yield an empty slice.
- `(*Roller).Cursor() *Cursor` snapshots the buckets for batched reads; `(*Cursor).Next(n int) []Window` returns the next batch and an empty slice once drained.
- `(*Roller).Merge(other *Roller) error` folds another roller into this one.
- `type Window struct { Start time.Time; Count int64; Sum, Min, Max float64 }`.
- `rollup.ErrOutOfOrder`, `rollup.ErrWindowMismatch` error values.

## Tests

    go test ./...

## Limits

One window size per roller; merging requires equal sizes.
Values are float64 with the usual precision limits.
No persistence and no retention policy.
