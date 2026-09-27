# metric-rollup

Downsamples a time series into fixed windows, so a dashboard reads rollups instead of scanning every sample, and neighbouring windows can be merged when the query range grows.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/rollupctl --file <path> --window <duration> windows
    go run ./cmd/rollupctl --file <path> --window <duration> --from <ns> --to <ns> range
    go run ./cmd/rollupctl --file <path> --file <path> --window <duration> merge

`rollupctl` reads one sample per line — nanoseconds since the epoch,
whitespace, then a float — and prints, per window, its epoch-aligned start,
count, sum, minimum, and maximum. `windows` prints every window; `range`
prints only the windows overlapping the half-open interval `[--from, --to)`
(both endpoints in nanoseconds since the epoch), streamed out in cursor
batches; `merge` aggregates two files with the same window duration and
prints their windows combined. Exit codes: 2 for bad arguments, 3 if a
file is missing or a directory, 4 for invalid file contents or a merge
that cannot be combined.

## Public interface

`rollup.New(window time.Duration) *Roller` builds a roller.
- `(*Roller).Add(at time.Time, value float64) error` files a sample into its window.
- `(*Roller).AddBatch(samples []Sample) error` files a batch of samples atomically: all of them or none.
- `type Sample struct { At time.Time; Value float64 }`.
- `(*Roller).Window(start time.Time) (Window, bool)` returns a single bucket.
- `(*Roller).Windows() []Window` returns buckets in time order.
- `(*Roller).Range(from, to time.Time) []Window` returns the buckets overlapping `[from, to)` in time order.
- `(*Roller).Cursor(from, to time.Time) *Cursor` snapshots a range for batched reads; `(*Cursor).Next(n int) []Window` returns the next batch, then an empty slice once the range is exhausted.
- `(*Roller).Merge(other *Roller) error` folds another roller into this one.
- `(*Roller).Rollup(factor int) *View` derives a read-only view whose windows are `factor` times wider, merged from the roller's own windows; it panics with `rollup: bad factor` if `factor <= 0`.
- `(*View).Window(start time.Time) Window` returns the coarse window starting at `start`, or a zero `Window` (count 0) if none does.
- `(*View).Range(from, to time.Time) []Window` returns the coarse windows overlapping `[from, to)` in time order.
- `(*View).Cursor(from, to time.Time) *Cursor` snapshots a coarse range for batched reads.
- `type Window struct { Start time.Time; Count int64; Sum, Min, Max float64 }`.
- `rollup.ErrOutOfOrder`, `rollup.ErrWindowMismatch` error values.

## Tests

    go test ./...

## Limits

One window size per roller; merging requires equal sizes.
Values are float64 with the usual precision limits.
No persistence and no retention policy.
