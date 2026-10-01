# metric-rollup

Downsamples a time series into fixed windows, so a dashboard reads rollups instead of scanning every sample, and neighbouring windows can be merged when the query range grows.

## Requirements

Go 1.22 or newer. Standard library only.

## Build

    go build ./...
    go run ./cmd/rollupctl --file <path> --window <duration> windows
    go run ./cmd/rollupctl --file <path> --window <duration> --from <ns> --to <ns> range
    go run ./cmd/rollupctl --file <path> --file <path> --window <duration> merge
    go run ./cmd/rollupctl --file <path> --window <duration> --from <ns> --to <ns> rates

`rollupctl` reads one sample per line — nanoseconds since the epoch,
whitespace, then a float — and prints, per window, its epoch-aligned start,
count, sum, minimum, and maximum. `windows` prints every window; `range`
prints only the windows overlapping the half-open interval `[--from, --to)`
(both endpoints in nanoseconds since the epoch), streamed out in cursor
batches; `merge` aggregates two files with the same window duration and
prints their windows combined. `rates` treats the file as a cumulative
counter (see below) and prints, per overlapping window that actually holds
increments, the start, increment count, increment sum, minimum increment,
maximum increment, and rate (sum over the window's seconds). Exit codes: 2
for bad arguments, 3 if a file is missing or a directory, 4 for invalid
file contents or a merge that cannot be combined.

## Public interface

`rollup.New(window time.Duration) *Roller` builds a roller.
- `(*Roller).Add(at time.Time, value float64) error` files a sample into its window. NaN and either infinity are rejected with `ErrNonFinite`.
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
- `rollup.ErrOutOfOrder`, `rollup.ErrWindowMismatch`, `rollup.ErrNonFinite` error values.

### Cumulative counters

`rollup.NewCounter(window time.Duration) *Counter` builds a counter; it
panics with `rollup: bad window` if the width is not positive. A counter
treats its samples as a running cumulative value:

- values must be finite and non-negative (negative zero is allowed);
- the first sample only sets the baseline and produces no increment;
- each later sample produces one increment relative to the preceding
  sample in arrival order: `value - previous`, rounded to float64 once,
  when `value >= previous`; when `value < previous` the counter is
  recognized as having reset and the increment is `value` itself;
- samples at the same timestamp are processed in arrival order and may
  yield a zero increment or a reset increment;
- every increment is attributed to the epoch-aligned window of the later
  sample.

- `(*Counter).Add(at time.Time, value float64) error` and
  `(*Counter).AddBatch(samples []Sample) error` mirror the Roller:
  non-finite values return `ErrNonFinite`, a negative value returns
  `ErrNegativeCounter`, a stale timestamp returns `ErrOutOfOrder`, a
  single failure changes nothing, and a batch is all-or-nothing (its
  samples may be listed in any order, but none may predate the newest
  accepted sample).
- `(*Counter).Rates(from, to time.Time) []RateWindow` returns, in start
  order, the windows overlapping `[from, to)` that actually contain
  increments; an empty range is a successful empty slice.
- `(*Counter).RateCursor(from, to time.Time) *RateCursor` snapshots the
  range for batched reads; `(*RateCursor).Next(n int) []RateWindow`
  returns the next batch and an empty slice once exhausted.
- `type RateWindow struct { Start time.Time; Count int64; Sum, Min, Max, Rate float64 }`.
  Count is the number of increments, Sum their exact real sum rounded to
  float64 once, Min/Max the single-increment range, and Rate
  `Sum / window-seconds`. Every increment is non-negative, so zero values
  are always positive zero.
- `rollup.ErrNegativeCounter` is the negative-value sentinel;
  `ErrNonFinite` and `ErrOutOfOrder` are shared with the Roller.

A `Counter` is safe for concurrent use like a `Roller`, and every read is
an internally consistent snapshot.

### Numeric semantics

The statistics of a window are fixed by its sample set, not by the order
or interleaving in which the samples arrived, so the same set read out
through any command path always compares bit for bit.

- The sum is the exact real sum of the finite samples, rounded to
  float64 once; submission order cannot change any bit.
- A zero sum is a negative zero only when every sample in the window is a
  negative zero; otherwise it is a positive zero.
- A minimum equal to zero is a negative zero when the window holds a
  negative-zero sample; a maximum equal to zero is a negative zero only
  when there is no positive-zero sample.
- A sum outside the float64 range rounds to `+Inf` or `-Inf`. Merging a
  positive-infinity window with a negative-infinity one yields `NaN`.
- NaN and infinity sample values are never filed; they are rejected with
  `ErrNonFinite`, and a rejected batch leaves the roller unchanged.
- The count saturates at the int64 ceiling and stops growing; the sum and
  extrema keep updating.

## Tests

    go test ./...

## Limits

One window size per roller; merging requires equal sizes.
Values are float64 with the usual precision limits.
No persistence and no retention policy.
