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
    go run ./cmd/rollupctl --file <path> --window <duration> --max-series <n> --overflow aggregate|reject series

`rollupctl` reads one sample per line — nanoseconds since the epoch,
whitespace, then a float — and prints, per window, its epoch-aligned start,
count, sum, minimum, and maximum. `windows` prints every window; `range`
prints only the windows overlapping the half-open interval `[--from, --to)`
(both endpoints in nanoseconds since the epoch), streamed out in cursor
batches; `merge` aggregates two files with the same window duration and
prints their windows combined. `rates` treats the values as a cumulative
counter that may reset and prints, for every epoch-aligned window
overlapping `[--from, --to)` that holds at least one increment, six
columns: start, increment count, increment sum, minimum, maximum, and rate
(the increment sum divided by the window width in seconds). `series` reads
one labeled sample per line — nanosecond timestamp, a float, and a JSON
object of string labels, such as `0 1 {"host":"a"}` (`{}` for the empty
label set) — bounds the number of named series with `--max-series`, files
new series beyond the quota into one shared overflow bucket
(`--overflow aggregate`) or refuses them (`--overflow reject`), and prints
one `metric-rollup/series-report/v1` JSON document. Exit codes: 2 for bad
arguments, 3 if a file is missing or a directory, 4 for invalid file
contents (including a negative counter value), a merge that cannot be
combined, an invalid labeled field, or a labeled sample refused under
`reject`.

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
panics with `rollup: bad window` if `window <= 0`. A `Counter` consumes
the same `Sample` records as a `Roller`, but its values must be finite
and non-negative:

- `(*Counter).Add(at time.Time, value float64) error` files one cumulative
  reading; `(*Counter).AddBatch(samples []Sample) error` files a batch
  atomically — all of it or none — processing the readings in slice order
  against a marker shared with `Add`.
- The first reading only establishes the baseline. Each later reading
  produces one increment relative to the previous one: `current-previous`
  (one float64 subtraction) on a normal rise or a tie, and the reading
  itself when a smaller reading identifies a counter reset.
- Readings with the same timestamp are taken in arrival order; an
  increment belongs to the later reading's epoch-aligned window, so ties
  and resets at one instant file zero or reset increments there.
- A rejected reading or batch changes nothing: non-finite values return
  `ErrNonFinite`, values before the marker return `ErrOutOfOrder`, and
  negative cumulative values return `ErrNegativeCounter`.
- `(*Counter).Rates(from, to time.Time) []RateWindow` returns, in time
  order, the windows overlapping `[from, to)` that actually hold at least
  one increment — windows with none are skipped. An empty or backwards
  range succeeds with no output.
- `(*Counter).RateCursor(from, to time.Time) *RateCursor` snapshots a
  range for batched reads; `(*RateCursor).Next(n int) []RateWindow`
  returns the next batch, then an empty slice once the range is exhausted.
- `type RateWindow struct { Start time.Time; Count int64; Sum, Min, Max, Rate float64 }`.
  `Count` is the increment count, `Sum` the exact real increment sum
  rounded to float64 once, `Min`/`Max` the single-increment range, and
  `Rate` `Sum / window.Seconds()`. Zero increments are positive zeros.
- A `Counter` is safe for concurrent use like a `Roller`, and every read
  observes one internally consistent snapshot.

### Labeled series with cardinality control

`rollup.NewSeriesSet(window time.Duration, cfg SeriesConfig) *SeriesSet`
builds a set of labeled series. `SeriesConfig{MaxSeries int, Overflow
OverflowPolicy}` requires a positive `MaxSeries` and either
`OverflowAggregate` or `OverflowReject`; an invalid window panics with
`rollup: bad window` and any other invalid config with
`rollup: bad series config`.

- A sample is `LabeledSample{At time.Time; Value float64; Labels
  map[string]string}`. Samples with equal label keys and values are one
  series regardless of map iteration order; the empty label map names one
  ordinary series, distinct from every labeled series and from the
  overflow bucket.
- Real series claim one of the `MaxSeries` slots in first-seen order. Once
  the slots are full, `OverflowAggregate` files a new series' samples into
  one shared overflow bucket that occupies no slot, and `OverflowReject`
  refuses each such sample with `ErrSeriesLimit` and counts it.
- `Add` validates before touching state: an empty label key or value is
  `ErrInvalidLabel`, a NaN or infinity `ErrNonFinite`, and a sample earlier
  than that series' own most recent sample `ErrOutOfOrder`. Ordering is per
  series; different series never constrain each other.
- `AddBatch(samples []LabeledSample) error` files the batch all or nothing;
  any validation, limit, or out-of-order failure rejects the whole unit and
  leaves no slot claimed, no series spilled, and no rejection counted.
- `Snapshot() []SeriesWindow` returns one consistent snapshot: slot series
  in first-seen order, the merged overflow bucket last, and windows in time
  order within each. `SeriesWindow{Labels map[string]string; Overflow bool;
  Window Window}` — `Labels` is `{}` for both the empty-label series and the
  bucket, which `Overflow` tells apart.
- `Cardinality() CardinalityReport` returns `AcceptedSeries` (slots used),
  `OverflowSeries` (distinct spilled series, each counted once at its first
  spill), and `RejectedSamples` (samples refused under reject).
- A `SeriesSet` is safe for concurrent use like a `Roller`, and every read
  observes one internally consistent snapshot.

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
