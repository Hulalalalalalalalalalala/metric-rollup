package rollup

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

// ErrNegativeCounter is returned by Counter.Add and Counter.AddBatch when
// a counter value is negative. Counter values must be finite and
// non-negative; such a sample is never filed: on Add the counter is left
// unchanged, and on AddBatch the whole batch is rejected as one unit.
var ErrNegativeCounter = errors.New("rollup: negative counter value")

// RateWindow holds the increments derived in one window. Start is the
// window's left-closed, right-open epoch-aligned lower bound. Count is the
// number of increments in the window; Sum, Min, and Max are their exact
// real sum (rounded to float64 once) and their single-increment range;
// Rate is Sum divided by the window width in seconds.
type RateWindow struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	Rate  float64
}

// Counter derives per-window rates from a monotonically non-decreasing
// cumulative counter that may reset. Samples are cumulative readings in
// non-decreasing time order, like a Roller's, but their values must be
// finite and non-negative. The first sample only establishes the
// baseline; each later sample produces one increment relative to the
// previous reading:
//
//   - a reading at least as large as the previous one increments by the
//     one float64 subtraction current-previous;
//   - a reading smaller than the previous one is a reset and increments by
//     the reading itself.
//
// Increments are always non-negative; a zero increment is filed like any
// other. Samples at the same instant are taken in arrival order, and each
// increment belongs to the window of the later sample, so same-instant
// samples can yield zero or reset increments.
//
// The zero value is not usable; build one with NewCounter. All methods
// are safe to call concurrently from multiple goroutines, and every read
// observes an internally consistent snapshot: a batch is visible to other
// callers either in full or not at all.
type Counter struct {
	// mu guards buckets, bstats, back, latest, prev, and has. window is
	// fixed at construction and read without the lock. The window storage
	// follows the same buckets-plus-backfill scheme as Roller: increments
	// arrive in non-decreasing time order, so they append in amortized
	// constant time and reads stay sorted.
	mu      sync.RWMutex
	window  time.Duration
	buckets []Window
	bstats  []stats
	back    map[time.Time]backWindow
	latest  time.Time
	prev    float64
	has     bool
}

// NewCounter builds a counter with the given window size. It panics with
// "rollup: bad window" if window is not positive.
func NewCounter(window time.Duration) *Counter {
	if window <= 0 {
		panic("rollup: bad window")
	}
	return &Counter{window: window}
}

// startOf returns the start of the window containing at, aligned to an
// integer multiple of the window size since the Unix epoch.
func (c *Counter) startOf(at time.Time) time.Time {
	return alignStart(at, c.window.Nanoseconds())
}

// Add files one cumulative counter reading. A non-finite value (NaN or an
// infinity) is rejected with ErrNonFinite, a negative value with
// ErrNegativeCounter, and a reading earlier than the most recent accepted
// reading with ErrOutOfOrder. In every rejection case the counter,
// including its baseline and most-recent marker, is left unchanged; a
// reading at the same instant as the marker is accepted and produces its
// increment in arrival order.
//
// Concurrent Adds are serialized: each takes effect in the order it
// acquires the counter, and the checks are measured against the readings
// accepted before it in that order.
func (c *Counter) Add(at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrNonFinite
	}
	if value < 0 {
		return ErrNegativeCounter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.has && at.Before(c.latest) {
		return ErrOutOfOrder
	}
	c.applyLocked(at, value)
	return nil
}

// AddBatch files a batch of cumulative readings as one atomic unit: either
// every reading takes effect or none does. The readings may arrive in any
// order and are not checked against each other; several readings at the
// same instant each produce an increment in arrival order. The first
// reading of the whole stream only establishes the baseline, and every
// later reading in the batch is diffed against the preceding one exactly
// as Add would diff it.
//
// A batch holding any non-finite value is rejected with ErrNonFinite, one
// holding any negative value with ErrNegativeCounter, and one whose
// readings predate the most recent reading already accepted with
// ErrOutOfOrder. Any rejection leaves the counter, its baseline, and its
// most-recent marker unchanged. Otherwise the marker advances to the
// later of its previous value and the newest reading in the batch. A nil
// or empty batch is accepted, changes nothing, and does not advance the
// marker.
//
// The batch is atomic with respect to every other call on the counter:
// concurrent adds, rate reads, and cursor creation observe the counter
// wholly before or wholly after the batch.
func (c *Counter) AddBatch(samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}
	// Value checks scan the whole batch first, so one bad reading rejects
	// the unit even when a stale reading sits earlier in the slice.
	// Nothing is filed until the batch is fully validated.
	for _, s := range samples {
		if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
			return ErrNonFinite
		}
	}
	for _, s := range samples {
		if s.Value < 0 {
			return ErrNegativeCounter
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	latest := samples[0].At
	for _, s := range samples {
		if c.has && s.At.Before(c.latest) {
			return ErrOutOfOrder
		}
		if s.At.After(latest) {
			latest = s.At
		}
	}
	if c.has && c.latest.After(latest) {
		latest = c.latest
	}
	for _, s := range samples {
		c.applyLocked(s.At, s.Value)
	}
	c.latest = latest
	return nil
}

// applyLocked folds one validated reading into the counter: the first
// sets the baseline, later ones file the increment against the previous
// reading. The caller must hold mu and must already have cleared the
// non-finite, negative, and out-of-order checks.
func (c *Counter) applyLocked(at time.Time, value float64) {
	if !c.has {
		c.has = true
		c.prev = value
		c.latest = at
		return
	}
	delta := value - c.prev
	if value < c.prev {
		// The counter reset: the reading since the reset is the reading
		// itself. Equal readings subtract to a positive zero and file a
		// zero increment; a reset to zero files one too.
		delta = value
	}
	if delta == 0 {
		// A zero increment is always a positive zero, even when the
		// reset reading arrived as a negative zero.
		delta = 0
	}
	c.fileDeltaLocked(at, delta)
	c.prev = value
	c.latest = at
}

// fileDeltaLocked folds one non-negative increment into its window. The
// caller must hold mu.
func (c *Counter) fileDeltaLocked(at time.Time, delta float64) {
	start := c.startOf(at)
	n := len(c.buckets)
	switch {
	case n == 0:
		c.buckets = append(c.buckets, newBucket(start, delta))
		c.bstats = append(c.bstats, seedStats(delta))
	case !start.Before(c.buckets[n-1].Start):
		// The common case: the increment lands in the newest window or
		// just past it, so the sorted order is maintained for free.
		if c.buckets[n-1].Start.Equal(start) {
			addTo(&c.buckets[n-1], &c.bstats[n-1], delta)
		} else {
			c.buckets = append(c.buckets, newBucket(start, delta))
			c.bstats = append(c.bstats, seedStats(delta))
		}
	default:
		// An older window can only receive an increment when the batch's
		// readings are out of time order. A window that already exists is
		// updated in place; a new one goes into the backfill map.
		if i, ok := windowIndex(c.buckets, start); ok {
			addTo(&c.buckets[i], &c.bstats[i], delta)
		} else if bw, ok := c.back[start]; ok {
			addTo(&bw.w, &bw.st, delta)
			c.back[start] = bw
		} else {
			if c.back == nil {
				c.back = make(map[time.Time]backWindow)
			}
			c.back[start] = backWindow{w: newBucket(start, delta), st: seedStats(delta)}
			c.maybeFlushBackLocked()
		}
	}
}

// maybeFlushBackLocked folds the backfilled windows into buckets once
// they number at least half as many as buckets, so the sorting and merge
// cost amortizes to constant time per backfilled window. The caller must
// hold mu.
func (c *Counter) maybeFlushBackLocked() {
	if len(c.back)*2 < len(c.buckets) {
		return
	}
	bw, bs := c.sortedBackLocked()
	c.buckets, c.bstats = mergeWindows(c.buckets, c.bstats, bw, bs)
	c.back = nil
}

// sortedBackLocked returns the backfilled windows in time order. The
// caller must hold mu; the returned slices are fresh and aligned and each
// stats owns an independent rational.
func (c *Counter) sortedBackLocked() ([]Window, []stats) {
	starts := make([]time.Time, 0, len(c.back))
	for s := range c.back {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i].Before(starts[j])
	})
	ws := make([]Window, len(starts))
	ss := make([]stats, len(starts))
	for i, s := range starts {
		bw := c.back[s]
		ws[i] = bw.w
		ss[i] = cloneStats(bw.st)
	}
	return ws, ss
}

// sortedBackWindowsLocked returns just the backfilled windows in time
// order for display reads. The caller must hold mu.
func (c *Counter) sortedBackWindowsLocked() []Window {
	starts := make([]time.Time, 0, len(c.back))
	for s := range c.back {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i].Before(starts[j])
	})
	ws := make([]Window, len(starts))
	for i, s := range starts {
		ws[i] = c.back[s].w
	}
	return ws
}

// windowsLocked returns every window holding at least one increment, in
// time order, as a snapshot copy. The caller must hold mu.
func (c *Counter) windowsLocked() []Window {
	if len(c.back) == 0 {
		out := make([]Window, len(c.buckets))
		copy(out, c.buckets)
		return out
	}
	back := c.sortedBackWindowsLocked()
	merged := make([]Window, 0, len(c.buckets)+len(back))
	i, j := 0, 0
	for i < len(c.buckets) && j < len(back) {
		if c.buckets[i].Start.Before(back[j].Start) {
			merged = append(merged, c.buckets[i])
			i++
		} else {
			merged = append(merged, back[j])
			j++
		}
	}
	merged = append(merged, c.buckets[i:]...)
	merged = append(merged, back[j:]...)
	return merged
}

// toRateWindows derives RateWindows from a time-ordered window snapshot,
// attaching each window's rate and dropping the ones outside the
// half-open interval [from, to).
func (c *Counter) toRateWindows(ws []Window, from, to time.Time) []RateWindow {
	out := []RateWindow{}
	secs := c.window.Seconds()
	lo := c.startOf(from)
	for _, w := range ws {
		if w.Start.Before(lo) {
			continue
		}
		if !w.Start.Before(to) {
			break
		}
		out = append(out, RateWindow{
			Start: w.Start,
			Count: w.Count,
			Sum:   w.Sum,
			Min:   w.Min,
			Max:   w.Max,
			Rate:  w.Sum / secs,
		})
	}
	return out
}

// Rates returns the windows that both overlap the half-open interval
// [from, to) and hold at least one increment, in time order by window
// start. Windows are selected by the same epoch-aligned, left-closed rule
// a Roller's Range uses: the first window reported is the one containing
// from, and a window starting exactly at to is excluded. Every reported
// window carries its rate, its increment sum divided by the window width
// in seconds as one float64 division. An interval whose end does not come
// after its start yields an empty slice, as does one no increment-bearing
// window overlaps. The result is a snapshot of one moment: readings filed
// after the call do not alter it.
func (c *Counter) Rates(from, to time.Time) []RateWindow {
	c.mu.RLock()
	ws := c.windowsLocked()
	c.mu.RUnlock()
	if !from.Before(to) {
		return []RateWindow{}
	}
	return c.toRateWindows(ws, from, to)
}

// RateCursor reads rate windows over a fixed range in batches. The
// windows are snapshotted when the cursor is created: readings filed
// afterwards do not alter what the cursor returns, so batched iteration
// over a large range never tears, skips, or repeats a window.
//
// A RateCursor is safe to advance from multiple goroutines at once; each
// batch is handed out exactly once.
type RateCursor struct {
	// mu guards pos. windows is fixed at creation and read without the
	// lock.
	mu      sync.Mutex
	windows []RateWindow
	pos     int
}

// RateCursor returns a cursor over the rate windows overlapping the
// half-open interval [from, to), selected by the same rule as Rates.
func (c *Counter) RateCursor(from, to time.Time) *RateCursor {
	return &RateCursor{windows: c.Rates(from, to)}
}

// Next returns the next batch of up to n rate windows, in time order.
// Batches are consecutive and non-overlapping: together they cover exactly
// the windows in the cursor's range. Once the range is exhausted, Next
// returns an empty slice and the cursor does not move. A non-positive n
// yields an empty slice and does not advance the cursor.
func (c *RateCursor) Next(n int) []RateWindow {
	c.mu.Lock()
	defer c.mu.Unlock()
	rest := len(c.windows) - c.pos
	if n <= 0 || rest == 0 {
		return []RateWindow{}
	}
	if n > rest {
		n = rest
	}
	out := make([]RateWindow, n)
	copy(out, c.windows[c.pos:c.pos+n])
	c.pos += n
	return out
}
