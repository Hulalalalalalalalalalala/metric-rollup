package rollup

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

// ErrNegativeCounter is returned by Counter.Add and Counter.AddBatch when a
// cumulative value is negative. Such a sample is never filed: on Add the
// counter is left unchanged, and on AddBatch the whole batch is rejected as
// one unit.
var ErrNegativeCounter = errors.New("rollup: negative counter")

// RateWindow holds the increments observed in one counter window. Start is
// the window's epoch-aligned, left-closed lower bound. Count is the number
// of increments filed into the window; Sum is their exact real sum rounded
// to float64 once; Min and Max are the smallest and largest single
// increments; Rate is Sum divided by the window width in seconds.
//
// Every increment is non-negative, so a zero Sum, Min, Max, or Rate is a
// positive zero, including the zero increment of an unchanged counter or
// an equal-timestamp pair.
type RateWindow struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	Rate  float64
}

// Counter tracks a cumulatively observed counter that may reset. Values
// must be finite and non-negative. The first sample only establishes the
// baseline; every later sample produces one increment relative to the
// preceding sample in arrival order:
//
//   - when the value is at least the preceding value, the increment is the
//     float64 subtraction of the two, rounded to float64 once;
//   - when the value is smaller than the preceding value, the counter is
//     recognized as having reset and the increment is the value itself.
//
// Increments are attributed to the epoch-aligned window of the later
// sample, including increments between equal timestamps.
//
// The zero value is not usable; build one with NewCounter. All methods are
// safe to call concurrently from multiple goroutines, and every read
// observes an internally consistent snapshot.
type Counter struct {
	// mu guards buckets, latest, prev, and hasPrev. window is fixed at
	// construction and read without the lock.
	//
	// buckets stays sorted by window start: samples arrive in
	// non-decreasing time order and every increment belongs to the later
	// sample's window, so an increment never lands before the newest
	// window and Add appends in amortized constant time. Rates and
	// RateCursor then read one contiguous span by binary search.
	mu      sync.RWMutex
	window  time.Duration
	buckets []rateBucket
	latest  time.Time
	prev    float64
	hasPrev bool
}

// rateBucket is one window's increments: the populated RateWindow plus the
// exact bookkeeping behind Sum.
type rateBucket struct {
	w  RateWindow
	st stats
}

// NewCounter builds a counter with the given window size. It panics with
// "rollup: bad window" if window is not positive.
func NewCounter(window time.Duration) *Counter {
	if window <= 0 {
		panic("rollup: bad window")
	}
	return &Counter{window: window}
}

// Add files one cumulative value observed at at. A non-finite value (NaN or
// an infinity) is rejected with ErrNonFinite, a negative value with
// ErrNegativeCounter, and a sample earlier than the most recent accepted
// sample with ErrOutOfOrder. In every rejection case the counter,
// including its baseline and most-recent marker, is left unchanged; a
// sample at the same instant as the marker is accepted and produces its
// own increment (zero, or a reset increment when the value is smaller).
//
// The first accepted sample only establishes the baseline and files no
// increment. Concurrent Adds are serialized: each takes effect in the
// order it acquires the counter, and the checks are measured against the
// samples accepted before it in that order.
func (c *Counter) Add(at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrNonFinite
	}
	if value < 0 {
		return ErrNegativeCounter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasPrev && at.Before(c.latest) {
		return ErrOutOfOrder
	}
	c.fileLocked(at, value)
	c.latest = at
	return nil
}

// AddBatch files a batch of cumulative samples as one atomic unit: either
// every sample takes effect or none does. The samples are processed
// strictly in slice order against the running cumulative value, so a reset
// inside the batch is recognized the same way as across Add calls. The
// file order itself need not be sorted by time; every sample must merely
// be no earlier than the newest sample already accepted. Several samples
// at the same instant each produce their own increment.
//
// A batch holding any non-finite value is rejected with ErrNonFinite, any
// negative value with ErrNegativeCounter, and any sample earlier than the
// most recent accepted sample with ErrOutOfOrder. Any rejection leaves the
// counter, its baseline, and its most-recent marker unchanged. A nil or
// empty batch is accepted, changes nothing, and does not advance the
// marker.
//
// The batch is atomic with respect to every other call on the counter:
// concurrent adds, batches, and rate reads observe the counter wholly
// before or wholly after it.
func (c *Counter) AddBatch(samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}
	// Every value is validated before anything is filed, so one bad value
	// rejects the unit regardless of where it sits in the slice.
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
	newest := samples[0].At
	for _, s := range samples[1:] {
		if s.At.After(newest) {
			newest = s.At
		}
	}
	if c.hasPrev {
		for _, s := range samples {
			if s.At.Before(c.latest) {
				return ErrOutOfOrder
			}
		}
	}
	for _, s := range samples {
		c.fileLocked(s.At, s.Value)
	}
	// The running value is the last sample in arrival order, but the
	// ordering marker is the newest timestamp the batch covered, so it can
	// never move backwards.
	c.latest = newest
	return nil
}

// fileLocked folds one validated sample into the running counter: the
// first sample sets the baseline and every later sample files one
// increment into the later sample's window. It updates the running value
// but not the most-recent timestamp marker, which the caller sets: Add to
// the sample's own instant and AddBatch to the newest instant in the
// batch. The caller must hold mu and must already have cleared the
// non-finite, negative, and out-of-order checks.
func (c *Counter) fileLocked(at time.Time, value float64) {
	// Negative zero is numerically zero; normalize it so every zero
	// increment and zero window carries the positive sign the contract
	// fixes, regardless of how the sample text was written.
	if value == 0 {
		value = 0
	}
	if c.hasPrev {
		inc := value - c.prev
		if value < c.prev {
			inc = value
		}
		c.addIncrementLocked(at, inc)
	}
	c.prev = value
	c.hasPrev = true
}

// addIncrementLocked files one increment into the window containing at.
// Samples normally move forward in time, so the target is the newest
// bucket or a freshly appended one; an AddBatch may list its samples in
// any order, though, and an increment that lands in an older window is
// found or inserted by binary search, keeping buckets sorted by start.
// The caller must hold mu.
func (c *Counter) addIncrementLocked(at time.Time, inc float64) {
	start := alignStart(at, c.window.Nanoseconds())
	seconds := c.windowSeconds()
	n := len(c.buckets)
	if n == 0 {
		c.buckets = append(c.buckets, newRateBucket(start, inc, seconds))
		return
	}
	// Fast path: the window is the newest one or just past it, so sorted
	// order is maintained for free.
	if !start.Before(c.buckets[n-1].w.Start) {
		if c.buckets[n-1].w.Start.Equal(start) {
			b := &c.buckets[n-1]
			addToRate(&b.w, &b.st, inc, seconds)
		} else {
			c.buckets = append(c.buckets, newRateBucket(start, inc, seconds))
		}
		return
	}
	i, ok := rateBucketIndex(c.buckets, start)
	if ok {
		addToRate(&c.buckets[i].w, &c.buckets[i].st, inc, seconds)
		return
	}
	// Insert a fresh bucket at its sorted position.
	c.buckets = append(c.buckets, rateBucket{})
	copy(c.buckets[i+1:], c.buckets[i:])
	c.buckets[i] = newRateBucket(start, inc, seconds)
}

// rateBucketIndex locates the bucket whose window starts at start in
// buckets, which must be sorted by start. The second result reports
// whether it exists; when it does not, the index is where the bucket
// would be inserted to stay sorted.
func rateBucketIndex(buckets []rateBucket, start time.Time) (int, bool) {
	i := sort.Search(len(buckets), func(i int) bool {
		return !buckets[i].w.Start.Before(start)
	})
	if i < len(buckets) && buckets[i].w.Start.Equal(start) {
		return i, true
	}
	return i, false
}

// windowSeconds returns the window width as a float64 count of seconds,
// the divisor fixed for every Rate of this counter.
func (c *Counter) windowSeconds() float64 {
	return float64(c.window.Nanoseconds()) / 1e9
}

// newRateBucket returns a window holding a single increment, with Sum,
// Min, Max, and Rate already canonical.
func newRateBucket(start time.Time, inc, seconds float64) rateBucket {
	w := RateWindow{Start: start, Count: 1, Sum: inc, Min: inc, Max: inc, Rate: inc / seconds}
	return rateBucket{w: w, st: seedStats(inc)}
}

// addToRate folds one non-negative increment into w and its bookkeeping
// st. Count saturates at math.MaxInt64 instead of overflowing; the sum,
// extrema, and rate keep updating afterwards under their fixed semantics.
// Every increment is a non-negative float64 and zero inputs are
// normalized to positive zero, so a zero extremum is always positive.
func addToRate(w *RateWindow, st *stats, inc, seconds float64) {
	if w.Count < math.MaxInt64 {
		w.Count++
	}
	st.accumulate(inc)
	if inc < w.Min {
		w.Min = inc
	}
	if inc > w.Max {
		w.Max = inc
	}
	w.Sum = st.canonicalSum()
	w.Rate = w.Sum / seconds
}

// Rates returns the windows that both overlap the half-open interval
// [from, to) and hold at least one increment, in time order by window
// start. Windows are selected by the same epoch-aligned, left-closed rule
// as a Roller's Range: the first window returned is the one containing
// from, and a window starting exactly at to is excluded. An empty or
// backwards interval is not an error and yields an empty slice, as does an
// interval no populated window overlaps. The result is a snapshot of one
// moment: samples filed after the call do not alter it.
func (c *Counter) Rates(from, to time.Time) []RateWindow {
	return c.RateCursor(from, to).Next(math.MaxInt)
}

// RateCursor reads increment windows over [from, to) in batches. The
// windows are snapshotted when the cursor is created: samples filed
// afterwards do not alter what the cursor returns, so batched iteration
// never tears, skips, or repeats a window, and the end of the range is
// reached deterministically no matter what is written while the cursor
// advances.
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

// RateCursor returns a cursor over the increment windows overlapping the
// half-open interval [from, to), selected by the same rule as Rates.
func (c *Counter) RateCursor(from, to time.Time) *RateCursor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	windows := []RateWindow{}
	if !from.Before(to) || len(c.buckets) == 0 {
		return &RateCursor{windows: windows}
	}
	lo := alignStart(from, c.window.Nanoseconds())
	first := sort.Search(len(c.buckets), func(i int) bool {
		return !c.buckets[i].w.Start.Before(lo)
	})
	for i := first; i < len(c.buckets); i++ {
		if !c.buckets[i].w.Start.Before(to) {
			break
		}
		windows = append(windows, c.buckets[i].w)
	}
	return &RateCursor{windows: windows}
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
