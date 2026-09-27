// Package rollup downsamples a time series into fixed windows aligned to
// the Unix epoch, and merges rollups that share a window size.
//
// A Roller is safe for concurrent use: samples may be filed, windows read,
// and rollers merged from multiple goroutines at once. Every read observes
// an internally consistent snapshot, and a merge or batch is visible to
// other callers either in full or not at all.
package rollup

import (
	"errors"
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrOutOfOrder is returned by Add when a sample predates the most recent
// sample the roller has already accepted.
var ErrOutOfOrder = errors.New("rollup: out of order")

// ErrWindowMismatch is returned by Merge when the two rollers use different
// window sizes.
var ErrWindowMismatch = errors.New("rollup: window mismatch")

// ErrNonFinite is returned by Add and AddBatch when a sample value is NaN
// or either infinity. Such a value is not filed and leaves the roller
// unchanged; a batch containing one takes no effect at all.
var ErrNonFinite = errors.New("rollup: non-finite value")

// Sample is one record in a batch passed to AddBatch: a value observed at
// one instant.
type Sample struct {
	At    time.Time
	Value float64
}

// Window holds the aggregate statistics of the samples filed into one
// window. Start is the window's left-closed, right-open lower bound.
type Window struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Roller aggregates samples into fixed windows of a single size.
//
// The zero value is not usable; build one with New. All methods are safe
// to call concurrently from multiple goroutines.
type Roller struct {
	// mu guards buckets, back, latest, and hasAny. window and id are
	// fixed at construction and read without the lock.
	//
	// buckets is one flat slice kept sorted by window start: samples
	// arrive in non-decreasing time order, so Add appends in amortized
	// constant time, Windows reads out without re-sorting, and Merge is
	// a single linear pass over both sides. sums[i] holds the exact,
	// order-independent total for buckets[i]; the Sum field there is
	// finalized only when a window is read.
	//
	// back holds backfilled windows keyed by window start: a sample that
	// lands before the newest window opens its bucket here, so a new
	// window costs one map insert instead of shifting a sorted tail and a
	// mass backfill stays linear. Once the number of backfilled windows
	// catches up with buckets, they are sorted once and folded into it,
	// amortizing that sort over the inserts. Every read consults both
	// buckets and back, and no window start appears in both at the same
	// time. backAgg holds back's exact totals the same way sums does.
	mu      sync.RWMutex
	id      uint64
	window  time.Duration
	buckets []Window
	sums    []*agg
	back    map[time.Time]Window
	backAgg map[time.Time]*agg
	latest  time.Time
	hasAny  bool
}

// idGen hands out unique roller IDs so Merge can lock two rollers in a
// consistent order and concurrent cross-merges cannot deadlock.
var idGen atomic.Uint64

// New builds a roller with the given window size. It panics with
// "rollup: bad window" if window is not positive.
func New(window time.Duration) *Roller {
	if window <= 0 {
		panic("rollup: bad window")
	}
	return &Roller{
		id:     idGen.Add(1),
		window: window,
	}
}

// startOf returns the start of the window containing at, aligned to an
// integer multiple of the window size since the Unix epoch.
func (r *Roller) startOf(at time.Time) time.Time {
	return alignStart(at, r.window.Nanoseconds())
}

// alignStart returns the start of the window of width w nanoseconds
// containing at, aligned to an integer multiple of w since the Unix epoch.
// The arithmetic is exact for any time value, including ones whose
// nanosecond offset from the epoch does not fit in an int64.
func alignStart(at time.Time, w int64) time.Time {
	sec := at.Unix()
	nsec := int64(at.Nanosecond())
	if sec >= -9223372036 && sec <= 9223372035 {
		// sec*1e9+nsec fits in an int64. Rounding a negative ns down to
		// a window multiple can reach ns-(w-1), so stay on the fast
		// path only when that product cannot overflow either.
		if ns := sec*1e9 + nsec; ns >= math.MinInt64+w-1 {
			start := ns / w
			if ns%w != 0 && ns < 0 {
				start--
			}
			return time.Unix(0, start*w).UTC()
		}
	}
	return windowStartWide(sec, nsec, w)
}

// windowStartWide is the 128-bit slow path of alignStart, for times whose
// nanosecond offset from the epoch overflows an int64.
func windowStartWide(sec, nsec, w int64) time.Time {
	// t = sec*1e9 + nsec as a 128-bit two's-complement value (hi, lo).
	uhi, ulo := bits.Mul64(uint64(sec), uint64(1e9))
	if sec < 0 {
		uhi -= 1e9
	}
	hi := int64(uhi)
	lo := ulo + uint64(nsec)
	if lo < ulo {
		hi++
	}
	// Round t down to a multiple of w: start = t - floorMod(t, w).
	rem := floorMod128(hi, lo, w)
	if lo < rem {
		hi--
	}
	lo -= rem
	// Split start into whole seconds and nanoseconds.
	ns := floorMod128(hi, lo, 1e9)
	if lo < ns {
		hi--
	}
	lo -= ns
	return time.Unix(quo128(hi, lo, 1e9), int64(ns)).UTC()
}

// floorMod128 returns t mod y for the signed 128-bit value t = (hi<<64)|lo
// and positive y, with the result in [0, y).
func floorMod128(hi int64, lo uint64, y int64) uint64 {
	yu := uint64(y)
	if hi >= 0 {
		return rem128(uint64(hi), lo, yu)
	}
	// Negate the 128-bit value, take the remainder, and reflect it back
	// into [0, y) for floored (not truncated) division.
	mlo := -lo
	mhi := -uint64(hi)
	if lo != 0 {
		mhi--
	}
	if r := rem128(mhi, mlo, yu); r != 0 {
		return yu - r
	}
	return 0
}

// quo128 returns (hi<<64|lo)/y for a signed 128-bit dividend known to be an
// exact multiple of y, with a quotient that fits in an int64.
func quo128(hi int64, lo uint64, y int64) int64 {
	yu := uint64(y)
	if hi >= 0 {
		q, _ := bits.Div64(uint64(hi), lo, yu)
		return int64(q)
	}
	mlo := -lo
	mhi := -uint64(hi)
	if lo != 0 {
		mhi--
	}
	q, _ := bits.Div64(mhi, mlo, yu)
	return -int64(q)
}

// rem128 returns (hi<<64 | lo) mod y for unsigned hi, lo and y > 0.
func rem128(hi, lo, y uint64) uint64 {
	if hi >= y {
		hi %= y
	}
	return bits.Rem64(hi, lo, y)
}

// windowIndex locates the window starting at start in ws, which must be
// sorted by window start. The second result reports whether it exists;
// when it does not, the index is where it would be inserted to keep ws
// sorted, i.e. the first window whose start is not before start.
func windowIndex(ws []Window, start time.Time) (int, bool) {
	sec, nsec := start.Unix(), int64(start.Nanosecond())
	lo, hi := 0, len(ws)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		s := ws[mid].Start
		if ss, sn := s.Unix(), int64(s.Nanosecond()); ss < sec || (ss == sec && sn < nsec) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(ws) {
		if s := ws[lo].Start; s.Unix() == sec && int64(s.Nanosecond()) == nsec {
			return lo, true
		}
	}
	return lo, false
}

// bucketAggLocked returns the accumulator for buckets[i], seeding and
// storing one from the bucket's fields if it entered the roller without
// an accumulator. The caller must hold mu.
func (r *Roller) bucketAggLocked(i int) *agg {
	for len(r.sums) < len(r.buckets) {
		r.sums = append(r.sums, nil)
	}
	if r.sums[i] == nil {
		r.sums[i] = seedAgg(r.buckets[i])
	}
	return r.sums[i]
}

// addTo folds one finite sample into w, keeping both the legacy running
// fields and the exact accumulator a. Count saturates at math.MaxInt64
// instead of overflowing; Sum, Min, and Max keep updating afterwards. The
// Sum field is provisional: reads replace it with the value finalized
// from a, and its zero signs follow a as well.
func addTo(w *Window, a *agg, value float64) {
	if w.Count < math.MaxInt64 {
		w.Count++
	}
	w.Sum += value
	a.add(value)
	if value < w.Min {
		w.Min = value
	}
	if value > w.Max {
		w.Max = value
	}
}

// Add files a sample into its window. A NaN or infinite value is refused
// with ErrNonFinite before anything else and leaves the roller, including
// its most-recent marker, unchanged. A sample earlier than the most recent
// accepted sample is rejected with ErrOutOfOrder and also leaves the
// roller unchanged; a sample at the same instant is accepted and counted
// again.
//
// Concurrent Adds are serialized: each takes effect in the order it
// acquires the roller, and the out-of-order check is measured against the
// samples accepted before it in that order.
func (r *Roller) Add(at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrNonFinite
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasAny && at.Before(r.latest) {
		return ErrOutOfOrder
	}
	r.fileLocked(at, value)
	r.latest = at
	r.hasAny = true
	return nil
}

// AddBatch files a batch of samples as one atomic unit: either every
// sample takes effect or none does. The samples may arrive in any order
// and are not checked against each other; several samples at the same
// instant are each counted. Each sample is filed into its epoch-aligned
// window exactly as Add would file it.
//
// If any sample holds a NaN or infinite value, the whole batch is
// rejected with ErrNonFinite before the ordering check. If any sample
// predates the most recent sample the roller has already accepted, the
// whole batch is rejected with ErrOutOfOrder; either rejection leaves the
// roller and its most-recent marker untouched. Otherwise the marker
// advances to the later of its previous value and the newest sample in
// the batch. A nil or empty batch is accepted, changes nothing, and does
// not advance the marker.
//
// The batch is atomic with respect to every other call on the roller:
// concurrent adds, batches, merges, range reads, and cursor creation
// observe the roller wholly before or wholly after the batch.
func (r *Roller) AddBatch(samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}
	// A non-finite value rejects the batch outright, before the
	// out-of-order check and without touching the roller.
	for _, s := range samples {
		if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
			return ErrNonFinite
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// One pass to validate the batch against the accepted marker and find
	// its newest instant; nothing is filed until every sample is known to
	// be acceptable, so a rejection leaves the roller untouched.
	latest := samples[0].At
	for _, s := range samples {
		if r.hasAny && s.At.Before(r.latest) {
			return ErrOutOfOrder
		}
		if s.At.After(latest) {
			latest = s.At
		}
	}
	for _, s := range samples {
		r.fileLocked(s.At, s.Value)
	}
	if r.hasAny && r.latest.After(latest) {
		latest = r.latest
	}
	r.latest = latest
	r.hasAny = true
	return nil
}

// fileLocked folds one finite sample into its window. The caller must
// hold mu and must already have cleared the out-of-order and non-finite
// checks.
func (r *Roller) fileLocked(at time.Time, value float64) {
	start := r.startOf(at)
	n := len(r.buckets)
	switch {
	case n == 0:
		w, a := newBucket(start, value)
		r.buckets = append(r.buckets, w)
		r.sums = append(r.sums, a)
	case !start.Before(r.buckets[n-1].Start):
		// The common case: the sample lands in the newest window or
		// just past it, so the sorted order is maintained for free.
		if r.buckets[n-1].Start.Equal(start) {
			addTo(&r.buckets[n-1], r.bucketAggLocked(n-1), value)
		} else {
			w, a := newBucket(start, value)
			r.buckets = append(r.buckets, w)
			r.sums = append(r.sums, a)
		}
	default:
		// The sample belongs to an older window, which can only happen
		// after a merge brought in windows past latest. A window that
		// already exists is updated in place; a new one goes into the
		// backfill map, so inserting it never shifts the sorted tail of
		// buckets and the cost of a mass backfill stays linear in the
		// number of windows accepted.
		if i, ok := windowIndex(r.buckets, start); ok {
			addTo(&r.buckets[i], r.bucketAggLocked(i), value)
		} else if w, ok := r.back[start]; ok {
			a := r.backAgg[start]
			if a == nil {
				a = seedAgg(w)
				r.backAgg[start] = a
			}
			addTo(&w, a, value)
			r.back[start] = w
		} else {
			if r.back == nil {
				r.back = make(map[time.Time]Window)
				r.backAgg = make(map[time.Time]*agg)
			}
			w, a := newBucket(start, value)
			r.back[start] = w
			r.backAgg[start] = a
			r.maybeFlushBack()
		}
	}
}

// newBucket returns a window holding a single finite sample, paired with
// its exact accumulator.
func newBucket(start time.Time, value float64) (Window, *agg) {
	a := newAgg()
	a.add(value)
	return Window{Start: start, Count: 1, Sum: value, Min: value, Max: value}, a
}

// maybeFlushBack folds the backfilled windows into buckets once they
// number at least half as many as buckets, so the sorting and merge cost
// amortizes to constant time per backfilled window. The caller must hold
// mu.
func (r *Roller) maybeFlushBack() {
	if len(r.back)*2 < len(r.buckets) {
		return
	}
	bw, ba := r.sortedBackLocked()
	r.buckets, r.sums = mergeWindows(r.buckets, r.sums, bw, ba, true)
	r.back = nil
	r.backAgg = nil
}

// sortedBackLocked returns the backfilled windows in time order, each
// paired with its accumulator. The caller must hold mu; the returned
// slices are a fresh snapshot.
func (r *Roller) sortedBackLocked() ([]Window, []*agg) {
	back := make([]Window, 0, len(r.back))
	aggs := make([]*agg, 0, len(r.back))
	for start, w := range r.back {
		a := r.backAgg[start]
		if a == nil {
			a = seedAgg(w)
		}
		back = append(back, w)
		aggs = append(aggs, a)
	}
	order := make([]int, len(back))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return back[order[i]].Start.Before(back[order[j]].Start)
	})
	sortedW := make([]Window, len(back))
	sortedA := make([]*agg, len(aggs))
	for i, k := range order {
		sortedW[i] = back[k]
		sortedA[i] = aggs[k]
	}
	return sortedW, sortedA
}

// aggAt returns the accumulator for entry i of ws, seeding one from the
// window when the side carries none (test-built buckets). It never
// mutates the passed slice: a seeded accumulator is returned fresh.
func aggAt(ag []*agg, i int, w Window) *agg {
	if i < len(ag) && ag[i] != nil {
		return ag[i]
	}
	return seedAgg(w)
}

// mergeWindows returns the sorted union of two window sides, each sorted
// by window start, combining statistics of windows that share a start:
// counts and sums add, minimums take the smaller, maximums take the
// larger.
//
// The a side is the destination's own windows and is being replaced by
// the caller, so its accumulator pointers are reused in place, which
// keeps a mass backfill linear with no per-carried-window allocation.
// The b side belongs to another roller (or, when ownB is true as in a
// backfill flush, to this same roller): a b-only window reuses its
// accumulator when ownB and otherwise gets a fresh deep copy, so another
// roller's state is never mutated or shared. Overlapping starts merge b
// into a's accumulator in place; b is never written.
func mergeWindows(a []Window, aa []*agg, b []Window, bb []*agg, ownB bool) ([]Window, []*agg) {
	merged := make([]Window, 0, len(a)+len(b))
	mergedAgg := make([]*agg, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].Start.Before(b[j].Start):
			merged = append(merged, a[i])
			mergedAgg = append(mergedAgg, aggAt(aa, i, a[i]))
			i++
		case b[j].Start.Before(a[i].Start):
			merged = append(merged, b[j])
			if ownB {
				mergedAgg = append(mergedAgg, aggAt(bb, j, b[j]))
			} else {
				x := newAgg()
				x.take(aggAt(bb, j, b[j]))
				mergedAgg = append(mergedAgg, x)
			}
			j++
		default:
			w := a[i]
			w.Count = satAdd(w.Count, b[j].Count)
			w.Sum += b[j].Sum
			if b[j].Min < w.Min {
				w.Min = b[j].Min
			}
			if b[j].Max > w.Max {
				w.Max = b[j].Max
			}
			x := aggAt(aa, i, a[i])
			x.merge(aggAt(bb, j, b[j]))
			merged = append(merged, w)
			mergedAgg = append(mergedAgg, x)
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		merged = append(merged, a[i])
		mergedAgg = append(mergedAgg, aggAt(aa, i, a[i]))
	}
	for ; j < len(b); j++ {
		merged = append(merged, b[j])
		if ownB {
			mergedAgg = append(mergedAgg, aggAt(bb, j, b[j]))
		} else {
			x := newAgg()
			x.take(aggAt(bb, j, b[j]))
			mergedAgg = append(mergedAgg, x)
		}
	}
	return merged, mergedAgg
}

// Window returns the bucket whose start matches start exactly. The second
// result is false when no bucket starts at that instant; no nearby bucket
// is substituted. The returned Window is a copy and stays valid no matter
// what is filed afterwards.
func (r *Roller) Window(start time.Time) (Window, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if i, ok := windowIndex(r.buckets, start); ok {
		return finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i])), true
	}
	key := start.UTC()
	if w, ok := r.back[key]; ok {
		return finalize(w, r.backAgg[key]), true
	}
	return Window{}, false
}

// windowsLocked returns every window in time order, finalized, merged
// from buckets and the backfill map. It is the read path: it allocates
// the single output window slice and nothing else, so tight snapshot
// loops cost no more than copying the windows. The caller must hold mu.
func (r *Roller) windowsLocked() []Window {
	if len(r.back) == 0 {
		ws := make([]Window, len(r.buckets))
		for i := range r.buckets {
			ws[i] = finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i]))
		}
		return ws
	}
	bw, ba := r.sortedBackLocked()
	ws := make([]Window, 0, len(r.buckets)+len(bw))
	i, j := 0, 0
	for i < len(r.buckets) && j < len(bw) {
		if bw[j].Start.Before(r.buckets[i].Start) {
			ws = append(ws, finalize(bw[j], ba[j]))
			j++
		} else {
			ws = append(ws, finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i])))
			i++
		}
	}
	for ; i < len(r.buckets); i++ {
		ws = append(ws, finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i])))
	}
	for ; j < len(bw); j++ {
		ws = append(ws, finalize(bw[j], ba[j]))
	}
	return ws
}

// snapshotLocked returns every window in time order paired with its live
// accumulator, merged from buckets and the backfill map. It is the merge
// path: the accumulators are read-only here and folded into fresh copies
// by mergeWindows. The caller must hold mu.
//
// buckets and the backfill map are disjoint in window starts by
// construction, so their union is a plain interleave: no statistics ever
// combine here.
func (r *Roller) snapshotLocked() ([]Window, []*agg) {
	if len(r.back) == 0 {
		ws := make([]Window, len(r.buckets))
		copy(ws, r.buckets)
		return ws, r.sums
	}
	bw, ba := r.sortedBackLocked()
	ws := make([]Window, 0, len(r.buckets)+len(bw))
	as := make([]*agg, 0, len(r.buckets)+len(bw))
	i, j := 0, 0
	for i < len(r.buckets) && j < len(bw) {
		if bw[j].Start.Before(r.buckets[i].Start) {
			ws = append(ws, bw[j])
			as = append(as, ba[j])
			j++
		} else {
			ws = append(ws, r.buckets[i])
			as = append(as, aggAt(r.sums, i, r.buckets[i]))
			i++
		}
	}
	for ; i < len(r.buckets); i++ {
		ws = append(ws, r.buckets[i])
		as = append(as, aggAt(r.sums, i, r.buckets[i]))
	}
	ws = append(ws, bw[j:]...)
	as = append(as, ba[j:]...)
	return ws, as
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice. The result is a snapshot of one moment: samples filed after
// the call do not alter it.
func (r *Roller) Windows() []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.windowsLocked()
}

// Range returns the buckets that overlap the half-open interval
// [from, to), in time order. The endpoints are attributed to windows by
// the same epoch-aligned, left-closed rule Add uses: the first bucket
// returned is the one containing from, and a bucket starting exactly at
// to is excluded. An interval whose end does not come after its start
// yields an empty slice, as does an interval no bucket overlaps. The
// result is a snapshot of one moment: samples filed after the call do
// not alter it.
func (r *Roller) Range(from, to time.Time) []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []Window{}
	if !from.Before(to) {
		return out
	}
	lo := r.startOf(from)
	i, _ := windowIndex(r.buckets, lo)
	if len(r.back) == 0 {
		for ; i < len(r.buckets); i++ {
			if !r.buckets[i].Start.Before(to) {
				break
			}
			out = append(out, finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i])))
		}
		return out
	}
	bw, ba := r.sortedBackLocked()
	j, _ := windowIndex(bw, lo)
	for i < len(r.buckets) || j < len(bw) {
		var w Window
		switch {
		case j >= len(bw) || (i < len(r.buckets) && r.buckets[i].Start.Before(bw[j].Start)):
			w = finalize(r.buckets[i], aggAt(r.sums, i, r.buckets[i]))
			i++
		default:
			w = finalize(bw[j], ba[j])
			j++
		}
		if !w.Start.Before(to) {
			break
		}
		out = append(out, w)
	}
	return out
}

// Cursor reads a fixed range of windows in batches. The windows are
// snapshot when the cursor is created: samples filed, backfilled, or
// merged in afterwards do not alter what the cursor returns, so batched
// iteration over a large range never tears, skips, or repeats a window,
// and the end of the range is reached deterministically no matter what
// is written while the cursor advances.
//
// A Cursor is safe to advance from multiple goroutines at once; each
// batch is handed out exactly once.
type Cursor struct {
	// mu guards pos. windows is fixed at creation and read without the
	// lock.
	mu      sync.Mutex
	windows []Window
	pos     int
}

// Cursor returns a cursor over the buckets overlapping the half-open
// interval [from, to), selected by the same rule as Range.
func (r *Roller) Cursor(from, to time.Time) *Cursor {
	return &Cursor{windows: r.Range(from, to)}
}

// Next returns the next batch of up to n windows, in time order. Batches
// are consecutive and non-overlapping: together they cover exactly the
// windows in the cursor's range. Once the range is exhausted, Next
// returns an empty slice and the cursor does not move. A non-positive n
// yields an empty slice and does not advance the cursor.
func (c *Cursor) Next(n int) []Window {
	c.mu.Lock()
	defer c.mu.Unlock()
	rest := len(c.windows) - c.pos
	if n <= 0 || rest == 0 {
		return []Window{}
	}
	if n > rest {
		n = rest
	}
	out := make([]Window, n)
	copy(out, c.windows[c.pos:c.pos+n])
	c.pos += n
	return out
}

// Merge folds other's buckets into this roller: counts and sums add,
// minimums take the smaller, maximums take the larger, and buckets only one
// side has are carried over as-is. Merging does not affect out-of-order
// tracking. It panics with "rollup: nil roller" if other is nil, and
// returns ErrWindowMismatch, leaving this roller unchanged, if the window
// sizes differ.
//
// The merge is atomic with respect to every other call on either roller:
// concurrent readers see this roller wholly before or wholly after the
// merge, and a sample filed into other while the merge runs is either
// folded in completely or not at all. If the two rollers merge each other
// at the same time, the outcome matches some serial order of the two
// merges; neither is lost.
func (r *Roller) Merge(other *Roller) error {
	if other == nil {
		panic("rollup: nil roller")
	}
	if other.window != r.window {
		return ErrWindowMismatch
	}
	if r == other {
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.back) != 0 {
			bw, ba := r.sortedBackLocked()
			r.buckets, r.sums = mergeWindows(r.buckets, r.sums, bw, ba, true)
			r.back = nil
			r.backAgg = nil
		}
		for i := range r.buckets {
			w := &r.buckets[i]
			w.Count = satAdd(w.Count, w.Count)
			w.Sum += w.Sum
			r.bucketAggLocked(i).double()
		}
		return nil
	}
	// Lock both rollers in ID order so simultaneous merges in
	// opposite directions cannot deadlock.
	first, second := r, other
	if other.id < r.id {
		first, second = other, r
	}
	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	if len(other.buckets) == 0 && len(other.back) == 0 {
		return nil
	}
	right, rightAgg := other.snapshotLocked()
	if len(r.buckets) == 0 && len(r.back) == 0 {
		// Adopt other's windows, but own independent accumulators so the
		// two rollers never share one.
		r.buckets = right
		r.sums = make([]*agg, len(rightAgg))
		for k, x := range rightAgg {
			c := newAgg()
			c.take(x)
			r.sums[k] = c
		}
		return nil
	}
	left, leftAgg := r.snapshotLocked()
	// Both sides are sorted by window start, so the merge is one linear
	// pass and one backing array, no per-bucket copies or lookups.
	r.buckets, r.sums = mergeWindows(left, leftAgg, right, rightAgg, false)
	r.back = nil
	r.backAgg = nil
	return nil
}

// satAdd returns a + b saturated at math.MaxInt64 instead of overflowing.
func satAdd(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}
