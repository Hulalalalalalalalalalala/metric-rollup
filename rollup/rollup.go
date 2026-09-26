// Package rollup downsamples a time series into fixed windows aligned to
// the Unix epoch, and merges rollups that share a window size.
//
// A Roller is safe for concurrent use: samples may be filed, windows read,
// and rollers merged from multiple goroutines at once. Every read observes
// an internally consistent snapshot, and a merge is visible to other
// callers either in full or not at all. A Cursor sees the windows exactly
// as they were when the cursor was created, no matter what is filed or
// merged while it is drained.
package rollup

import (
	"errors"
	"math"
	"math/bits"
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
	// mu guards buckets, side, latest, and hasAny. window and id are fixed
	// at construction and read without the lock.
	//
	// buckets is the main store: one flat slice kept sorted by window
	// start. Samples arrive in non-decreasing time order, so Add appends
	// in amortized constant time and Merge is a single linear pass.
	//
	// side holds backfilled windows — ones older than the newest bucket,
	// filed after a merge brought in later windows — in a treap keyed by
	// window start, so a backfill insert never shifts the accepted tail
	// of buckets. Every window start lives in exactly one of the two
	// stores, and reads merge them back into one sorted sequence.
	mu      sync.RWMutex
	id      uint64
	window  time.Duration
	buckets []Window
	side    *backNode
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
// integer multiple of the window size since the Unix epoch. The arithmetic
// is exact for any time value, including ones whose nanosecond offset from
// the epoch does not fit in an int64.
func (r *Roller) startOf(at time.Time) time.Time {
	sec := at.Unix()
	nsec := int64(at.Nanosecond())
	w := r.window.Nanoseconds()
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

// windowStartWide is the 128-bit slow path of startOf, for times whose
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

// compareStart orders two window starts by wall clock alone, ignoring any
// monotonic reading the times may carry.
func compareStart(a, b time.Time) int {
	if as, bs := a.Unix(), b.Unix(); as != bs {
		if as < bs {
			return -1
		}
		return 1
	}
	if an, bn := int64(a.Nanosecond()), int64(b.Nanosecond()); an != bn {
		if an < bn {
			return -1
		}
		return 1
	}
	return 0
}

// find locates the bucket starting at start. The second result reports
// whether it exists; when it does not, the index is where it would be
// inserted to keep buckets sorted.
func (r *Roller) find(start time.Time) (int, bool) {
	sec, nsec := start.Unix(), int64(start.Nanosecond())
	lo, hi := 0, len(r.buckets)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		s := r.buckets[mid].Start
		if ss, sn := s.Unix(), int64(s.Nanosecond()); ss < sec || (ss == sec && sn < nsec) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(r.buckets) {
		if s := r.buckets[lo].Start; s.Unix() == sec && int64(s.Nanosecond()) == nsec {
			return lo, true
		}
	}
	return lo, false
}

// backNode is one node of the treap behind Roller.side: a binary search
// tree keyed by window start, heap-ordered by a priority hashed from the
// start, so it stays balanced no matter what order backfills arrive in.
// Inserting or updating a backfilled window touches O(log n) nodes and
// never moves the accepted tail of buckets.
type backNode struct {
	w           Window
	prio        uint64
	left, right *backNode
}

// backPrio derives a deterministic heap priority from a window start.
func backPrio(start time.Time) uint64 {
	x := uint64(start.Unix())*0x9E3779B97F4A7C15 + uint64(int64(start.Nanosecond()))
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

// find returns the node holding the window that starts at start, or nil.
func (n *backNode) find(start time.Time) *backNode {
	for n != nil {
		switch c := compareStart(start, n.w.Start); {
		case c < 0:
			n = n.left
		case c > 0:
			n = n.right
		default:
			return n
		}
	}
	return nil
}

// insert adds w to the treap and returns the new root. The caller must
// have checked that no node starts at w.Start.
func (n *backNode) insert(w Window, prio uint64) *backNode {
	if n == nil {
		return &backNode{w: w, prio: prio}
	}
	if compareStart(w.Start, n.w.Start) < 0 {
		n.left = n.left.insert(w, prio)
		if n.left.prio < n.prio {
			n = rotateRight(n)
		}
	} else {
		n.right = n.right.insert(w, prio)
		if n.right.prio < n.prio {
			n = rotateLeft(n)
		}
	}
	return n
}

func rotateRight(n *backNode) *backNode {
	l := n.left
	n.left = l.right
	l.right = n
	return l
}

func rotateLeft(n *backNode) *backNode {
	r := n.right
	n.right = r.left
	r.left = n
	return r
}

// appendTo appends every window in the treap to out, in start order.
func (n *backNode) appendTo(out []Window) []Window {
	if n == nil {
		return out
	}
	out = n.left.appendTo(out)
	out = append(out, n.w)
	return n.right.appendTo(out)
}

// appendRange appends every window whose start lies in [from, to) to out,
// in start order.
func (n *backNode) appendRange(out []Window, from, to time.Time) []Window {
	if n == nil {
		return out
	}
	if compareStart(n.w.Start, from) >= 0 {
		out = n.left.appendRange(out, from, to)
		if compareStart(n.w.Start, to) < 0 {
			out = append(out, n.w)
		}
	}
	if compareStart(n.w.Start, to) < 0 {
		out = n.right.appendRange(out, from, to)
	}
	return out
}

// each calls fn on every window in the treap, in start order.
func (n *backNode) each(fn func(*Window)) {
	if n == nil {
		return
	}
	n.left.each(fn)
	fn(&n.w)
	n.right.each(fn)
}

// addTo folds one sample into w. Count saturates at math.MaxInt64 instead
// of overflowing; Sum, Min, and Max keep updating afterwards.
func addTo(w *Window, value float64) {
	if w.Count < math.MaxInt64 {
		w.Count++
	}
	w.Sum += value
	if value < w.Min {
		w.Min = value
	}
	if value > w.Max {
		w.Max = value
	}
}

// Add files a sample into its window. A sample earlier than the most recent
// accepted sample is rejected with ErrOutOfOrder and leaves the roller
// unchanged; a sample at the same instant is accepted and counted again.
//
// Concurrent Adds are serialized: each takes effect in the order it
// acquires the roller, and the out-of-order check is measured against the
// samples accepted before it in that order.
func (r *Roller) Add(at time.Time, value float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasAny && at.Before(r.latest) {
		return ErrOutOfOrder
	}
	start := r.startOf(at)
	n := len(r.buckets)
	switch {
	case n == 0:
		r.buckets = append(r.buckets, newBucket(start, value))
	case !start.Before(r.buckets[n-1].Start):
		// The common case: the sample lands in the newest window or
		// just past it, so the sorted order is maintained for free.
		if r.buckets[n-1].Start.Equal(start) {
			addTo(&r.buckets[n-1], value)
		} else {
			r.buckets = append(r.buckets, newBucket(start, value))
		}
	default:
		// The sample belongs to an older window, which can only happen
		// after a merge brought in windows past latest. Backfills land in
		// the side treap, so filing one never shifts the accepted tail.
		if i, ok := r.find(start); ok {
			addTo(&r.buckets[i], value)
		} else if bn := r.side.find(start); bn != nil {
			addTo(&bn.w, value)
		} else {
			r.side = r.side.insert(newBucket(start, value), backPrio(start))
		}
	}
	r.latest = at
	r.hasAny = true
	return nil
}

// newBucket returns a window holding a single sample.
func newBucket(start time.Time, value float64) Window {
	return Window{Start: start, Count: 1, Sum: value, Min: value, Max: value}
}

// Window returns the bucket whose start matches start exactly. The second
// result is false when no bucket starts at that instant; no nearby bucket
// is substituted. The returned Window is a copy and stays valid no matter
// what is filed afterwards.
func (r *Roller) Window(start time.Time) (Window, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if i, ok := r.find(start); ok {
		return r.buckets[i], true
	}
	if bn := r.side.find(start); bn != nil {
		return bn.w, true
	}
	return Window{}, false
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice. The result is a snapshot of one moment: samples filed after
// the call do not alter it.
func (r *Roller) Windows() []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sortedLocked()
}

// Range returns the buckets whose start lies in [from, to), in time order.
// The bounds follow the same left-closed, right-open convention as the
// windows themselves: a from that lands exactly on a window start includes
// that window, a to that lands exactly on one excludes it. An interval
// holding no windows, from == to, and from after to all yield an empty
// slice; none of them is an error. The result is a snapshot of one moment:
// samples filed after the call do not alter it.
func (r *Roller) Range(from, to time.Time) []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !from.Before(to) {
		return []Window{}
	}
	// find returns the insertion point for a start, i.e. the index of the
	// first bucket at or past the bound.
	lo, _ := r.find(from)
	hi, _ := r.find(to)
	main := r.buckets[lo:hi]
	if r.side == nil {
		out := make([]Window, len(main))
		copy(out, main)
		return out
	}
	return unionWindows(main, r.side.appendRange(nil, from, to))
}

// sortedLocked returns every window in time order. The caller must hold
// the lock. The result shares no memory with the roller.
func (r *Roller) sortedLocked() []Window {
	if r.side == nil {
		out := make([]Window, len(r.buckets))
		copy(out, r.buckets)
		return out
	}
	return unionWindows(r.buckets, r.side.appendTo(nil))
}

// unionWindows merges two sorted window slices with no shared starts into
// one freshly allocated sorted slice.
func unionWindows(a, b []Window) []Window {
	out := make([]Window, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if compareStart(a[i].Start, b[j].Start) < 0 {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// A Cursor reads a roller's windows out in batches instead of all at once.
// It captures every window the roller holds when the cursor is created and
// serves that snapshot from then on: samples filed, backfilled, or merged
// afterwards are not visible to it, so a drained cursor can never tear a
// window, skip one, or repeat one. A Cursor is not safe for concurrent use
// by multiple goroutines.
type Cursor struct {
	windows []Window
	pos     int
}

// Cursor returns a cursor over a snapshot of the roller's windows, taken
// at the moment of the call.
func (r *Roller) Cursor() *Cursor {
	return &Cursor{windows: r.Windows()}
}

// Next returns the next batch of up to n windows, in time order, and
// advances the cursor past them. Successive batches are complete and never
// overlap. Once the snapshot is exhausted, Next returns an empty slice and
// the cursor stays put. A non-positive n yields an empty slice without
// advancing.
func (c *Cursor) Next(n int) []Window {
	if n < 0 {
		n = 0
	}
	end := c.pos + n
	if end > len(c.windows) {
		end = len(c.windows)
	}
	out := make([]Window, end-c.pos)
	copy(out, c.windows[c.pos:end])
	c.pos = end
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
		double := func(w *Window) {
			w.Count = satAdd(w.Count, w.Count)
			w.Sum += w.Sum
		}
		for i := range r.buckets {
			double(&r.buckets[i])
		}
		r.side.each(double)
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

	if len(other.buckets) == 0 && other.side == nil {
		return nil
	}
	// Both stores on each side are sorted by window start, so the merge is
	// one linear pass and one backing array, no per-bucket copies or
	// lookups. The merged result becomes the new main store and the side
	// treap is folded away.
	ours := r.buckets
	if r.side != nil {
		ours = unionWindows(r.buckets, r.side.appendTo(nil))
	}
	theirs := other.buckets
	if other.side != nil {
		theirs = unionWindows(other.buckets, other.side.appendTo(nil))
	}
	r.buckets = mergeWindows(ours, theirs)
	r.side = nil
	return nil
}

// mergeWindows combines two sorted window slices into a freshly allocated
// sorted slice, folding windows that share a start into one: counts and
// sums add, minimums take the smaller, maximums take the larger.
func mergeWindows(a, b []Window) []Window {
	merged := make([]Window, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := compareStart(a[i].Start, b[j].Start); {
		case c < 0:
			merged = append(merged, a[i])
			i++
		case c > 0:
			merged = append(merged, b[j])
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
			merged = append(merged, w)
			i++
			j++
		}
	}
	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)
	return merged
}

// satAdd returns a + b saturated at math.MaxInt64 instead of overflowing.
func satAdd(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}
