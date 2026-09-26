// Package rollup downsamples a time series into fixed windows aligned to
// the Unix epoch, and merges rollups that share a window size.
//
// A Roller is safe for concurrent use: samples may be filed, windows read,
// and rollers merged from multiple goroutines at once. Every read observes
// an internally consistent snapshot, and a merge is visible to other
// callers either in full or not at all.
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
	// mu guards buckets, latest, and hasAny. window and id are fixed at
	// construction and read without the lock.
	//
	// buckets is one flat slice kept sorted by window start: samples
	// arrive in non-decreasing time order, so Add appends in amortized
	// constant time, Windows reads out without re-sorting, and Merge is
	// a single linear pass over both sides.
	mu      sync.RWMutex
	id      uint64
	window  time.Duration
	buckets []Window
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
		// after a merge brought in windows past latest.
		if i, ok := r.find(start); ok {
			addTo(&r.buckets[i], value)
		} else {
			r.buckets = append(r.buckets, Window{})
			copy(r.buckets[i+1:], r.buckets[i:])
			r.buckets[i] = newBucket(start, value)
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
	return Window{}, false
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice. The result is a snapshot of one moment: samples filed after
// the call do not alter it.
func (r *Roller) Windows() []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Window, len(r.buckets))
	copy(out, r.buckets)
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
		for i := range r.buckets {
			w := &r.buckets[i]
			w.Count = satAdd(w.Count, w.Count)
			w.Sum += w.Sum
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

	switch {
	case len(other.buckets) == 0:
		return nil
	case len(r.buckets) == 0:
		r.buckets = append(r.buckets, other.buckets...)
		return nil
	}
	// Both sides are sorted by window start, so the merge is one linear
	// pass and one backing array, no per-bucket copies or lookups.
	merged := make([]Window, 0, len(r.buckets)+len(other.buckets))
	i, j := 0, 0
	for i < len(r.buckets) && j < len(other.buckets) {
		a, b := &r.buckets[i], &other.buckets[j]
		switch {
		case a.Start.Before(b.Start):
			merged = append(merged, *a)
			i++
		case b.Start.Before(a.Start):
			merged = append(merged, *b)
			j++
		default:
			w := *a
			w.Count = satAdd(w.Count, b.Count)
			w.Sum += b.Sum
			if b.Min < w.Min {
				w.Min = b.Min
			}
			if b.Max > w.Max {
				w.Max = b.Max
			}
			merged = append(merged, w)
			i++
			j++
		}
	}
	merged = append(merged, r.buckets[i:]...)
	merged = append(merged, other.buckets[j:]...)
	r.buckets = merged
	return nil
}

// satAdd returns a + b saturated at math.MaxInt64 instead of overflowing.
func satAdd(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}
