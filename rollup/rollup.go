// Package rollup downsamples a time series into fixed windows aligned to
// the Unix epoch, and merges rollups that share a window size.
//
// A Roller is safe for concurrent use: samples may be filed, windows read,
// and rollers merged from multiple goroutines at once. Every read observes
// an internally consistent snapshot, and a merge is atomic — other callers
// see the state either before or after it, never a half-applied fold.
package rollup

import (
	"errors"
	"sort"
	"sync"
	"time"
	"unsafe"
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

// Roller aggregates samples into fixed windows of a single size. The zero
// value is not usable; build one with New. A Roller is safe for concurrent
// use by multiple goroutines.
type Roller struct {
	mu      sync.Mutex
	window  time.Duration
	buckets map[int64]*Window
	latest  time.Time
	hasAny  bool
}

// New builds a roller with the given window size. It panics with
// "rollup: bad window" if window is not positive.
func New(window time.Duration) *Roller {
	if window <= 0 {
		panic("rollup: bad window")
	}
	return &Roller{
		window:  window,
		buckets: make(map[int64]*Window),
	}
}

// startOf returns the start of the window containing at, aligned to an
// integer multiple of the window size since the Unix epoch.
func (r *Roller) startOf(at time.Time) int64 {
	ns := at.UnixNano()
	w := r.window.Nanoseconds()
	start := ns / w
	if ns%w != 0 && ns < 0 {
		start--
	}
	return start * w
}

// Add files a sample into its window. A sample earlier than the most recent
// accepted sample is rejected with ErrOutOfOrder and leaves the roller
// unchanged; a sample at the same instant is accepted and counted again.
//
// Under concurrency, "most recent" is decided by the order in which calls
// take effect, not by the order callers were invoked in.
func (r *Roller) Add(at time.Time, value float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasAny && at.Before(r.latest) {
		return ErrOutOfOrder
	}
	start := r.startOf(at)
	w, ok := r.buckets[start]
	if !ok {
		w = &Window{
			Start: time.Unix(0, start).UTC(),
			Min:   value,
			Max:   value,
		}
		r.buckets[start] = w
	}
	w.Count++
	w.Sum += value
	if value < w.Min {
		w.Min = value
	}
	if value > w.Max {
		w.Max = value
	}
	r.latest = at
	r.hasAny = true
	return nil
}

// Window returns the bucket whose start matches start exactly. The second
// result is false when no bucket starts at that instant; no nearby bucket
// is substituted. The returned Window is a copy and stays valid no matter
// what is filed afterwards.
func (r *Roller) Window(start time.Time) (Window, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.buckets[start.UnixNano()]
	if !ok {
		return Window{}, false
	}
	return *w, true
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice. The result is a snapshot of one moment: samples filed after
// the call do not alter it.
func (r *Roller) Windows() []Window {
	r.mu.Lock()
	defer r.mu.Unlock()
	starts := make([]int64, 0, len(r.buckets))
	for start := range r.buckets {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	out := make([]Window, 0, len(starts))
	for _, start := range starts {
		out = append(out, *r.buckets[start])
	}
	return out
}

// Merge folds other's buckets into this roller: counts and sums add,
// minimums take the smaller, maximums take the larger, and buckets only one
// side has are carried over as-is. Merging does not affect out-of-order
// tracking. It panics with "rollup: nil roller" if other is nil, and
// returns ErrWindowMismatch, leaving both rollers unchanged, if the window
// sizes differ.
//
// The fold is atomic: concurrent readers of either roller observe the state
// before the merge or after it, never a partial result, and a sample filed
// concurrently lands entirely before or entirely after the fold. Two
// rollers merging each other at the same time behave as if the merges ran
// in some serial order; neither fold is lost.
func (r *Roller) Merge(other *Roller) error {
	if other == nil {
		panic("rollup: nil roller")
	}
	if r == other {
		r.mu.Lock()
		defer r.mu.Unlock()
	} else if less(r, other) {
		r.mu.Lock()
		defer r.mu.Unlock()
		other.mu.Lock()
		defer other.mu.Unlock()
	} else {
		other.mu.Lock()
		defer other.mu.Unlock()
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	if other.window != r.window {
		return ErrWindowMismatch
	}
	for start, ow := range other.buckets {
		w, ok := r.buckets[start]
		if !ok {
			cp := *ow
			r.buckets[start] = &cp
			continue
		}
		w.Count += ow.Count
		w.Sum += ow.Sum
		if ow.Min < w.Min {
			w.Min = ow.Min
		}
		if ow.Max > w.Max {
			w.Max = ow.Max
		}
	}
	return nil
}

// less orders rollers by identity so that a merge always takes the two
// locks in the same order no matter which side initiated it, keeping
// simultaneous cross-merges deadlock-free.
func less(a, b *Roller) bool {
	return uintptr(unsafe.Pointer(a)) < uintptr(unsafe.Pointer(b))
}
