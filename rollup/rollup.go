// Package rollup downsamples a time series into fixed windows aligned to
// the Unix epoch, and merges rollups that share a window size.
package rollup

import (
	"errors"
	"sort"
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
type Roller struct {
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
func (r *Roller) Add(at time.Time, value float64) error {
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
// is substituted.
func (r *Roller) Window(start time.Time) (Window, bool) {
	w, ok := r.buckets[start.UnixNano()]
	if !ok {
		return Window{}, false
	}
	return *w, true
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice.
func (r *Roller) Windows() []Window {
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
// returns ErrWindowMismatch, leaving this roller unchanged, if the window
// sizes differ.
func (r *Roller) Merge(other *Roller) error {
	if other == nil {
		panic("rollup: nil roller")
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
