// Package rollup downsamples time series samples into fixed-size time
// windows aligned to the Unix epoch.
package rollup

import (
	"errors"
	"sort"
	"time"
)

// Window is the aggregate of every sample accepted into one fixed-size
// bucket. Start is the UTC instant the bucket starts at, Count is the
// number of distinct samples in it and Sum, Min and Max are the sum,
// minimum and maximum of their values.
type Window struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

var (
	// ErrOutOfOrder is returned when a sample is filed earlier than an
	// already accepted sample, or at the same timestamp with a
	// different value.
	ErrOutOfOrder = errors.New("out of order")

	// ErrWindowMismatch is returned when Merge is given a nil roller or
	// a roller whose window size differs from the receiver's.
	ErrWindowMismatch = errors.New("window size mismatch")
)

// Roller accumulates samples into windows. It is safe for neither
// concurrent use nor copying after use.
type Roller struct {
	window  time.Duration
	buckets map[int64]Window

	lastAt    time.Time
	lastValue float64
	hasLast   bool
}

// New returns a roller with the given fixed window size. It panics when
// window is not positive.
func New(window time.Duration) *Roller {
	if window <= 0 {
		panic("rollup: window must be positive")
	}
	return &Roller{
		window:  window,
		buckets: make(map[int64]Window),
	}
}

// startNano floors a timestamp to its window start, expressed as Unix
// nanoseconds, so timestamps before the epoch align downward as well.
func startNano(at time.Time, window time.Duration) int64 {
	n := at.UnixNano()
	d := int64(window)
	q := n / d
	if r := n % d; r != 0 && n < 0 {
		q--
	}
	return q * d
}

// Add files a sample into the window containing at. An exact duplicate
// (same timestamp and value) is counted once; a sample earlier than an
// already accepted one, or one sharing its timestamp with a different
// value, is rejected with ErrOutOfOrder.
func (r *Roller) Add(at time.Time, value float64) error {
	if r.hasLast {
		switch {
		case at.Before(r.lastAt):
			return ErrOutOfOrder
		case at.Equal(r.lastAt) && value != r.lastValue:
			return ErrOutOfOrder
		case at.Equal(r.lastAt):
			// Same timestamp and value: an exact duplicate, already
			// counted. Equal timestamps are always consecutive because
			// nothing can be filed between two equal instants.
			return nil
		}
	}

	key := startNano(at, r.window)
	w, ok := r.buckets[key]
	if !ok {
		w = Window{
			Start: time.Unix(0, key).UTC(),
			Count: 1,
			Sum:   value,
			Min:   value,
			Max:   value,
		}
	} else {
		w.Count++
		w.Sum += value
		if value < w.Min {
			w.Min = value
		}
		if value > w.Max {
			w.Max = value
		}
	}

	r.buckets[key] = w
	r.lastAt = at
	r.lastValue = value
	r.hasLast = true
	return nil
}

// Window returns the bucket starting exactly at start and true. For any
// other instant, including the middle of an existing bucket, it returns
// the zero Window and false.
func (r *Roller) Window(start time.Time) (Window, bool) {
	w, ok := r.buckets[start.UnixNano()]
	return w, ok
}

// Windows returns every non-empty bucket ordered by Start. It returns
// an empty slice when no samples have been accepted.
func (r *Roller) Windows() []Window {
	keys := make([]int64, 0, len(r.buckets))
	for key := range r.buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]Window, 0, len(keys))
	for _, key := range keys {
		out = append(out, r.buckets[key])
	}
	return out
}

// Merge folds every bucket of other into the receiver, combining Count,
// Sum, Min and Max where buckets overlap. It returns ErrWindowMismatch
// when other is nil or uses a different window size.
func (r *Roller) Merge(other *Roller) error {
	if other == nil || other.window != r.window {
		return ErrWindowMismatch
	}
	if other == r {
		return nil
	}

	for key, ow := range other.buckets {
		if w, ok := r.buckets[key]; ok {
			w.Count += ow.Count
			w.Sum += ow.Sum
			if ow.Min < w.Min {
				w.Min = ow.Min
			}
			if ow.Max > w.Max {
				w.Max = ow.Max
			}
			r.buckets[key] = w
		} else {
			r.buckets[key] = ow
		}
	}
	return nil
}
