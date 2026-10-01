// Package rollup downsamples a time series into fixed-size windows aligned to
// the Unix epoch.
//
// One roller uses one window size and holds windows in memory only. Samples
// must arrive in non-decreasing time order: an exact duplicate (same time and
// value) is counted once, while the same time carrying a different value, or
// any sample earlier than an accepted one, is reported as ErrOutOfOrder.
package rollup

import (
	"errors"
	"sort"
	"time"
)

var (
	// ErrOutOfOrder is returned when a sample arrives earlier than an already
	// accepted sample, or when the same timestamp carries a different value.
	ErrOutOfOrder = errors.New("rollup: sample out of order")

	// ErrWindowMismatch is returned when Merge receives nil or a roller using
	// a different window size.
	ErrWindowMismatch = errors.New("rollup: window size mismatch")
)

// Window is the aggregate of every sample accepted into one fixed time bucket.
type Window struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Roller accumulates samples into windows of one fixed size. The zero value is
// not usable; create one with New.
type Roller struct {
	window  time.Duration
	buckets map[int64]*Window

	lastAt    time.Time
	lastValue float64
	haveLast  bool
}

// New returns a roller that buckets samples into windows of the given size.
// It panics when window is not positive.
func New(window time.Duration) *Roller {
	if window <= 0 {
		panic("rollup: window must be positive")
	}
	return &Roller{
		window:  window,
		buckets: make(map[int64]*Window),
	}
}

// Add files one sample into the window covering at. Samples must be offered in
// non-decreasing time order. A repeat of the last time and value is a no-op;
// an earlier time or a new value for the last time returns ErrOutOfOrder.
func (r *Roller) Add(at time.Time, value float64) error {
	if r.haveLast {
		switch {
		case at.Before(r.lastAt):
			return ErrOutOfOrder
		case at.Equal(r.lastAt):
			if value == r.lastValue {
				return nil
			}
			return ErrOutOfOrder
		}
	}

	r.lastAt = at
	r.lastValue = value
	r.haveLast = true

	b := r.bucket(at)
	if b == nil {
		r.buckets[r.index(at)] = &Window{
			Start: r.start(at),
			Count: 1,
			Sum:   value,
			Min:   value,
			Max:   value,
		}
		return nil
	}
	b.Count++
	b.Sum += value
	if value < b.Min {
		b.Min = value
	}
	if value > b.Max {
		b.Max = value
	}
	return nil
}

// Window returns the bucket whose aligned start time equals start, together
// with true. For any other time, including a time merely inside an existing
// window, it returns the zero Window and false.
func (r *Roller) Window(start time.Time) (Window, bool) {
	n := start.UnixNano()
	w := int64(r.window)
	q := floorDiv(n, w)
	if q*w != n {
		return Window{}, false
	}
	b, ok := r.buckets[q]
	if !ok {
		return Window{}, false
	}
	return *b, true
}

// Windows returns all populated windows ordered by Start. It returns an empty
// (non-nil) slice when no sample has been accepted.
func (r *Roller) Windows() []Window {
	out := make([]Window, 0, len(r.buckets))
	for _, b := range r.buckets {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// Merge folds every window of other into r, combining counts, sums, minima and
// maxima for overlapping windows. It returns ErrWindowMismatch when other is
// nil or uses a different window size.
func (r *Roller) Merge(other *Roller) error {
	if other == nil || other.window != r.window {
		return ErrWindowMismatch
	}
	if other == r {
		return nil
	}
	for q, ob := range other.buckets {
		if b, ok := r.buckets[q]; ok {
			b.Count += ob.Count
			b.Sum += ob.Sum
			if ob.Min < b.Min {
				b.Min = ob.Min
			}
			if ob.Max > b.Max {
				b.Max = ob.Max
			}
			continue
		}
		cp := *ob
		r.buckets[q] = &cp
	}
	return nil
}

// index returns the epoch-aligned bucket index for t.
func (r *Roller) index(t time.Time) int64 {
	return floorDiv(t.UnixNano(), int64(r.window))
}

// start returns the aligned UTC start time of the bucket covering t.
func (r *Roller) start(t time.Time) time.Time {
	w := int64(r.window)
	q := floorDiv(t.UnixNano(), w)
	return time.Unix(0, q*w).UTC()
}

func (r *Roller) bucket(t time.Time) *Window {
	return r.buckets[r.index(t)]
}

// floorDiv divides n by d rounding toward negative infinity, so bucket
// boundaries stay aligned to the epoch for times before 1970.
func floorDiv(n, d int64) int64 {
	q := n / d
	if (n % d) < 0 {
		q--
	}
	return q
}
