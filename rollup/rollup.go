// Package rollup downsamples a time series into fixed-size windows aligned to
// the Unix epoch. Window boundaries are left-closed and right-open: a sample
// whose timestamp equals a window end belongs to the following window.
package rollup

import (
	"errors"
	"math"
	"sort"
	"time"
)

var (
	// ErrOutOfOrder is returned when a sample is older than the newest sample
	// already accepted by the roller.
	ErrOutOfOrder = errors.New("rollup: sample out of order")
	// ErrWindowMismatch is returned when merging rollers with different
	// window lengths.
	ErrWindowMismatch = errors.New("rollup: window length mismatch")
)

// errNonFinite rejects NaN and Inf samples; no statistics are defined for them.
var errNonFinite = errors.New("rollup: non-finite value")

// Window holds the aggregate of every sample filed into one bucket.
type Window struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Roller accumulates samples into epoch-aligned windows of a fixed length.
type Roller struct {
	window time.Duration
	bucket map[int64]*Window
	last   time.Time
	have   bool
}

// New builds a roller with the given window length. It panics when the window
// length is not positive.
func New(window time.Duration) *Roller {
	if window <= 0 {
		panic("rollup: bad window")
	}
	return &Roller{
		window: window,
		bucket: make(map[int64]*Window),
	}
}

// Add files a sample into the window containing at. Samples must not be older
// than the most recently accepted sample; equal timestamps are allowed and
// counted separately. Non-finite values are rejected without changing state.
func (r *Roller) Add(at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errNonFinite
	}
	if r.have && at.Before(r.last) {
		return ErrOutOfOrder
	}

	key := alignStart(at.UnixNano(), int64(r.window))
	b, ok := r.bucket[key]
	if !ok {
		b = &Window{
			Start: time.Unix(0, key).UTC(),
			Min:   value,
			Max:   value,
		}
		r.bucket[key] = b
	}
	if value < b.Min {
		b.Min = value
	}
	if value > b.Max {
		b.Max = value
	}
	b.Sum += value
	b.Count++

	r.last = at
	r.have = true
	return nil
}

// Window returns the bucket starting exactly at start. It does not snap start
// to a nearby window: an unknown start yields an empty result.
func (r *Roller) Window(start time.Time) (Window, bool) {
	b, ok := r.bucket[start.UnixNano()]
	if !ok {
		return Window{}, false
	}
	return *b, true
}

// Windows returns all populated windows ordered by start time.
func (r *Roller) Windows() []Window {
	out := make([]Window, 0, len(r.bucket))
	for _, b := range r.bucket {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Start.Before(out[j].Start)
	})
	return out
}

// Merge folds another roller's statistics into this one. Counts and sums add,
// minima and maxima combine. Windows present only on either side are kept, and
// merging the same statistics twice counts them twice. Merge does not affect
// out-of-order bookkeeping: samples older than the last locally accepted
// sample may still be added afterwards.
//
// It fails without changing the receiver when the window lengths differ and
// panics when other is nil.
func (r *Roller) Merge(other *Roller) error {
	if other == nil {
		panic("rollup: nil roller")
	}
	if r.window != other.window {
		return ErrWindowMismatch
	}
	for key, ob := range other.bucket {
		if rb, ok := r.bucket[key]; ok {
			rb.Count += ob.Count
			rb.Sum += ob.Sum
			if ob.Min < rb.Min {
				rb.Min = ob.Min
			}
			if ob.Max > rb.Max {
				rb.Max = ob.Max
			}
		} else {
			cp := *ob
			r.bucket[key] = &cp
		}
	}
	return nil
}

// alignStart floors the Unix nanosecond timestamp ns to a multiple of the
// positive window length w, so pre-epoch timestamps round down as well.
func alignStart(ns, w int64) int64 {
	q, rem := ns/w, ns%w
	if rem != 0 && ns < 0 {
		q--
	}
	return q * w
}
