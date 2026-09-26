package rollup

import "time"

// View is a read-only derived view of a Roller at a coarser window size.
// Each view window covers a fixed number of the roller's own windows and
// carries statistics merged from them; no raw samples are re-read. A View
// is a snapshot of the moment it was derived: samples filed, backfilled,
// or merged into the roller afterwards do not alter it.
//
// All methods are safe to call concurrently from multiple goroutines.
type View struct {
	// window is the coarse window size: the roller's window size times
	// the factor the view was derived with. windows is fixed at
	// derivation and read without a lock.
	window  time.Duration
	windows []Window
}

// Rollup derives a read-only view of the roller whose windows are factor
// times wider than the roller's own, aligned to integer multiples of the
// coarser size since the Unix epoch, left-closed and right-open exactly
// like the roller's windows. Each coarse window merges the statistics of
// the roller windows it covers: counts and sums add, minimums take the
// smaller, maximums take the larger. A factor of 1 yields a view
// window-for-window identical to the roller's own.
//
// Rollup panics with "rollup: bad factor" if factor is not positive. It
// is a pure read: the roller's statistics and its most-recent marker are
// untouched, and concurrent adds, batches, backfills, and merges proceed
// as usual — the view observes one consistent snapshot of the roller.
func (r *Roller) Rollup(factor int) *View {
	if factor <= 0 {
		panic("rollup: bad factor")
	}
	coarse := time.Duration(factor) * r.window
	w := coarse.Nanoseconds()

	r.mu.RLock()
	defer r.mu.RUnlock()
	fine := r.buckets
	if len(r.back) != 0 {
		fine = mergeWindows(r.buckets, r.back)
	}
	// The coarse start is monotone in the fine start, so covered windows
	// are consecutive and the whole derivation is one linear pass with a
	// single output allocation bounded by the number of fine windows.
	windows := make([]Window, 0, len(fine))
	for _, f := range fine {
		start := alignStart(f.Start, w)
		if n := len(windows); n != 0 && windows[n-1].Start.Equal(start) {
			cur := &windows[n-1]
			cur.Count = satAdd(cur.Count, f.Count)
			cur.Sum += f.Sum
			if f.Min < cur.Min {
				cur.Min = f.Min
			}
			if f.Max > cur.Max {
				cur.Max = f.Max
			}
		} else {
			windows = append(windows, Window{
				Start: start,
				Count: f.Count,
				Sum:   f.Sum,
				Min:   f.Min,
				Max:   f.Max,
			})
		}
	}
	return &View{window: coarse, windows: windows}
}

// Window returns the coarse window whose start matches start exactly, by
// the same exact-match rule as the roller's Window. A start no coarse
// window begins at yields the zero Window and no error; whether the window
// exists is told from its sample count. The returned Window is a copy and
// stays valid no matter what is filed into the roller afterwards.
func (v *View) Window(start time.Time) Window {
	if i, ok := windowIndex(v.windows, start); ok {
		return v.windows[i]
	}
	return Window{}
}

// Range returns the coarse windows overlapping the half-open interval
// [from, to), in time order, selected by the same rule as the roller's
// Range: the first window returned is the one containing from, and a
// window starting exactly at to is excluded. An interval whose end does
// not come after its start yields an empty slice, as does an interval no
// coarse window overlaps.
func (v *View) Range(from, to time.Time) []Window {
	out := []Window{}
	if !from.Before(to) {
		return out
	}
	i, _ := windowIndex(v.windows, alignStart(from, v.window.Nanoseconds()))
	for ; i < len(v.windows); i++ {
		w := v.windows[i]
		if !w.Start.Before(to) {
			break
		}
		out = append(out, w)
	}
	return out
}

// Cursor returns a cursor over the coarse windows overlapping the
// half-open interval [from, to), selected by the same rule as Range. The
// view is already a snapshot, so the cursor's batches never tear, skip,
// or repeat a window no matter what is filed into the roller while the
// cursor advances.
func (v *View) Cursor(from, to time.Time) *Cursor {
	return &Cursor{windows: v.Range(from, to)}
}
