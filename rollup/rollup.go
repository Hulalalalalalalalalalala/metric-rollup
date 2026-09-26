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

// errNonFinite is returned by Add for NaN/Inf values. Such samples are
// refused and define no statistics; the sentinel stays unexported because
// the public error surface is limited to ErrOutOfOrder and
// ErrWindowMismatch.
var errNonFinite = errors.New("rollup: non-finite value")

// Window holds the aggregate statistics of the samples filed into one
// window. Start is the window's left-closed, right-open lower bound.
type Window struct {
	Start time.Time
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// stats is the per-window aggregate. The window start is deliberately not
// stored: it is derived from the slot key and the window length on read,
// keeping each in-memory window to 40 bytes instead of duplicating the
// 24-byte time stamp.
type stats struct {
	count    int64
	sum      float64
	min, max float64
}

// slot is one window kept inline in the roller's sorted backing arrays.
// Keying by window index (floor division of the epoch-nanosecond timestamp
// by the window length), rather than by the start instant, keeps the key
// valid even when the start instant itself would overflow int64
// nanoseconds.
type slot struct {
	key int64
	s   stats
}

// chunkCap bounds the size of one sorted chunk. Inserts never move more
// than chunkCap slots, so filing a window is constant work regardless of
// how many windows the roller holds.
const chunkCap = 128

// Roller aggregates samples into fixed windows of a single size.
//
// Windows live in a list of sorted chunks: every chunk is non-empty and
// sorted by key, and key ranges across chunks are disjoint and ordered.
// The structure stays dense for gapless high-throughput streams and does
// not grow with the time span between windows, so sparse streams and the
// minimum allowed window length cost the same as dense ones.
//
// The zero value is not usable; build one with New. All methods are safe
// to call concurrently from multiple goroutines.
type Roller struct {
	// mu guards chunks, n, latest, and hasAny. id, window, and winNS are
	// fixed at construction and read without the lock.
	mu     sync.RWMutex
	id     uint64
	window time.Duration
	winNS  int64
	chunks [][]slot
	n      int
	latest time.Time
	hasAny bool
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
		winNS:  window.Nanoseconds(),
	}
}

// windowKey returns the epoch-aligned index of the window containing ns:
// floor(ns / w), using flooring (not truncation) for pre-epoch instants.
func windowKey(ns, w int64) int64 {
	q := ns / w
	if ns%w != 0 && ns < 0 {
		q--
	}
	return q
}

// windowStart is the UTC time of epoch + key*w nanoseconds. The product is
// evaluated in 128 bits so a start just outside the int64-nanosecond range
// (possible for negative extreme timestamps) is placed correctly instead
// of overflowing into a positive window.
func windowStart(key, w int64) time.Time {
	abs := uint64(key)
	if key < 0 {
		// uint64(-key) is the correct magnitude even for MinInt64.
		abs = uint64(-key)
	}
	// |key*w| < 2^64 for every key derived from an int64 timestamp, so the
	// 128-bit product holds and the seconds quotient fits int64.
	hi, lo := bits.Mul64(abs, uint64(w))
	sec, nsec := bits.Div64(hi, lo, 1e9)
	if key < 0 {
		return time.Unix(-int64(sec), -int64(nsec)).UTC()
	}
	return time.Unix(int64(sec), int64(nsec)).UTC()
}

// asWindow materializes the public view of a slot.
func (r *Roller) asWindow(sl slot) Window {
	return Window{
		Start: windowStart(sl.key, r.winNS),
		Count: sl.s.count,
		Sum:   sl.s.sum,
		Min:   sl.s.min,
		Max:   sl.s.max,
	}
}

// addCount sums two non-negative window counts with saturation at
// math.MaxInt64: a window that has reached the cap keeps the cap instead
// of wrapping negative.
func addCount(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

// chunkFor returns the index of the chunk in which key belongs or would be
// inserted: the last chunk whose first key is <= key, or -1 when key
// precedes every chunk.
func chunkFor(chunks [][]slot, key int64) int {
	i := sort.Search(len(chunks), func(c int) bool { return chunks[c][0].key > key })
	return i - 1
}

// insertSlot inserts ns into chunk ci, which is known not to contain its
// key. A full chunk is grown and split into two chunks.
func (r *Roller) insertSlot(ci int, ns slot) {
	ch := r.chunks[ci]
	if len(ch) < cap(ch) {
		p := sort.Search(len(ch), func(k int) bool { return ch[k].key > ns.key })
		ch = ch[:len(ch)+1]
		copy(ch[p+1:], ch[p:])
		ch[p] = ns
		r.chunks[ci] = ch
		return
	}
	p := sort.Search(len(ch), func(k int) bool { return ch[k].key > ns.key })
	if p == len(ch) && ci == len(r.chunks)-1 {
		// Append past the end of the last chunk: open a fresh one without
		// copying.
		next := make([]slot, 1, chunkCap)
		next[0] = ns
		r.chunks = append(r.chunks, next)
		return
	}
	// Interior insertion into a full chunk: grow to a temporary
	// 2*chunkCap backing, insert, then halve into two chunkCap-backed
	// chunks; each window is copied a constant small number of times over
	// its lifetime.
	big := make([]slot, chunkCap*2)
	copy(big[:p], ch[:p])
	big[p] = ns
	copy(big[p+1:], ch[p:])
	total := len(ch) + 1
	half := total / 2
	left := make([]slot, half, chunkCap)
	copy(left, big[:half])
	right := make([]slot, total-half, chunkCap)
	copy(right, big[half:total])
	r.chunks[ci] = left
	r.chunks = append(r.chunks, nil)
	copy(r.chunks[ci+2:], r.chunks[ci+1:])
	r.chunks[ci+1] = right
}

// Add files a sample into its window. A sample earlier than the most recent
// accepted sample is rejected with ErrOutOfOrder and leaves the roller
// unchanged; a sample at the same instant is accepted and counted again.
// NaN and infinite values are refused and likewise leave it unchanged.
//
// Concurrent Adds are serialized: each takes effect in the order it
// acquires the roller, and the out-of-order check is measured against the
// samples accepted before it in that order.
func (r *Roller) Add(at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errNonFinite
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasAny && at.Before(r.latest) {
		return ErrOutOfOrder
	}
	key := windowKey(at.UnixNano(), r.winNS)
	if len(r.chunks) == 0 {
		first := make([]slot, 1, chunkCap)
		first[0] = slot{key: key, s: stats{count: 1, sum: value, min: value, max: value}}
		r.chunks = [][]slot{first}
		r.n = 1
		r.latest = at
		r.hasAny = true
		return nil
	}
	ci := chunkFor(r.chunks, key)
	if ci < 0 {
		ci = 0
	}
	ch := r.chunks[ci]
	p := sort.Search(len(ch), func(k int) bool { return ch[k].key >= key })
	if p < len(ch) && ch[p].key == key {
		s := &ch[p].s
		if s.count < math.MaxInt64 {
			s.count++
		}
		s.sum += value
		if value < s.min {
			s.min = value
		}
		if value > s.max {
			s.max = value
		}
	} else {
		r.insertSlot(ci, slot{
			key: key,
			s:   stats{count: 1, sum: value, min: value, max: value},
		})
		r.n++
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
	ns := start.UnixNano()
	if ns%r.winNS != 0 {
		// Not a window boundary; only exact starts match.
		return Window{}, false
	}
	key := ns / r.winNS // exact division, so truncation is flooring here
	r.mu.RLock()
	defer r.mu.RUnlock()
	ci := chunkFor(r.chunks, key)
	if ci < 0 {
		return Window{}, false
	}
	ch := r.chunks[ci]
	p := sort.Search(len(ch), func(k int) bool { return ch[k].key >= key })
	if p >= len(ch) || ch[p].key != key {
		return Window{}, false
	}
	return r.asWindow(ch[p]), true
}

// Windows returns all buckets in time order. An empty roller yields an
// empty slice. The result is a snapshot of one moment: samples filed after
// the call do not alter it.
func (r *Roller) Windows() []Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Window, r.n)
	o := 0
	for _, ch := range r.chunks {
		for i := range ch {
			out[o] = r.asWindow(ch[i])
			o++
		}
	}
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
		// Merging a roller with itself doubles every statistic.
		for _, ch := range r.chunks {
			for i := range ch {
				s := &ch[i].s
				s.count = addCount(s.count, s.count)
				s.sum *= 2
				// Min and max of a window with itself are unchanged.
			}
		}
		return nil
	}
	// Lock both rollers in ID order so simultaneous merges in opposite
	// directions cannot deadlock.
	first, second := r, other
	if other.id < r.id {
		first, second = other, r
	}
	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	// Linear two-pointer fold over both rollers' chunks into fresh
	// chunkCap-sized chunks: O(n+m) time with one allocation per chunkCap
	// windows, regardless of key overlap.
	out := make([][]slot, 0, (r.n+other.n)/chunkCap+1)
	cur := make([]slot, 0, chunkCap)
	total := 0
	flush := func() {
		if len(cur) > 0 {
			out = append(out, cur)
			cur = make([]slot, 0, chunkCap)
		}
	}
	emit := func(s slot) {
		cur = append(cur, s)
		total++
		if len(cur) == chunkCap {
			flush()
		}
	}
	ci, cj := 0, 0
	pi, pj := 0, 0
	a, b := r.chunks, other.chunks
	for ci < len(a) || cj < len(b) {
		// The next slot of a side is tracked by (chunk index, position).
		switch {
		case cj == len(b) || (ci < len(a) && a[ci][pi].key < b[cj][pj].key):
			emit(a[ci][pi])
			pi++
			if pi == len(a[ci]) {
				ci++
				pi = 0
			}
		case ci == len(a) || b[cj][pj].key < a[ci][pi].key:
			emit(b[cj][pj])
			pj++
			if pj == len(b[cj]) {
				cj++
				pj = 0
			}
		default:
			x, y := a[ci][pi].s, b[cj][pj].s
			if y.min < x.min {
				x.min = y.min
			}
			if y.max > x.max {
				x.max = y.max
			}
			x.sum += y.sum
			x.count = addCount(x.count, y.count)
			emit(slot{key: a[ci][pi].key, s: x})
			pi++
			if pi == len(a[ci]) {
				ci++
				pi = 0
			}
			pj++
			if pj == len(b[cj]) {
				cj++
				pj = 0
			}
		}
	}
	flush()
	r.chunks = out
	r.n = total
	return nil
}
