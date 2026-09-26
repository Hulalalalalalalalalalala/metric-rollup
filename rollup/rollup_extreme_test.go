package rollup

import (
	"math"
	"math/big"
	"testing"
	"time"
)

// wantStart computes the epoch-aligned window start for at with 128-bit
// big-integer arithmetic, as an independent oracle for startOf.
func wantStart(at time.Time, w time.Duration) time.Time {
	ns := big.NewInt(at.Unix())
	ns.Mul(ns, big.NewInt(1e9))
	ns.Add(ns, big.NewInt(int64(at.Nanosecond())))
	wns := big.NewInt(w.Nanoseconds())
	q := new(big.Int).Quo(ns, wns)
	if new(big.Int).Rem(ns, wns).Sign() != 0 && ns.Sign() < 0 {
		q.Sub(q, big.NewInt(1))
	}
	start := q.Mul(q, wns)
	sec := new(big.Int).Quo(start, big.NewInt(1e9))
	rem := new(big.Int).Rem(start, big.NewInt(1e9))
	if rem.Sign() != 0 && start.Sign() < 0 {
		sec.Sub(sec, big.NewInt(1))
		rem.Add(rem, big.NewInt(1e9))
	}
	return time.Unix(sec.Int64(), rem.Int64()).UTC()
}

// TestExtremeTimestampsAlign checks that samples far outside the int64
// nanosecond range still land in epoch-aligned windows.
func TestExtremeTimestampsAlign(t *testing.T) {
	const window = 10 * time.Second
	for _, at := range []time.Time{
		time.Unix(1<<62, 123456789),       // far future, UnixNano overflows
		time.Unix(-1<<62, 987654321),      // far past, UnixNano overflows
		time.Unix(0, math.MaxInt64),       // largest representable nanos
		time.Unix(0, math.MinInt64),       // smallest representable nanos
		time.Unix(-9223372036, 999999999), // fast-path boundary
		time.Unix(9223372036, 854775807),  // slow-path boundary
	} {
		r := New(window)
		if err := r.Add(at, 5); err != nil {
			t.Fatalf("Add(%v): %v", at, err)
		}
		want := wantStart(at, window)
		w, ok := r.Window(want)
		if !ok {
			t.Fatalf("Add(%v): no window at aligned start %v", at, want)
		}
		if w.Count != 1 || w.Sum != 5 || w.Min != 5 || w.Max != 5 {
			t.Fatalf("Add(%v): window %+v", at, w)
		}
		if got := r.Windows(); len(got) != 1 || !got[0].Start.Equal(want) {
			t.Fatalf("Add(%v): Windows() = %v, want start %v", at, got, want)
		}
	}
}

// TestMinWindowSize checks that a 1-nanosecond window behaves like any
// other size: boundary ownership, alignment, and lookup by start.
func TestMinWindowSize(t *testing.T) {
	r := New(1)
	for _, ns := range []int64{0, 0, 1, 2, 5} {
		if err := r.Add(time.Unix(0, ns), float64(ns)); err != nil {
			t.Fatal(err)
		}
	}
	got := r.Windows()
	if len(got) != 4 {
		t.Fatalf("want 4 windows, got %d", len(got))
	}
	for i, ns := range []int64{0, 1, 2, 5} {
		if !got[i].Start.Equal(time.Unix(0, ns)) {
			t.Fatalf("window %d starts at %v, want %v", i, got[i].Start, time.Unix(0, ns))
		}
	}
	w, ok := r.Window(time.Unix(0, 0))
	if !ok || w.Count != 2 || w.Sum != 0 {
		t.Fatalf("window 0: %+v ok=%v", w, ok)
	}
}

// TestCountSaturates checks that a window count at the int64 ceiling stops
// accumulating while sum, min, and max keep updating.
func TestCountSaturates(t *testing.T) {
	r := New(time.Minute)
	r.buckets = append(r.buckets, Window{
		Start: time.Unix(0, 0).UTC(),
		Count: math.MaxInt64,
		Sum:   10,
		Min:   1,
		Max:   9,
	})
	r.latest = time.Unix(0, 0)
	r.hasAny = true
	if err := r.Add(time.Unix(0, 0), 20); err != nil {
		t.Fatal(err)
	}
	w, _ := r.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count overflowed: %d", w.Count)
	}
	if w.Sum != 30 || w.Min != 1 || w.Max != 20 {
		t.Fatalf("stats did not keep updating: %+v", w)
	}
}

// TestMergeCountSaturates checks that merging counts past the int64
// ceiling clamps at the ceiling instead of wrapping.
func TestMergeCountSaturates(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	start := time.Unix(0, 0).UTC()
	a.buckets = append(a.buckets, Window{Start: start, Count: math.MaxInt64 - 1, Sum: 1, Min: 1, Max: 1})
	b.buckets = append(b.buckets, Window{Start: start, Count: 10, Sum: 2, Min: 0, Max: 2})
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	w, _ := a.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count overflowed: %d", w.Count)
	}
	if w.Sum != 3 || w.Min != 0 || w.Max != 2 {
		t.Fatalf("merged stats: %+v", w)
	}
}

// TestMergeSortedUnion cross-checks a large merge against a map-based
// oracle, including windows only one side has.
func TestMergeSortedUnion(t *testing.T) {
	a := New(time.Second)
	b := New(time.Second)
	base := time.Unix(0, 0)
	want := map[int64]Window{}
	add := func(r *Roller, i int64, v float64) {
		at := base.Add(time.Duration(i) * time.Second)
		if err := r.Add(at, v); err != nil {
			t.Fatal(err)
		}
		key := at.Unix()
		w := want[key]
		if w.Count == 0 {
			w = Window{Start: at.UTC()}
		}
		w.Count++
		w.Sum += v
		if v < w.Min || w.Count == 1 {
			w.Min = v
		}
		if v > w.Max || w.Count == 1 {
			w.Max = v
		}
		want[key] = w
	}
	for i := int64(0); i < 5000; i++ {
		if i%2 == 0 {
			add(a, i, float64(i))
		}
		if i%2 == 1 || i%3 == 0 {
			add(b, i, float64(-i))
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	got := a.Windows()
	if len(got) != len(want) {
		t.Fatalf("got %d windows, want %d", len(got), len(want))
	}
	for i := 1; i < len(got); i++ {
		if !got[i-1].Start.Before(got[i].Start) {
			t.Fatalf("windows out of order at %d: %v then %v", i, got[i-1].Start, got[i].Start)
		}
	}
	for _, w := range got {
		if w != want[w.Start.Unix()] {
			t.Fatalf("window %v: got %+v, want %+v", w.Start, w, want[w.Start.Unix()])
		}
	}
}

// TestBackfillAfterMerge checks that samples earlier than merged-in windows
// can still be filed and land in the right place.
func TestBackfillAfterMerge(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	if err := a.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	for i := 100; i < 110; i++ {
		if err := b.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	// Backfill between the pre-merge sample and the merged windows.
	for i := 1; i < 100; i++ {
		if err := a.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatalf("backfill %d: %v", i, err)
		}
	}
	got := a.Windows()
	if len(got) != 110 {
		t.Fatalf("got %d windows, want 110", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i)*60, 0)) {
			t.Fatalf("window %d starts at %v", i, w.Start)
		}
	}
}
