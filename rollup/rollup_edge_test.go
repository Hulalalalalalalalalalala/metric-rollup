package rollup

import (
	"math"
	"testing"
	"time"
)

// TestNonFiniteRejected checks NaN and Inf are refused and leave state
// untouched; no statistic is defined for them.
func TestNonFiniteRejected(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		r := New(time.Second)
		if err := r.Add(time.Unix(0, 0), 1); err != nil {
			t.Fatal(err)
		}
		if err := r.Add(time.Unix(1, 0), v); err == nil {
			t.Fatalf("value %v accepted", v)
		}
		got := r.Windows()
		if len(got) != 1 || got[0].Count != 1 {
			t.Fatalf("non-finite sample mutated state: %+v", got)
		}
		// A later finite sample is still accepted: refusal does not move
		// the out-of-order watermark.
		if err := r.Add(time.Unix(2, 0), 2); err != nil {
			t.Fatalf("add after refused non-finite: %v", err)
		}
	}
}

// TestExtremeTimestampWindows checks epoch-aligned windowing at the int64
// nanosecond boundaries: flooring near the negative extreme must not
// overflow into a positive window, and start reconstruction must match.
func TestExtremeTimestampWindows(t *testing.T) {
	// Minimum allowed window length: every nanosecond is its own window,
	// so each extreme timestamp lands exactly at its own start.
	r := New(time.Nanosecond)
	for _, ns := range []int64{math.MinInt64, math.MinInt64 + 1, math.MaxInt64 - 1, math.MaxInt64} {
		if err := r.Add(time.Unix(0, ns), 1); err != nil {
			t.Fatalf("Add(%d): %v", ns, err)
		}
	}
	w, ok := r.Window(time.Unix(0, math.MinInt64))
	if !ok {
		t.Fatal("window at MinInt64 missing")
	}
	if w.Count != 1 || !w.Start.Equal(time.Unix(0, math.MinInt64)) {
		t.Fatalf("min window: %+v", w)
	}
	w, ok = r.Window(time.Unix(0, math.MaxInt64))
	if !ok {
		t.Fatal("window at MaxInt64 missing")
	}
	if w.Count != 1 || !w.Start.Equal(time.Unix(0, math.MaxInt64)) {
		t.Fatalf("max window: %+v", w)
	}
	if got := len(r.Windows()); got != 4 {
		t.Fatalf("want 4 windows at extremes, got %d", got)
	}

	// Negative flooring with a coarser window: the two samples after the
	// boundary share the window starting exactly at MinInt64 (which is a
	// multiple of 2), instead of one being truncated toward zero.
	r2 := New(2 * time.Nanosecond)
	if err := r2.Add(time.Unix(0, math.MinInt64), 1); err != nil {
		t.Fatal(err)
	}
	if err := r2.Add(time.Unix(0, math.MinInt64+1), 1); err != nil {
		t.Fatal(err)
	}
	w, _ = r2.Window(time.Unix(0, math.MinInt64))
	if w.Count != 2 {
		t.Fatalf("floored negative window: %+v", w)
	}

	// Key/start round-trips at both extremes.
	for _, key := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		st := windowStart(key, 1)
		if got := windowKey(st.UnixNano(), 1); got != key {
			t.Fatalf("key %d round-trip got %d", key, got)
		}
	}
}

// TestCountSaturation verifies counts stop at MaxInt64 while sum/min/max
// keep following the ordinary rules.
func TestCountSaturation(t *testing.T) {
	r := New(time.Second)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	r.chunks[0][0].s.count = math.MaxInt64 - 1
	if err := r.Add(time.Unix(0, 0), 5); err != nil {
		t.Fatal(err)
	}
	w, _ := r.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count = %d, want MaxInt64", w.Count)
	}
	if err := r.Add(time.Unix(0, 0), 7); err != nil {
		t.Fatal(err)
	}
	w, _ = r.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count wrapped past cap: %d", w.Count)
	}
	if w.Sum != 1+5+7 || w.Min != 1 || w.Max != 7 {
		t.Fatalf("stats after cap: %+v", w)
	}

	// Merge of two saturated counts saturates.
	a := New(time.Second)
	b := New(time.Second)
	a.Add(time.Unix(10, 0), 1)
	b.Add(time.Unix(10, 0), 1)
	a.chunks[0][0].s.count = math.MaxInt64
	b.chunks[0][0].s.count = math.MaxInt64
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	wa, _ := a.Window(time.Unix(10, 0))
	if wa.Count != math.MaxInt64 {
		t.Fatalf("merged count = %d", wa.Count)
	}
}

// TestMergeThenBackfill verifies earlier samples can still be filed after
// a merge brought in later windows, and that backfilled windows sort into
// the middle without corrupting ordering.
func TestMergeThenBackfill(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	a.Add(time.Unix(0, 0), 1)
	for i := 10; i < 20; i++ {
		b.Add(time.Unix(int64(i)*60, 0), float64(i))
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	// Backfill windows 1..9 out of order relative to a's watermark would
	// fail; a's latest is still 0, so an increasing backfill works.
	for i := int64(1); i < 10; i++ {
		if err := a.Add(time.Unix(i*60, 0), float64(i)); err != nil {
			t.Fatalf("backfill %d: %v", i, err)
		}
	}
	got := a.Windows()
	if len(got) != 20 {
		t.Fatalf("want 20 windows, got %d", len(got))
	}
	for i := range got {
		if !got[i].Start.Equal(time.Unix(int64(i)*60, 0)) {
			t.Fatalf("window %d out of order: %v", i, got[i].Start)
		}
	}
}

// TestMinuteWindowAlignmentAcrossRollers checks the minimum window length
// aligns across independently built rollers and reads by exact start.
func TestMinWindowAlignmentAcrossRollers(t *testing.T) {
	a := New(time.Nanosecond)
	b := New(time.Nanosecond)
	a.Add(time.Unix(0, 5), 1)
	b.Add(time.Unix(0, 5), 9)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	w, ok := a.Window(time.Unix(0, 5))
	if !ok || w.Count != 2 || w.Sum != 10 {
		t.Fatalf("1ns merged window: ok=%v %+v", ok, w)
	}
	// Boundary ownership: 4 belongs elsewhere.
	if _, ok := a.Window(time.Unix(0, 4)); ok {
		t.Fatal("window at 4ns should not exist")
	}
	// Unaligned lookup still misses.
	if _, ok := a.Window(time.Unix(0, 3)); ok {
		t.Fatal("unaligned lookup should miss")
	}
}

// TestSparseWindows is a structural check that windows with huge gaps
// between them cost memory proportional to window count, not span: 1000
// windows spread over nearly the full int64 range must stay fast and
// correctly ordered.
func TestSparseWindows(t *testing.T) {
	r := New(time.Nanosecond)
	const nw = 1000
	start := -int64(1) << 62
	stride := (int64(1) << 62) / nw // ~4.6 quadrillion nanoseconds per window
	for i := 0; i < nw; i++ {
		at := start + int64(i)*stride
		if err := r.Add(time.Unix(0, at), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	got := r.Windows()
	if len(got) != nw {
		t.Fatalf("want %d sparse windows, got %d", nw, len(got))
	}
	for i := 1; i < len(got); i++ {
		if !got[i-1].Start.Before(got[i].Start) {
			t.Fatalf("sparse windows not ordered at %d", i)
		}
	}
}
