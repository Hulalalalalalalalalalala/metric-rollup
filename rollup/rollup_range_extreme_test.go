package rollup

import (
	"math"
	"testing"
	"time"
)

// TestRangeExtremeEndpoints files samples near and past the int64
// nanosecond range and queries them with endpoints built from the same
// extreme instants. Alignment is epoch based at every step, so no window
// is dropped into a neighboring bucket and the result stays a complete,
// time-ordered set.
func TestRangeExtremeEndpoints(t *testing.T) {
	const window = 10 * time.Second
	ats := []time.Time{
		time.Unix(0, math.MinInt64),
		time.Unix(0, math.MaxInt64),
		time.Unix(-1<<62, 0),
		time.Unix(1<<62, 0),
		time.Unix(-9223372037, 900000000),
		time.Unix(9223372036, 900000000),
	}
	r := New(window)
	// Submit in time order so every sample is accepted.
	order := make([]int, len(ats))
	for i := range order {
		order[i] = i
	}
	// Insertion sort of the sample indices by time.
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && ats[order[j]].Before(ats[order[j-1]]); j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	for _, idx := range order {
		if err := r.Add(ats[idx], 1); err != nil {
			t.Fatalf("Add(%v): %v", ats[idx], err)
		}
	}
	// Deduplicate window starts (two extreme samples can share a bucket).
	sorted := r.Windows()
	if len(sorted) == 0 {
		t.Fatal("no windows")
	}
	for i := 1; i < len(sorted); i++ {
		if !sorted[i-1].Start.Before(sorted[i].Start) {
			t.Fatalf("windows not ordered: %v then %v", sorted[i-1].Start, sorted[i].Start)
		}
	}
	first := sorted[0].Start
	last := sorted[len(sorted)-1].Start

	// A range from the first window start to five seconds past the last
	// covers every window, in order, even though those instants are far
	// outside the int64-nanosecond range.
	from := first
	to := time.Unix(last.Unix()+5, int64(last.Nanosecond()))
	got := r.Range(from, to)
	if len(got) != len(sorted) {
		t.Fatalf("extreme range returned %d windows, want %d", len(got), len(sorted))
	}
	for i := range got {
		if !got[i].Start.Equal(sorted[i].Start) {
			t.Fatalf("window %d: got %v want %v", i, got[i].Start, sorted[i].Start)
		}
	}

	// Ending exactly at the last window's start excludes that window.
	upTo := r.Range(from, last)
	if len(upTo) != len(sorted)-1 {
		t.Fatalf("end-at-start returned %d windows, want %d", len(upTo), len(sorted)-1)
	}
	// An empty extreme interval returns an empty slice.
	if e := r.Range(last, last); len(e) != 0 {
		t.Fatalf("empty extreme interval: %v", e)
	}
	// A point inside the first window still selects it.
	inside := time.Unix(first.Unix(), int64(first.Nanosecond())+1)
	if g := r.Range(inside, to); len(g) == 0 || !g[0].Start.Equal(first) {
		t.Fatalf("inside-window selection at the extreme: %v", g)
	}

	// The same holds for a derived coarse view and its cursor.
	v := r.Rollup(1)
	vg := v.Range(from, to)
	if len(vg) != len(sorted) {
		t.Fatalf("view range returned %d windows, want %d", len(vg), len(sorted))
	}
	c := r.Cursor(from, to)
	if b := c.Next(len(sorted) + 10); len(b) != len(sorted) {
		t.Fatalf("cursor returned %d windows, want %d", len(b), len(sorted))
	}
	vc := v.Cursor(from, to)
	if b := vc.Next(len(sorted) + 10); len(b) != len(sorted) {
		t.Fatalf("view cursor returned %d windows, want %d", len(b), len(sorted))
	}
}
