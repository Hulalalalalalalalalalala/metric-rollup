package rollup

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// sinkView keeps BenchmarkRollup's results live.
var sinkView *View

// TestRollupBadFactor checks that a non-positive factor panics with the
// fixed message, before touching the roller.
func TestRollupBadFactor(t *testing.T) {
	r := New(time.Second)
	for _, factor := range []int{0, -1, -100} {
		func() {
			defer func() {
				if got := recover(); got != "rollup: bad factor" {
					t.Fatalf("factor %d: panic = %v", factor, got)
				}
			}()
			r.Rollup(factor)
		}()
	}
	// The panics changed nothing.
	if got := r.Windows(); len(got) != 0 {
		t.Fatalf("bad factor mutated the roller: %v", got)
	}
}

// TestRollupFactorOneEqualsRoller checks that factor 1 reproduces the
// roller's own window set item by item, including windows held in the
// backfill buffer.
func TestRollupFactorOneEqualsRoller(t *testing.T) {
	r := New(10 * time.Second)
	for i := int64(0); i < 50; i++ {
		if err := r.Add(time.Unix(i*7, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Pull in a window past the latest marker, then backfill into the gap
	// so both the main slice and the back buffer hold windows.
	ahead := New(10 * time.Second)
	if err := ahead.Add(time.Unix(1000, 0), -5); err != nil {
		t.Fatal(err)
	}
	if err := r.Merge(ahead); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(500, 0), 99); err != nil {
		t.Fatal(err)
	}

	want := r.Windows()
	v := r.Rollup(1)
	if got := v.Range(time.Unix(-1000, 0), time.Unix(2000, 0)); len(got) != len(want) {
		t.Fatalf("factor 1 range: got %d windows, want %d", len(got), len(want))
	}
	got := v.Range(time.Unix(math.MinInt64/4, 0), time.Unix(math.MaxInt64/4, 0))
	if len(got) != len(want) {
		t.Fatalf("factor 1 full range: got %d windows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("factor 1: window %d differs: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// TestRollupMergesCoveredWindows checks the core coarse-window statistics:
// they are merged from the covered fine windows, not recomputed from raw
// samples.
func TestRollupMergesCoveredWindows(t *testing.T) {
	r := New(10 * time.Second)
	batch := []Sample{
		{At: time.Unix(0, 0), Value: 1},
		{At: time.Unix(5, 0), Value: 2},
		{At: time.Unix(15, 0), Value: 3},
		{At: time.Unix(25, 0), Value: 4},
		{At: time.Unix(35, 0), Value: 10},
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(3) // 30s coarse windows

	w := v.Window(time.Unix(0, 0))
	if !w.Start.Equal(time.Unix(0, 0)) || w.Count != 4 || w.Sum != 10 || w.Min != 1 || w.Max != 4 {
		t.Fatalf("coarse window 0: %+v", w)
	}
	w = v.Window(time.Unix(30, 0))
	if !w.Start.Equal(time.Unix(30, 0)) || w.Count != 1 || w.Sum != 10 || w.Min != 10 || w.Max != 10 {
		t.Fatalf("coarse window 30: %+v", w)
	}
	// Missing coarse windows and non-aligned starts both read as zero
	// statistics with no error; existence is told from the count.
	for _, start := range []int64{60, 10, 5} {
		if w := v.Window(time.Unix(start, 0)); w != (Window{}) {
			t.Fatalf("lookup at %ds should miss with a zero window: %+v", start, w)
		}
	}
}

// TestRollupAlignmentPreEpoch checks floor alignment before the epoch:
// a -5s sample in a 10s fine window rolls up to the coarse window at -30s
// for factor 3, never to 0.
func TestRollupAlignmentPreEpoch(t *testing.T) {
	r := New(10 * time.Second)
	if err := r.Add(time.Unix(-5, 0), 7); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(3)
	w := v.Window(time.Unix(-30, 0))
	if w.Count != 1 || w.Sum != 7 || w.Min != 7 || w.Max != 7 {
		t.Fatalf("coarse window -30: %+v", w)
	}
	if w := v.Window(time.Unix(0, 0)); w.Count != 0 {
		t.Fatalf("sample must not roll up across the epoch: %+v", w)
	}
}

// TestRollupRange checks half-open range reads: empty, equal-ended, and
// inverted intervals all return an empty slice rather than erroring.
func TestRollupRange(t *testing.T) {
	r := New(time.Minute)
	for i := int64(0); i < 10; i++ {
		if err := r.Add(time.Unix(i*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(5) // two 5-minute coarse windows

	got := v.Range(time.Unix(61, 0), time.Unix(300, 0))
	if len(got) != 1 || !got[0].Start.Equal(time.Unix(0, 0)) || got[0].Count != 5 {
		t.Fatalf("range [61s,300s): %+v", got)
	}
	got = v.Range(time.Unix(0, 0), time.Unix(600, 0))
	if len(got) != 2 || got[1].Count != 5 || got[1].Sum != 35 {
		t.Fatalf("range [0,600s): %+v", got)
	}
	// A from inside a covered coarse window includes that window too:
	// [299s,301s) overlaps both halves around the boundary.
	got = v.Range(time.Unix(299, 0), time.Unix(301, 0))
	if len(got) != 2 {
		t.Fatalf("range [299s,301s): %+v", got)
	}
	// Starting exactly at the boundary excludes the earlier window.
	got = v.Range(time.Unix(300, 0), time.Unix(301, 0))
	if len(got) != 1 || !got[0].Start.Equal(time.Unix(300, 0)) {
		t.Fatalf("range [300s,301s): %+v", got)
	}
	for _, pair := range [][2]int64{{300, 300}, {600, 0}, {1200, 1800}} {
		if got := v.Range(time.Unix(pair[0], 0), time.Unix(pair[1], 0)); len(got) != 0 {
			t.Fatalf("range [%d,%d): want empty, got %+v", pair[0], pair[1], got)
		}
	}
}

// TestRollupCursor checks batched reads: consecutive non-overlapping
// batches cover the range exactly once, an exhausted cursor returns empty
// segments forever, and a non-positive batch size neither returns data nor
// advances the cursor.
func TestRollupCursor(t *testing.T) {
	r := New(time.Second)
	for i := int64(0); i < 12; i++ {
		if err := r.Add(time.Unix(i, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(2) // six 2s coarse windows
	c := v.Cursor(time.Unix(0, 0), time.Unix(12, 0))

	var starts []time.Time
	for _, n := range []int{2, 3, 10} {
		for _, w := range c.Next(n) {
			starts = append(starts, w.Start)
		}
	}
	if len(starts) != 6 {
		t.Fatalf("cursor delivered %d windows, want 6", len(starts))
	}
	for i, s := range starts {
		if !s.Equal(time.Unix(int64(i*2), 0)) {
			t.Fatalf("window %d starts at %v, want %ds", i, s, i*2)
		}
	}
	if got := c.Next(1); len(got) != 0 {
		t.Fatalf("exhausted cursor returned more: %+v", got)
	}

	c2 := v.Cursor(time.Unix(0, 0), time.Unix(12, 0))
	if got := c2.Next(0); len(got) != 0 {
		t.Fatalf("non-positive batch: %+v", got)
	}
	if got := c2.Next(6); len(got) != 6 {
		t.Fatalf("non-positive batch advanced the cursor: %d windows left", len(got))
	}
}

// TestRollupIsSnapshot checks that a view freezes the moment it was
// derived and never observes later writes, while a fresh view does.
func TestRollupIsSnapshot(t *testing.T) {
	r := New(time.Second)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(10)
	if err := r.Add(time.Unix(5, 0), 2); err != nil {
		t.Fatal(err)
	}
	if err := r.AddBatch([]Sample{{At: time.Unix(7, 0), Value: 4}}); err != nil {
		t.Fatal(err)
	}
	if w := v.Window(time.Unix(0, 0)); w.Count != 1 || w.Sum != 1 {
		t.Fatalf("snapshot changed after later writes: %+v", w)
	}
	if w := r.Rollup(10).Window(time.Unix(0, 0)); w.Count != 3 || w.Sum != 7 {
		t.Fatalf("fresh view: %+v", w)
	}
}

// TestRollupIsPureRead checks that deriving a view leaves the roller's
// statistics and out-of-order marker untouched, that a mismatched merge
// still fails exactly as before, and that out-of-order submission still
// returns the existing error value.
func TestRollupIsPureRead(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(60, 0), 1); err != nil {
		t.Fatal(err)
	}
	before := r.Windows()
	_ = r.Rollup(4)
	_ = r.Rollup(7)
	after := r.Windows()
	if len(after) != len(before) || after[0] != before[0] {
		t.Fatalf("rollup mutated roller stats: %v -> %v", before, after)
	}
	// The most-recent marker did not move: a stale sample is still rejected
	// and the boundary instant is still accepted.
	if err := r.Add(time.Unix(59, 0), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("want ErrOutOfOrder after rollup, got %v", err)
	}
	if err := r.Add(time.Unix(60, 0), 2); err != nil {
		t.Fatal(err)
	}
	// Merging a different window size keeps returning the existing error.
	if err := r.Merge(New(time.Hour)); !errors.Is(err, ErrWindowMismatch) {
		t.Fatalf("want ErrWindowMismatch, got %v", err)
	}
	// Merge does not enter ordering: after merging in later windows, an
	// older sample can still be backfilled.
	ahead := New(time.Minute)
	if err := ahead.Add(time.Unix(600, 0), 9); err != nil {
		t.Fatal(err)
	}
	if err := r.Merge(ahead); err != nil {
		t.Fatal(err)
	}
	_ = r.Rollup(3)
	if err := r.Add(time.Unix(120, 0), 5); err != nil {
		t.Fatalf("backfill after merge+rollup: %v", err)
	}
}

// TestRollupEmptyRoller checks every read entry on a view of an empty
// roller.
func TestRollupEmptyRoller(t *testing.T) {
	v := New(time.Second).Rollup(4)
	if w := v.Window(time.Unix(0, 0)); w != (Window{}) {
		t.Fatalf("empty view window: %+v", w)
	}
	if got := v.Range(time.Unix(0, 0), time.Unix(1000, 0)); len(got) != 0 {
		t.Fatalf("empty view range: %+v", got)
	}
	if got := v.Cursor(time.Unix(0, 0), time.Unix(1000, 0)).Next(5); len(got) != 0 {
		t.Fatalf("empty view cursor: %+v", got)
	}
}

// TestRollupCountSaturates checks that counts merged from covered fine
// windows saturate at the int64 ceiling, by the same rule as merging.
func TestRollupCountSaturates(t *testing.T) {
	r := New(time.Minute)
	r.buckets = append(r.buckets,
		Window{Start: time.Unix(0, 0).UTC(), Count: math.MaxInt64 - 2, Sum: 1, Min: -2, Max: 1},
		Window{Start: time.Unix(60, 0).UTC(), Count: 10, Sum: 4, Min: -7, Max: 6},
	)
	v := r.Rollup(2)
	w := v.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count overflowed: %d", w.Count)
	}
	if w.Sum != 5 || w.Min != -7 || w.Max != 6 {
		t.Fatalf("coarse stats: %+v", w)
	}
}

// TestRollupMinWindowSize checks derivation from the smallest allowed
// window: coarse starts stay epoch aligned at nanosecond granularity.
func TestRollupMinWindowSize(t *testing.T) {
	r := New(1) // 1-nanosecond windows
	for _, ns := range []int64{0, 1, 2, 5} {
		if err := r.Add(time.Unix(0, ns), float64(ns)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(3) // coarse windows [0,3) and [3,6) nanoseconds
	w := v.Window(time.Unix(0, 0))
	if w.Count != 3 || w.Sum != 3 || w.Min != 0 || w.Max != 2 {
		t.Fatalf("coarse [0,3) ns: %+v", w)
	}
	w = v.Window(time.Unix(0, 3))
	if w.Count != 1 || w.Sum != 5 {
		t.Fatalf("coarse [3,6) ns: %+v", w)
	}
}

// TestRollupExtremeTimestamps checks that windows far outside the
// int64-nanosecond range still align to coarse multiples of the epoch and
// do not spill into a neighboring coarse window through overflow.
func TestRollupExtremeTimestamps(t *testing.T) {
	const fine = 10 * time.Second
	for _, at := range []time.Time{
		time.Unix(1<<62, 123456789),
		time.Unix(-1<<62, 987654321),
		time.Unix(0, math.MaxInt64),
		time.Unix(0, math.MinInt64),
	} {
		r := New(fine)
		if err := r.Add(at, 3); err != nil {
			t.Fatal(err)
		}
		fineStart := wantStart(at, fine)
		v := r.Rollup(4) // 40s coarse windows
		want := wantStart(fineStart, 40*time.Second)
		w := v.Window(want)
		if w.Count != 1 || w.Sum != 3 || w.Min != 3 || w.Max != 3 {
			t.Fatalf("at %v: coarse window at %v: %+v", at, want, w)
		}
		if !w.Start.Equal(want) {
			t.Fatalf("at %v: coarse start %v, want %v", at, w.Start, want)
		}
		// The aligned fine start itself is never a coarse boundary here
		// unless it is a multiple of the coarse size.
		if !fineStart.Equal(want) {
			if w := v.Window(fineStart); w.Count != 0 {
				t.Fatalf("at %v: fine start %v is not a coarse start", at, fineStart)
			}
		}
	}
}

// TestRollupConcurrentWithWrites derives views while batches are filed
// continuously: every derived coarse window must be epoch aligned and
// internally consistent, and a batch must appear whole or not at all.
// Run with -race to catch data races.
func TestRollupConcurrentWithWrites(t *testing.T) {
	r := New(time.Millisecond)
	var writers sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		writers.Add(1)
		go func(g int) {
			defer writers.Done()
			at := time.Unix(0, 0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = r.AddBatch([]Sample{
					{At: at, Value: float64(g)},
					{At: at.Add(time.Millisecond), Value: float64(g)},
				})
				at = at.Add(2 * time.Millisecond)
			}
		}(g)
	}
	for i := 0; i < 200; i++ {
		v := r.Rollup(100) // 100ms coarse windows
		ws := v.Range(time.Unix(-1, 0), time.Unix(1<<30, 0))
		for j := 1; j < len(ws); j++ {
			if !ws[j-1].Start.Before(ws[j].Start) {
				t.Fatalf("coarse windows out of order: %v then %v", ws[j-1].Start, ws[j].Start)
			}
		}
		for _, w := range ws {
			if w.Start.UnixNano()%(100*time.Millisecond.Nanoseconds()) != 0 {
				t.Fatalf("misaligned coarse window: %+v", w)
			}
			if w.Count <= 0 || w.Min > w.Max {
				t.Fatalf("torn coarse window: %+v", w)
			}
		}
		// A cursor over the same frozen view must hand out each window once.
		c := v.Cursor(time.Unix(-1, 0), time.Unix(1<<30, 0))
		seen := 0
		for got := c.Next(37); len(got) != 0; got = c.Next(37) {
			seen += len(got)
		}
		if seen != len(ws) {
			t.Fatalf("cursor saw %d windows, view has %d", seen, len(ws))
		}
	}
	close(stop)
	writers.Wait()
}

// BenchmarkRollup measures deriving a coarse view; the cost should scale
// linearly with the number of fine windows and the temporary allocation
// stay bounded by that number.
func BenchmarkRollup(b *testing.B) {
	for _, n := range []int{1000, 100000} {
		r := benchRoller(time.Second, n)
		b.Run(fmt.Sprintf("n=%d/factor=10", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sinkView = r.Rollup(10)
			}
		})
		b.Run(fmt.Sprintf("n=%d/factor=1", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sinkView = r.Rollup(1)
			}
		})
	}
}
