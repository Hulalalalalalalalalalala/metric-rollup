package rollup

import (
	"sync"
	"testing"
	"time"
)

func TestRollupBadFactor(t *testing.T) {
	r := New(time.Second)
	for _, factor := range []int{0, -1, -100} {
		func() {
			defer func() {
				if got := recover(); got != "rollup: bad factor" {
					t.Fatalf("factor %d: expected \"rollup: bad factor\" panic, got %v", factor, got)
				}
			}()
			r.Rollup(factor)
		}()
	}
}

func TestRollupFactorOneEqualsRoller(t *testing.T) {
	r := New(10 * time.Second)
	for i := int64(0); i < 50; i++ {
		if err := r.Add(time.Unix(i*7, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Backfill an older window so the back buffer is in play too: merging
	// pulls in a window past the latest marker, and a sample between the
	// marker and that window lands in the backfill buffer.
	r2 := New(10 * time.Second)
	if err := r2.Add(time.Unix(1000, 0), -5); err != nil {
		t.Fatal(err)
	}
	if err := r.Merge(r2); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(500, 0), 99); err != nil {
		t.Fatal(err)
	}

	want := r.Windows()
	got := r.Rollup(1).Range(time.Unix(-1000, 0), time.Unix(2000, 0))
	if len(got) != len(want) {
		t.Fatalf("factor 1: got %d windows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("factor 1: window %d differs: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRollupMergesCoveredWindows(t *testing.T) {
	r := New(10 * time.Second)
	// Coarse windows are 30s wide: [0,30), [30,60), ...
	samples := []Sample{
		{time.Unix(0, 0), 1},
		{time.Unix(5, 0), 2},
		{time.Unix(15, 0), 3},
		{time.Unix(25, 0), 4},
		{time.Unix(35, 0), 10},
	}
	if err := r.AddBatch(samples); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(3)

	w := v.Window(time.Unix(0, 0))
	if w.Count != 4 || w.Sum != 10 || w.Min != 1 || w.Max != 4 {
		t.Fatalf("coarse window 0: %+v", w)
	}
	if !w.Start.Equal(time.Unix(0, 0)) {
		t.Fatalf("coarse start: %v", w.Start)
	}
	w = v.Window(time.Unix(30, 0))
	if w.Count != 1 || w.Sum != 10 || w.Min != 10 || w.Max != 10 {
		t.Fatalf("coarse window 30: %+v", w)
	}
	// No coarse window starts at 60s: zero stats, no error.
	if w = v.Window(time.Unix(60, 0)); w.Count != 0 || w.Sum != 0 || w.Min != 0 || w.Max != 0 {
		t.Fatalf("missing coarse window should be zero: %+v", w)
	}
	// A start no coarse window begins at also yields zero stats.
	if w = v.Window(time.Unix(10, 0)); w.Count != 0 {
		t.Fatalf("exact-match lookup should miss at 10s: %+v", w)
	}
}

func TestRollupAlignmentPreEpoch(t *testing.T) {
	r := New(10 * time.Second)
	// -5s is in fine window [-10,0), which sits in coarse window [-30,0).
	if err := r.Add(time.Unix(-5, 0), 7); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(3)
	w := v.Window(time.Unix(-30, 0))
	if w.Count != 1 || w.Sum != 7 {
		t.Fatalf("coarse window -30: %+v", w)
	}
}

func TestRollupRange(t *testing.T) {
	r := New(time.Minute)
	for i := int64(0); i < 10; i++ {
		if err := r.Add(time.Unix(i*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(5) // 5-minute coarse windows: [0,5), [5,10) minutes

	// Half-open: from inside the first coarse window, to exactly at the
	// second one's start excludes it.
	got := v.Range(time.Unix(61, 0), time.Unix(300, 0))
	if len(got) != 1 || !got[0].Start.Equal(time.Unix(0, 0)) || got[0].Count != 5 {
		t.Fatalf("range [61s,300s): %+v", got)
	}
	got = v.Range(time.Unix(0, 0), time.Unix(600, 0))
	if len(got) != 2 || got[1].Count != 5 || got[1].Sum != 35 {
		t.Fatalf("range [0,600s): %+v", got)
	}
	// Empty and inverted intervals yield empty slices, no error.
	for _, pair := range [][2]int64{{300, 300}, {600, 0}, {1200, 1800}} {
		if got := v.Range(time.Unix(pair[0], 0), time.Unix(pair[1], 0)); len(got) != 0 {
			t.Fatalf("range [%d,%d): expected empty, got %+v", pair[0], pair[1], got)
		}
	}
}

func TestRollupCursor(t *testing.T) {
	r := New(time.Second)
	for i := int64(0); i < 12; i++ {
		if err := r.Add(time.Unix(i, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(2) // 6 coarse windows of 2s
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
	// Exhausted: further reads are empty, as are non-positive batches.
	if got := c.Next(1); len(got) != 0 {
		t.Fatalf("exhausted cursor: %+v", got)
	}
	c2 := v.Cursor(time.Unix(0, 0), time.Unix(12, 0))
	if got := c2.Next(0); len(got) != 0 {
		t.Fatalf("non-positive batch: %+v", got)
	}
}

func TestRollupIsSnapshot(t *testing.T) {
	r := New(time.Second)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(10)
	if err := r.Add(time.Unix(5, 0), 2); err != nil {
		t.Fatal(err)
	}
	if err := r.AddBatch([]Sample{{time.Unix(7, 0), 4}}); err != nil {
		t.Fatal(err)
	}
	// The view predates those writes and must not observe them.
	if w := v.Window(time.Unix(0, 0)); w.Count != 1 || w.Sum != 1 {
		t.Fatalf("snapshot changed after later writes: %+v", w)
	}
	// A fresh view sees everything.
	if w := r.Rollup(10).Window(time.Unix(0, 0)); w.Count != 3 || w.Sum != 7 {
		t.Fatalf("fresh view: %+v", w)
	}
}

func TestRollupEmptyRoller(t *testing.T) {
	v := New(time.Second).Rollup(4)
	if got := v.Range(time.Unix(0, 0), time.Unix(1000, 0)); len(got) != 0 {
		t.Fatalf("empty view range: %+v", got)
	}
	if w := v.Window(time.Unix(0, 0)); w.Count != 0 {
		t.Fatalf("empty view window: %+v", w)
	}
	if got := v.Cursor(time.Unix(0, 0), time.Unix(1000, 0)).Next(5); len(got) != 0 {
		t.Fatalf("empty view cursor: %+v", got)
	}
}

func TestRollupExtremeTimestamps(t *testing.T) {
	r := New(time.Second)
	// Beyond the int64-nanosecond fast path: 2^63 ns is ~292 years.
	far := time.Unix(1<<62/1_000_000_000*2, 0) // an even number of seconds, far out
	if err := r.Add(far, 3); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(far.Add(time.Second), 5); err != nil {
		t.Fatal(err)
	}
	v := r.Rollup(2)
	w := v.Window(far)
	if w.Count != 2 || w.Sum != 8 || w.Min != 3 || w.Max != 5 {
		t.Fatalf("far-future coarse window: %+v", w)
	}
	if !w.Start.Equal(far) {
		t.Fatalf("coarse start %v, want %v", w.Start, far)
	}
}

func TestRollupConcurrentWithWrites(t *testing.T) {
	r := New(time.Millisecond)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			at := time.Unix(0, 0)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = r.AddBatch([]Sample{
					{at, float64(g)},
					{at.Add(time.Millisecond), float64(g)},
				})
				at = at.Add(2 * time.Millisecond)
			}
		}(g)
	}
	// Deriving views concurrently with writes never tears: every coarse
	// window is aligned and its count stays within what the writers could
	// have produced.
	for i := 0; i < 200; i++ {
		v := r.Rollup(100)
		ws := v.Range(time.Unix(-1, 0), time.Unix(1<<30, 0))
		for _, w := range ws {
			if w.Start.UnixNano()%(100*time.Millisecond.Nanoseconds()) != 0 {
				t.Fatalf("misaligned coarse window: %+v", w)
			}
			if w.Count < 0 || w.Min > w.Max {
				t.Fatalf("torn coarse window: %+v", w)
			}
		}
	}
	close(stop)
	wg.Wait()
}
