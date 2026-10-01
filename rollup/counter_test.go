package rollup

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// at returns the UTC time n nanoseconds since the epoch.
func at(n int64) time.Time { return time.Unix(0, n).UTC() }

func TestCounterBaselineFilesNoIncrement(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.Add(at(0), 100); err != nil {
		t.Fatal(err)
	}
	if got := c.Rates(at(math.MinInt64), at(math.MaxInt64)); len(got) != 0 {
		t.Fatalf("baseline produced windows: %+v", got)
	}
	// The second sample produces the first increment.
	if err := c.Add(at(500_000_000), 140); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(1_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %d windows, want 1: %+v", len(got), got)
	}
	w := got[0]
	if w.Count != 1 || w.Sum != 40 || w.Min != 40 || w.Max != 40 || w.Rate != 40 {
		t.Fatalf("window: %+v", w)
	}
}

func TestCounterResetAndEqualTimestamps(t *testing.T) {
	c := NewCounter(time.Second)
	samples := []Sample{
		{At: at(0), Value: 100},            // baseline
		{At: at(500_000_000), Value: 140},  // +40
		{At: at(1_000_000_000), Value: 10}, // reset: +10
		{At: at(1_500_000_000), Value: 30}, // +20
		{At: at(1_500_000_000), Value: 30}, // same ts: +0
		{At: at(1_500_000_000), Value: 5},  // same ts reset: +5
	}
	if err := c.AddBatch(samples); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(2_000_000_000))
	if len(got) != 2 {
		t.Fatalf("got %d windows: %+v", len(got), got)
	}
	if got[0].Start != at(0) || got[0].Count != 1 || got[0].Sum != 40 ||
		got[0].Min != 40 || got[0].Max != 40 || got[0].Rate != 40 {
		t.Fatalf("window 0: %+v", got[0])
	}
	w := got[1]
	if w.Start != at(1_000_000_000) || w.Count != 4 || w.Sum != 35 ||
		w.Min != 0 || w.Max != 20 || w.Rate != 35 {
		t.Fatalf("window 1s: %+v", w)
	}
}

func TestCounterResetToZeroThenRise(t *testing.T) {
	c := NewCounter(time.Minute)
	must := func(ns int64, v float64) {
		if err := c.Add(at(ns), v); err != nil {
			t.Fatal(err)
		}
	}
	must(0, 7)
	must(1, 3) // reset to 3
	must(2, 0) // reset to 0
	must(3, 0) // +0
	must(4, 9) // +9
	got := c.Rates(at(0), at(60_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	w := got[0]
	if w.Count != 4 || w.Sum != 12 || w.Min != 0 || w.Max != 9 {
		t.Fatalf("window: %+v", w)
	}
}

func TestCounterZeroIncrementsArePositiveZero(t *testing.T) {
	c := NewCounter(time.Second)
	must := func(ns int64, v float64) {
		t.Helper()
		if err := c.Add(at(ns), v); err != nil {
			t.Fatal(err)
		}
	}
	// Four non-negative increments that are all zero: a negative-zero
	// baseline followed by zeros, then a rise and a reset back to zero.
	// Every reported zero must carry the positive sign even though the
	// baseline sample was a negative zero.
	must(0, math.Copysign(0, -1))
	must(100, 0) // +0
	must(200, 0) // +0
	must(300, 0) // +0
	must(400, 0) // +0
	got := c.Rates(at(0), at(1_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	w := got[0]
	if w.Count != 4 || w.Sum != 0 || w.Min != 0 || w.Max != 0 || w.Rate != 0 {
		t.Fatalf("window: %+v", w)
	}
	for _, v := range []float64{w.Sum, w.Min, w.Rate} {
		if math.Signbit(v) {
			t.Fatalf("zero carries negative sign: %v", w)
		}
	}
}

func TestCounterRateUsesWindowSeconds(t *testing.T) {
	c := NewCounter(2500 * time.Millisecond)
	if err := c.AddBatch([]Sample{
		{At: at(0), Value: 0},
		{At: at(1_000_000), Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(2_500_000_000))
	if len(got) != 1 || got[0].Rate != 2 {
		t.Fatalf("rate = %+v, want 5/2.5 = 2", got)
	}
}

func TestCounterAttributesIncrementToLaterWindow(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch([]Sample{
		{At: at(900_000_000), Value: 0},
		// Crosses the 1s boundary; the increment belongs to window [1s,2s).
		{At: at(1_100_000_000), Value: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if got := c.Rates(at(0), at(1_000_000_000)); len(got) != 0 {
		t.Fatalf("increment leaked into earlier window: %+v", got)
	}
	got := c.Rates(at(1_000_000_000), at(2_000_000_000))
	if len(got) != 1 || got[0].Start != at(1_000_000_000) || got[0].Sum != 7 {
		t.Fatalf("got %+v", got)
	}
}

func TestCounterRatesSelection(t *testing.T) {
	c := NewCounter(time.Second)
	// One populated window per second at 0, 1, 3 seconds (a gap at 2).
	if err := c.AddBatch([]Sample{
		{At: at(0), Value: 0},
		{At: at(100), Value: 1},
		{At: at(1_000_000_000), Value: 2},
		{At: at(3_000_000_000), Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		from, to   int64
		wantStarts []int64
	}{
		{"full", 0, 4_000_000_000, []int64{0, 1_000_000_000, 3_000_000_000}},
		{"empty windows are skipped", 2_000_000_000, 2_500_000_000, nil},
		{"gap start still includes containing window", 1_500_000_000, 2_500_000_000, []int64{1_000_000_000}},
		{"half open at to", 0, 1_000_000_000, []int64{0}},
		{"from inside a window", 900_000_000, 1_100_000_000, []int64{0, 1_000_000_000}},
		{"start before first", -1_000_000_000, 500_000_000, []int64{0}},
		{"empty interval", 1_000_000_000, 1_000_000_000, nil},
		{"backwards interval", 2_000_000_000, 1_000_000_000, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Rates(at(tc.from), at(tc.to))
			if len(got) != len(tc.wantStarts) {
				t.Fatalf("got %+v, want starts %v", got, tc.wantStarts)
			}
			for i, ws := range tc.wantStarts {
				if got[i].Start != at(ws) {
					t.Fatalf("window %d starts at %v, want %d", i, got[i].Start, ws)
				}
			}
		})
	}
}

func TestCounterEmptyRatesIsNotEmptySlice(t *testing.T) {
	c := NewCounter(time.Second)
	if got := c.Rates(at(0), at(1)); got == nil || len(got) != 0 {
		t.Fatalf("empty rates = %v", got)
	}
}

func TestRateCursorBatches(t *testing.T) {
	c := NewCounter(time.Second)
	var samples []Sample
	samples = append(samples, Sample{At: at(0), Value: 0})
	for i := int64(1); i <= 5; i++ {
		samples = append(samples, Sample{At: at(i * 1_000_000_000), Value: float64(i)})
	}
	if err := c.AddBatch(samples); err != nil {
		t.Fatal(err)
	}
	cur := c.RateCursor(at(0), at(6_000_000_000))
	var seen []RateWindow
	seen = append(seen, cur.Next(2)...)
	seen = append(seen, cur.Next(2)...)
	rest := cur.Next(10)
	if len(rest) != 1 {
		t.Fatalf("final batch = %d windows, want 1", len(rest))
	}
	seen = append(seen, rest...)
	if len(seen) != 5 {
		t.Fatalf("saw %d windows, want 5", len(seen))
	}
	for i := range seen {
		// The first sample is the baseline, so the populated windows are
		// 1s..5s.
		if want := at(int64(i+1) * 1_000_000_000); seen[i].Start != want {
			t.Fatalf("window %d: %v, want %v", i, seen[i].Start, want)
		}
	}
	// Exhausted: every later Next is an empty batch and never moves.
	if b := cur.Next(1); len(b) != 0 {
		t.Fatalf("exhausted cursor returned %+v", b)
	}
	if b := cur.Next(0); len(b) != 0 {
		t.Fatalf("non-positive batch returned %+v", b)
	}
}

func TestRateCursorIsSnapshot(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch([]Sample{
		{At: at(0), Value: 0},
		{At: at(100), Value: 1},
	}); err != nil {
		t.Fatal(err)
	}
	cur := c.RateCursor(at(0), at(10_000_000_000))
	snap := c.Rates(at(0), at(10_000_000_000))
	if err := c.AddBatch([]Sample{
		{At: at(2_000_000_000), Value: 2},
		{At: at(8_000_000_000), Value: 99},
	}); err != nil {
		t.Fatal(err)
	}
	if got := cur.Next(100); len(got) != 1 {
		t.Fatalf("cursor changed after later writes: %+v", got)
	}
	if len(snap) != 1 || snap[0].Sum != 1 {
		t.Fatalf("Rates snapshot changed: %+v", snap)
	}
}

func TestCounterRejections(t *testing.T) {
	c := NewCounter(10 * time.Second)
	if err := c.Add(at(100), math.NaN()); !errors.Is(err, ErrNonFinite) {
		t.Fatalf("NaN: got %v", err)
	}
	if err := c.Add(at(100), math.Inf(1)); !errors.Is(err, ErrNonFinite) {
		t.Fatalf("+Inf: got %v", err)
	}
	if err := c.Add(at(100), -0.0); err != nil {
		t.Fatalf("negative zero is not negative: %v", err)
	}
	if err := c.Add(at(200), -1); !errors.Is(err, ErrNegativeCounter) {
		t.Fatalf("negative: got %v", err)
	}
	// Establish a baseline and an increment, then verify each rejection
	// leaves baseline, marker, and windows untouched.
	if err := c.Add(at(300), 10); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(400), 12); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(399), 100); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("out of order: got %v", err)
	}
	if err := c.Add(at(500), math.NaN()); !errors.Is(err, ErrNonFinite) {
		t.Fatalf("late NaN: got %v", err)
	}
	if err := c.Add(at(500), -3); !errors.Is(err, ErrNegativeCounter) {
		t.Fatalf("late negative: got %v", err)
	}
	// The last accepted value is still 12 at t=400: 14 adds 2.
	if err := c.Add(at(500), 14); err != nil {
		t.Fatalf("add after rejections: %v", err)
	}
	// Accepted so far: baseline 0 at t=100, then +10, +2, and +2; the
	// rejected negative sample never became the baseline.
	got := c.Rates(at(0), at(10_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	if got[0].Count != 3 || got[0].Sum != 14 {
		t.Fatalf("rejections mutated state: %+v", got[0])
	}
	// Same instant as the marker is still accepted and resets.
	if err := c.Add(at(500), 3); err != nil {
		t.Fatalf("same-instant reset: %v", err)
	}
	got = c.Rates(at(0), at(10_000_000_000))
	if got[0].Count != 4 || got[0].Sum != 17 || got[0].Min != 2 || got[0].Max != 10 {
		t.Fatalf("same-instant reset window: %+v", got[0])
	}
}

func TestCounterAddBatchRejectsAsUnit(t *testing.T) {
	good := []Sample{
		{At: at(0), Value: 0},
		{At: at(100), Value: 5},
	}
	if err := NewCounter(time.Second).AddBatch(good); err != nil {
		t.Fatal(err)
	}
	for name, batch := range map[string][]Sample{
		"non-finite": {{At: at(200), Value: 1}, {At: at(300), Value: math.NaN()}},
		"negative":   {{At: at(200), Value: 1}, {At: at(300), Value: -2}},
		"stale":      {{At: at(50), Value: 9}},
	} {
		t.Run(name, func(t *testing.T) {
			c := NewCounter(time.Second)
			if err := c.AddBatch(good); err != nil {
				t.Fatal(err)
			}
			before := c.Rates(at(0), at(1_000_000_000))
			err := c.AddBatch(batch)
			switch name {
			case "non-finite":
				if !errors.Is(err, ErrNonFinite) {
					t.Fatalf("got %v", err)
				}
			case "negative":
				if !errors.Is(err, ErrNegativeCounter) {
					t.Fatalf("got %v", err)
				}
			case "stale":
				if !errors.Is(err, ErrOutOfOrder) {
					t.Fatalf("got %v", err)
				}
			}
			after := c.Rates(at(0), at(1_000_000_000))
			if len(after) != len(before) || after[0].Count != before[0].Count ||
				after[0].Sum != before[0].Sum {
				t.Fatalf("rejected batch changed state: before %+v after %+v", before, after)
			}
			// A valid batch after the rejection still works against the
			// pre-batch baseline of 5 at t=100.
			if err := c.AddBatch([]Sample{{At: at(200), Value: 8}}); err != nil {
				t.Fatalf("add after rejected batch: %v", err)
			}
		})
	}
}

// TestCounterAddBatchUnsortedKeepsWindowsSorted files a batch whose
// samples are out of time order. Increments still follow arrival (slice)
// order — including the reset — but the resulting rate windows must be
// emitted in time order.
func TestCounterAddBatchUnsortedKeepsWindowsSorted(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch([]Sample{{At: at(0), Value: 0}}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddBatch([]Sample{
		{At: at(5_000_000_000), Value: 10}, // +10 -> window 5s
		{At: at(1_000_000_000), Value: 5},  // reset: +5 -> window 1s
		{At: at(5_500_000_000), Value: 12}, // 12-5 = +7 -> window 5s
	}); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(10_000_000_000))
	want := []RateWindow{
		{Start: at(1_000_000_000), Count: 1, Sum: 5, Min: 5, Max: 5, Rate: 5},
		{Start: at(5_000_000_000), Count: 2, Sum: 17, Min: 7, Max: 10, Rate: 17},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("window %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

func TestCounterAddBatchEmpty(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch(nil); err != nil {
		t.Fatal(err)
	}
	if err := c.AddBatch([]Sample{}); err != nil {
		t.Fatal(err)
	}
	if got := c.Rates(at(0), at(1)); len(got) != 0 {
		t.Fatalf("empty batch produced windows: %+v", got)
	}
	// Marker never advanced: any first instant is accepted.
	if err := c.AddBatch([]Sample{{At: at(123), Value: 1}}); err != nil {
		t.Fatalf("first batch after empty batches: %v", err)
	}
}

func TestCounterAddAndAddBatchAgree(t *testing.T) {
	// The same logical observations filed one at a time and as one batch
	// must produce bit-identical windows; both routes use the same exact
	// per-window sum, so submission interleave cannot change a bit.
	values := []float64{0, 4.7, 4.7, 9.25, 3.5, 3.5, 8.0}
	build := func(useBatch bool) []RateWindow {
		c := NewCounter(2 * time.Second)
		if useBatch {
			batch := make([]Sample, len(values))
			for i, v := range values {
				batch[i] = Sample{At: at(int64(i) * 300_000_000), Value: v}
			}
			if err := c.AddBatch(batch); err != nil {
				t.Fatal(err)
			}
		} else {
			for i, v := range values {
				if err := c.Add(at(int64(i)*300_000_000), v); err != nil {
					t.Fatal(err)
				}
			}
		}
		return c.Rates(at(math.MinInt64), at(math.MaxInt64))
	}
	a, b := build(false), build(true)
	if len(a) != len(b) {
		t.Fatalf("%+v != %+v", a, b)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("window %d:\n add  %+v\n batch %+v", i, a[i], b[i])
		}
	}
}

func TestNewCounterBadWindowPanics(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Nanosecond} {
		func() {
			defer func() {
				if r := recover(); r != "rollup: bad window" {
					t.Fatalf("panic = %v", r)
				}
			}()
			NewCounter(d)
		}()
	}
}

func TestCounterConcurrentAddAndRead(t *testing.T) {
	c := NewCounter(2500 * time.Millisecond)
	const writers = 8
	const perWriter = 200

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 2; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, w := range c.Rates(at(math.MinInt64), at(math.MaxInt64)) {
					if w.Count <= 0 || w.Min < 0 || w.Max < w.Min || w.Sum < 0 || w.Rate < 0 {
						t.Errorf("inconsistent snapshot: %+v", w)
					}
				}
				cur := c.RateCursor(at(0), at(perWriter*int64(2500*time.Millisecond)))
				cur.Next(7)
			}
		}()
	}

	// Writers share one counter and one time axis, so a writer that loses
	// the ordering race is rejected with ErrOutOfOrder exactly as for a
	// Roller; count the accepted samples. Values are positive and strictly
	// rise per writer, so no other error is possible.
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				ns := int64(i)*int64(2500*time.Millisecond) + int64(w)
				err := c.Add(at(ns), float64(i+1))
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrOutOfOrder):
					// Lost the race to a later sample; fine.
				default:
					t.Errorf("Add: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	var total int64
	for _, win := range c.Rates(at(math.MinInt64), at(math.MaxInt64)) {
		total += win.Count
	}
	// Exactly one accepted sample is the global baseline and files no
	// increment; every other accepted sample files exactly one.
	if accepted.Load() == 0 || total != accepted.Load()-1 {
		t.Fatalf("total increments = %d, want %d", total, accepted.Load()-1)
	}
}
