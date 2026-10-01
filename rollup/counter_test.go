package rollup

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// at builds a UTC instant from nanoseconds since the epoch.
func at(ns int64) time.Time { return time.Unix(0, ns).UTC() }

func TestCounterFirstSampleIsBaselineOnly(t *testing.T) {
	c := NewCounter(time.Minute)
	if err := c.Add(at(0), 42); err != nil {
		t.Fatal(err)
	}
	if got := c.Rates(at(math.MinInt64), at(math.MaxInt64)); len(got) != 0 {
		t.Fatalf("baseline produced windows: %+v", got)
	}
	// A later reading produces the one increment, and the baseline value
	// is not counted.
	if err := c.Add(at(60_000_000_000), 47); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 1 || got[0].Count != 1 || got[0].Sum != 5 || got[0].Min != 5 || got[0].Max != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestCounterIncrementsAndReset(t *testing.T) {
	c := NewCounter(time.Second)
	readings := []struct {
		ns    int64
		value float64
	}{
		{0, 5},             // baseline
		{100, 10},          // +5
		{200, 12},          // +2
		{1_000_000_000, 2}, // reset: +2
		{1_500_000_000, 2}, // +0
		{2_000_000_000, 9}, // +7
	}
	for _, r := range readings {
		if err := c.Add(at(r.ns), r.value); err != nil {
			t.Fatal(err)
		}
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 3 {
		t.Fatalf("got %d windows: %+v", len(got), got)
	}
	if w := got[0]; w.Count != 2 || w.Sum != 7 || w.Min != 2 || w.Max != 5 || w.Rate != 7 {
		t.Fatalf("window 0: %+v", w)
	}
	if w := got[1]; w.Count != 2 || w.Sum != 2 || w.Min != 0 || w.Max != 2 || w.Rate != 2 {
		t.Fatalf("window 1: %+v", w)
	}
	if w := got[2]; w.Count != 1 || w.Sum != 7 || w.Min != 7 || w.Max != 7 || w.Rate != 7 {
		t.Fatalf("window 2: %+v", w)
	}
}

func TestCounterSameInstantInArrivalOrder(t *testing.T) {
	c := NewCounter(time.Second)
	must := func(value float64) {
		t.Helper()
		if err := c.Add(at(0), value); err != nil {
			t.Fatal(err)
		}
	}
	must(10) // baseline
	must(7)  // reset at the same instant: increment 7
	must(7)  // equal: zero increment
	must(9)  // increase: increment 2
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	w := got[0]
	if w.Count != 3 || w.Sum != 9 || w.Min != 0 || w.Max != 7 || w.Rate != 9 {
		t.Fatalf("window: %+v", w)
	}
}

func TestCounterIncrementAttributedToLaterSampleWindow(t *testing.T) {
	c := NewCounter(time.Second)
	// Baseline just before a boundary; the first increment lands exactly
	// on the boundary and belongs to the later window.
	if err := c.Add(at(999_999_999), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(1_000_000_000), 3); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 1 || !got[0].Start.Equal(at(1_000_000_000)) || got[0].Sum != 3 {
		t.Fatalf("got %+v", got)
	}
}

func TestCounterRatesRangeIsHalfOpenAndSkipsEmptyWindows(t *testing.T) {
	c := NewCounter(time.Second)
	readings := []struct {
		ns    int64
		value float64
	}{
		{0, 1},             // baseline; window 0 holds no increment
		{500_000_000, 1},   // zero increment: window 0 now counts
		{2_000_000_000, 2}, // window 2; window 1 never opens
		{4_000_000_000, 3}, // window 4
	}
	for _, r := range readings {
		if err := c.Add(at(r.ns), r.value); err != nil {
			t.Fatal(err)
		}
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 3 {
		t.Fatalf("windows without increments must be absent: %+v", got)
	}

	// [2s, 4s): the window containing 2s is included, the one at 4s not.
	got = c.Rates(at(2_000_000_000), at(4_000_000_000))
	if len(got) != 1 || !got[0].Start.Equal(at(2_000_000_000)) {
		t.Fatalf("half-open range: %+v", got)
	}

	// An endpoint inside a window selects the whole window.
	got = c.Rates(at(3_900_000_000), at(4_500_000_000))
	if len(got) != 1 || !got[0].Start.Equal(at(4_000_000_000)) {
		t.Fatalf("window containing from: %+v", got)
	}

	for _, r := range [][2]int64{
		{1_000_000_000, 1_000_000_000}, // equal endpoints
		{2_000_000_000, 1_000_000_000}, // backwards
		{5_000_000_000, 6_000_000_000}, // no window inside
	} {
		if got := c.Rates(at(r[0]), at(r[1])); len(got) != 0 {
			t.Fatalf("range %d..%d: %+v", r[0], r[1], got)
		}
	}
}

func TestCounterRateDividesByWindowSeconds(t *testing.T) {
	c := NewCounter(2500 * time.Millisecond)
	if err := c.Add(at(0), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(1_000_000), 3); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(10_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	if want := 3.0 / 2.5; got[0].Rate != want {
		t.Fatalf("rate = %v, want %v", got[0].Rate, want)
	}
}

func TestCounterZeroIncrementsArePositiveZero(t *testing.T) {
	c := NewCounter(time.Second)
	// A negative-zero baseline and a reset to negative zero must still
	// file positive-zero increments and a positive-zero rate.
	negZero := math.Copysign(0, -1)
	for _, v := range []float64{negZero, 0, 10, negZero, negZero, 5} {
		if err := c.Add(at(0), v); err != nil {
			t.Fatal(err)
		}
	}
	got := c.Rates(at(0), at(1_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	w := got[0]
	// Baseline -0; deltas are: equal +0 -> 0, rise to 10, reset to -0
	// -> 0, equal -0 -> 0, rise to 5: five deltas summing to 15.
	if w.Count != 5 || w.Sum != 15 || w.Min != 0 || w.Max != 10 {
		t.Fatalf("window: %+v", w)
	}
	for _, z := range []float64{w.Sum, w.Min, w.Rate} {
		if math.Signbit(z) {
			t.Fatalf("negative sign on zero value: %v", z)
		}
	}
}

func TestCounterSumRoundsOnce(t *testing.T) {
	c := NewCounter(time.Second)
	// Baseline 0; a jump to 1e16 (delta 1e16), then a reset to 1 and two
	// unit increases make deltas 1e16, 1, 1, 1. The exact real sum is
	// 10000000000000003, which rounds to 10000000000000004; rounding
	// after each float64 addition would stay at 1e16.
	for _, v := range []float64{0, 1e16, 1, 2, 3} {
		if err := c.Add(at(0), v); err != nil {
			t.Fatal(err)
		}
	}
	got := c.Rates(at(0), at(1_000_000_000))
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	if got[0].Sum != 10000000000000004 {
		t.Fatalf("sum = %v, want 10000000000000004", got[0].Sum)
	}
	if got[0].Count != 4 {
		t.Fatalf("count = %d", got[0].Count)
	}
}

func TestCounterAddRejections(t *testing.T) {
	c := NewCounter(time.Minute)
	if err := c.Add(at(0), 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		at    time.Time
		value float64
		want  error
	}{
		{"nan", at(1), math.NaN(), ErrNonFinite},
		{"plus inf", at(1), math.Inf(1), ErrNonFinite},
		{"minus inf", at(1), math.Inf(-1), ErrNonFinite},
		{"negative", at(1), -0.5, ErrNegativeCounter},
		{"negative zero is allowed", at(1), math.Copysign(0, -1), nil},
		{"out of order", at(-1), 2, ErrOutOfOrder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Add(tc.at, tc.value)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Add = %v, want %v", err, tc.want)
			}
		})
	}
	// The rejected readings changed nothing: the negative-zero reading
	// was the one accepted addition, producing one zero increment; the
	// marker still accepts t=1 but rejects t=0's strict predecessors.
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 1 || got[0].Count != 1 || got[0].Sum != 0 {
		t.Fatalf("state after rejections: %+v", got)
	}
	if err := c.Add(at(1), 1); err != nil {
		t.Fatalf("same instant must stay accepted: %v", err)
	}
}

func TestCounterAddBatchValidation(t *testing.T) {
	good := []Sample{{At: at(0), Value: 0}, {At: at(1), Value: 5}}

	for _, tc := range []struct {
		name    string
		samples []Sample
		want    error
	}{
		{"nan", []Sample{{At: at(0), Value: 0}, {At: at(1), Value: math.NaN()}}, ErrNonFinite},
		{"nan first", []Sample{{At: at(0), Value: math.NaN()}}, ErrNonFinite},
		{"negative", []Sample{{At: at(0), Value: 0}, {At: at(1), Value: -1}}, ErrNegativeCounter},
		{"negative first", []Sample{{At: at(0), Value: -1}}, ErrNegativeCounter},
		{"nan beats negative", []Sample{{At: at(0), Value: -1}, {At: at(1), Value: math.NaN()}}, ErrNonFinite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCounter(time.Minute)
			if err := c.AddBatch(tc.samples); !errors.Is(err, tc.want) {
				t.Fatalf("AddBatch = %v, want %v", err, tc.want)
			}
			if got := c.Rates(at(math.MinInt64), at(math.MaxInt64)); len(got) != 0 {
				t.Fatalf("rejected batch left state: %+v", got)
			}
			// The baseline was never established: a later reading is a
			// fresh baseline and files no increment.
			if err := c.AddBatch(good); err != nil {
				t.Fatal(err)
			}
			if got := c.Rates(at(math.MinInt64), at(math.MaxInt64)); len(got) != 1 || got[0].Sum != 5 {
				t.Fatalf("state after rejected batch: %+v", got)
			}
		})
	}
}

func TestCounterAddBatchAtomicOnOutOfOrder(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch([]Sample{{At: at(5_000_000_000), Value: 3}}); err != nil {
		t.Fatal(err)
	}
	// One stale reading rejects the whole batch, including the increments
	// its well-ordered readings would have filed.
	err := c.AddBatch([]Sample{
		{At: at(6_000_000_000), Value: 4},
		{At: at(4_000_000_000), Value: 5},
	})
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("AddBatch = %v, want ErrOutOfOrder", err)
	}
	if got := c.Rates(at(math.MinInt64), at(math.MaxInt64)); len(got) != 0 {
		t.Fatalf("rejected batch left increments: %+v", got)
	}
	// Baseline and marker unchanged.
	if err := c.Add(at(5_000_000_000), 3); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 1 || got[0].Sum != 0 {
		t.Fatalf("equal reading after marker: %+v", got)
	}
}

func TestCounterAddBatchFilesInSliceOrder(t *testing.T) {
	c := NewCounter(time.Second)
	// Internally out of time order, but every reading is at or after the
	// marker. Increments follow slice order and land in backfilled older
	// windows.
	batch := []Sample{
		{At: at(2_000_000_000), Value: 10}, // baseline
		{At: at(1_000_000_000), Value: 5},  // reset: 5 in window 1
		{At: at(2_000_000_000), Value: 12}, // +7 in window 2
		{At: at(500_000_000), Value: 2},    // reset: 2 in window 0
	}
	if err := c.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(math.MinInt64), at(math.MaxInt64))
	if len(got) != 3 {
		t.Fatalf("got %+v", got)
	}
	if !got[0].Start.Equal(at(0)) || got[0].Sum != 2 {
		t.Fatalf("window 0: %+v", got[0])
	}
	if !got[1].Start.Equal(at(1_000_000_000)) || got[1].Sum != 5 {
		t.Fatalf("window 1: %+v", got[1])
	}
	if !got[2].Start.Equal(at(2_000_000_000)) || got[2].Count != 1 || got[2].Sum != 7 {
		t.Fatalf("window 2: %+v", got[2])
	}
	// The marker advanced to the newest timestamp; an earlier reading is
	// now out of order.
	if err := c.AddBatch([]Sample{{At: at(1_999_999_999), Value: 100}}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("marker did not advance: %v", err)
	}
}

func TestCounterBatchesContinueFromLastReading(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.AddBatch(nil); err != nil {
		t.Fatalf("nil batch: %v", err)
	}
	if err := c.AddBatch([]Sample{{At: at(0), Value: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddBatch([]Sample{{At: at(1), Value: 5}, {At: at(2), Value: 10}}); err != nil {
		t.Fatal(err)
	}
	got := c.Rates(at(0), at(1_000_000_000))
	if len(got) != 1 || got[0].Count != 2 || got[0].Sum != 5 || got[0].Min != 0 || got[0].Max != 5 {
		t.Fatalf("cross-batch continuity: %+v", got)
	}
}

func TestCounterRateCursorBatches(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.Add(at(0), 0); err != nil {
		t.Fatal(err)
	}
	// Readings advance by two every second, so every window in range
	// holds one increment of 2 and a rate of 2.
	for i := int64(1); i <= 5; i++ {
		if err := c.Add(at(i*1_000_000_000), float64(i*2)); err != nil {
			t.Fatal(err)
		}
	}
	cur := c.RateCursor(at(0), at(5_000_000_000))
	var seen []RateWindow
	// Non-positive batch sizes hand out nothing and do not advance.
	if b := cur.Next(0); len(b) != 0 {
		t.Fatalf("Next(0) = %+v", b)
	}
	if b := cur.Next(-1); len(b) != 0 {
		t.Fatalf("Next(-1) = %+v", b)
	}
	for {
		batch := cur.Next(2)
		if len(batch) == 0 {
			break
		}
		if len(batch) > 2 {
			t.Fatalf("batch larger than requested: %d", len(batch))
		}
		seen = append(seen, batch...)
	}
	if len(seen) != 4 {
		t.Fatalf("cursor covered %d windows, want 4: %+v", len(seen), seen)
	}
	for i, w := range seen {
		if !w.Start.Equal(at(int64(i+1) * 1_000_000_000)) {
			t.Fatalf("window %d start: %v", i, w.Start)
		}
		if w.Rate != 2 {
			t.Fatalf("window %d rate: %v", i, w.Rate)
		}
	}
	// Exhausted cursors keep returning empty batches.
	if b := cur.Next(2); len(b) != 0 {
		t.Fatalf("exhausted cursor returned %+v", b)
	}
}

func TestCounterRateCursorIsSnapshot(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.Add(at(0), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(1_000_000_000), 1); err != nil {
		t.Fatal(err)
	}
	cur := c.RateCursor(at(0), at(100_000_000_000))
	if err := c.Add(at(2_000_000_000), 2); err != nil {
		t.Fatal(err)
	}
	batch := cur.Next(100)
	if len(batch) != 1 {
		t.Fatalf("cursor saw a later add: %+v", batch)
	}
	// A fresh cursor sees both windows.
	fresh := c.RateCursor(at(0), at(100_000_000_000))
	if b := fresh.Next(100); len(b) != 2 {
		t.Fatalf("fresh cursor: %+v", b)
	}
}

func TestCounterRatesIsSnapshot(t *testing.T) {
	c := NewCounter(time.Second)
	if err := c.Add(at(0), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(at(1_000_000_000), 1); err != nil {
		t.Fatal(err)
	}
	snap := c.Rates(at(0), at(100_000_000_000))
	if err := c.Add(at(2_000_000_000), 2); err != nil {
		t.Fatal(err)
	}
	if len(snap) != 1 || snap[0].Sum != 1 {
		t.Fatalf("snapshot changed: %+v", snap)
	}
}

func TestNewCounterBadWindowPanics(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
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

// TestCounterConcurrentAddAndRead files readings from many goroutines
// while others read rates; the final increment count must account for
// every accepted reading after the first exactly once. Run with -race.
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
				ws := c.Rates(at(math.MinInt64), at(math.MaxInt64))
				for _, w := range ws {
					if w.Count <= 0 || w.Min < 0 || w.Max < w.Min || w.Rate < 0 {
						t.Errorf("inconsistent snapshot: %+v", w)
					}
				}
				c.RateCursor(at(0), at(1<<62)).Next(16)
			}
		}()
	}

	// Every writer emits non-decreasing readings on its own lane; lanes
	// interleave, so a reading can legitimately lose the out-of-order
	// race. Values never decrease, so no resets occur.
	var accepted atomic.Int64
	var writersWg sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			for i := 0; i < perWriter; i++ {
				ns := int64(i)*int64(2500*time.Millisecond) + int64(w)
				err := c.Add(at(ns), float64(i+1)*1000+float64(w))
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, ErrOutOfOrder):
				default:
					t.Errorf("Add: %v", err)
				}
			}
		}(w)
	}
	writersWg.Wait()
	close(stop)
	readers.Wait()

	var total int64
	for _, w := range c.Rates(at(math.MinInt64), at(math.MaxInt64)) {
		total += w.Count
	}
	// The first accepted reading only sets the baseline; every later
	// accepted reading files exactly one increment.
	if want := accepted.Load() - 1; total != want {
		t.Fatalf("total increments = %d, want %d", total, want)
	}
}
