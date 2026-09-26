package rollup

import (
	"sync"
	"testing"
	"time"
)

// fillRoller files n samples, one per window, starting at the Unix epoch.
func fillRoller(t *testing.T, window time.Duration, n int) *Roller {
	t.Helper()
	r := New(window)
	for i := 0; i < n; i++ {
		if err := r.Add(time.Unix(0, 0).Add(time.Duration(i)*window), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func TestRangeOrderedAndBounded(t *testing.T) {
	r := fillRoller(t, time.Minute, 10) // windows at 0m..9m, values 0..

	got := r.Range(time.Unix(120, 0), time.Unix(300, 0))
	if len(got) != 3 {
		t.Fatalf("Range(2m, 5m): got %d windows, want 3", len(got))
	}
	for i, sec := range []int64{120, 180, 240} {
		w := got[i]
		if !w.Start.Equal(time.Unix(sec, 0)) {
			t.Fatalf("window %d starts at %v, want %ds", i, w.Start, sec)
		}
		want := float64(sec / 60)
		if w.Count != 1 || w.Sum != want || w.Min != want || w.Max != want {
			t.Fatalf("window %ds: %+v", sec, w)
		}
	}

	// Bounds landing exactly on window starts follow the left-closed,
	// right-open convention: from's window is in, to's window is out.
	got = r.Range(time.Unix(0, 0), time.Unix(600, 0))
	if len(got) != 10 {
		t.Fatalf("Range(0, 10m): got %d windows, want 10", len(got))
	}
	// Bounds inside a window locate by window start.
	got = r.Range(time.Unix(90, 0), time.Unix(270, 0))
	if len(got) != 3 || !got[0].Start.Equal(time.Unix(120, 0)) {
		t.Fatalf("Range(90s, 270s): got %v", got)
	}
}

func TestRangeEmptyResults(t *testing.T) {
	r := fillRoller(t, time.Minute, 5)
	cases := []struct {
		name     string
		from, to time.Time
	}{
		{"empty roller interval", time.Unix(600, 0), time.Unix(1200, 0)},
		{"between windows", time.Unix(10, 0), time.Unix(50, 0)},
		{"from == to", time.Unix(60, 0), time.Unix(60, 0)},
		{"from after to", time.Unix(120, 0), time.Unix(60, 0)},
	}
	for _, c := range cases {
		if got := r.Range(c.from, c.to); len(got) != 0 {
			t.Errorf("%s: got %v, want empty", c.name, got)
		}
	}
	if got := New(time.Minute).Range(time.Unix(0, 0), time.Unix(600, 0)); len(got) != 0 {
		t.Errorf("empty roller: got %v, want empty", got)
	}
}

// TestRangeAcrossBackfill checks that a range spanning the main store and
// backfilled windows returns one ordered sequence with intact statistics.
func TestRangeAcrossBackfill(t *testing.T) {
	a := New(time.Minute)
	if err := a.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	b := fillRoller(t, time.Minute, 0)
	for i := 100; i < 110; i++ {
		if err := b.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 100; i++ {
		if err := a.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatalf("backfill %d: %v", i, err)
		}
	}

	got := a.Range(time.Unix(95*60, 0), time.Unix(105*60, 0))
	if len(got) != 10 {
		t.Fatalf("got %d windows, want 10", len(got))
	}
	for i, w := range got {
		want := time.Unix(int64(95+i)*60, 0)
		if !w.Start.Equal(want) {
			t.Fatalf("window %d starts at %v, want %v", i, w.Start, want)
		}
		if w.Count != 1 || w.Sum != float64(95+i) {
			t.Fatalf("window %v: %+v", want, w)
		}
	}

	// A range covering everything matches Windows().
	all := a.Range(time.Unix(-1, 0), time.Unix(110*60, 0))
	if want := a.Windows(); len(all) != len(want) {
		t.Fatalf("full range: got %d windows, want %d", len(all), len(want))
	} else {
		for i := range all {
			if all[i] != want[i] {
				t.Fatalf("window %d: range %+v, windows %+v", i, all[i], want[i])
			}
		}
	}
}

// TestRangeExtremeTimestamps queries ranges with bounds far outside the
// int64 nanosecond range; the comparison-only path must not overflow.
func TestRangeExtremeTimestamps(t *testing.T) {
	r := New(10 * time.Second)
	for _, at := range []time.Time{
		time.Unix(-1<<62, 987654321),
		time.Unix(0, 0),
		time.Unix(1<<62, 123456789),
	} {
		if err := r.Add(at, 5); err != nil {
			t.Fatal(err)
		}
	}
	// The extreme windows start a few seconds before their samples (10s
	// alignment), so the bounds reach a little past the samples.
	got := r.Range(time.Unix(-(1<<62)-16, 0), time.Unix(1<<62+16, 0))
	if len(got) != 3 {
		t.Fatalf("full span: got %d windows, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if !got[i-1].Start.Before(got[i].Start) {
			t.Fatalf("out of order at %d: %v then %v", i, got[i-1].Start, got[i].Start)
		}
	}
	got = r.Range(time.Unix(1<<62-16, 0), time.Unix(1<<62, 999999999))
	if len(got) != 1 || got[0].Sum != 5 {
		t.Fatalf("far future: got %v", got)
	}
}

func TestCursorBatches(t *testing.T) {
	r := fillRoller(t, time.Minute, 10)
	c := r.Cursor()

	var got []Window
	for _, n := range []int{4, 4, 4} {
		got = append(got, c.Next(n)...)
	}
	if len(got) != 10 {
		t.Fatalf("drained %d windows, want 10", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i)*60, 0)) || w.Sum != float64(i) {
			t.Fatalf("window %d: %+v", i, w)
		}
	}
	// Exhausted: empty segments, cursor stays put.
	for i := 0; i < 3; i++ {
		if batch := c.Next(4); len(batch) != 0 {
			t.Fatalf("exhausted cursor: got %v", batch)
		}
	}
	// A non-positive batch yields an empty segment without advancing.
	c2 := r.Cursor()
	if batch := c2.Next(0); len(batch) != 0 {
		t.Fatalf("Next(0): got %v", batch)
	}
	if batch := c2.Next(1); len(batch) != 1 {
		t.Fatalf("after Next(0): got %d windows, want 1", len(batch))
	}
}

// TestCursorSnapshotIsolation drains a cursor while samples are filed,
// backfilled, and merged; the cursor must see only the creation-time state.
func TestCursorSnapshotIsolation(t *testing.T) {
	r := fillRoller(t, time.Minute, 5)
	c := r.Cursor()

	// New windows, extra samples in existing windows, and a merge all
	// happen after the snapshot and must be invisible to the cursor.
	if err := r.Add(time.Unix(240, 0), 1000); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(300, 0), 99); err != nil {
		t.Fatal(err)
	}
	if err := r.Merge(fillRoller(t, time.Minute, 8)); err != nil {
		t.Fatal(err)
	}

	var got []Window
	for {
		batch := c.Next(2)
		if len(batch) == 0 {
			break
		}
		got = append(got, batch...)
	}
	if len(got) != 5 {
		t.Fatalf("cursor saw %d windows, want the 5 at creation", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i)*60, 0)) {
			t.Fatalf("window %d starts at %v", i, w.Start)
		}
		if w.Count != 1 || w.Sum != float64(i) {
			t.Fatalf("window %d mutated after snapshot: %+v", i, w)
		}
	}
}

// TestCursorConcurrentWrites drains a cursor while other goroutines keep
// filing and merging; every batch must come from the creation snapshot.
func TestCursorConcurrentWrites(t *testing.T) {
	r := fillRoller(t, time.Second, 100)
	c := r.Cursor()
	src := fillRoller(t, time.Second, 3)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for j := int64(0); ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				r.Add(time.Unix(base+j, 0), 1) // may lose the order race; fine
				r.Merge(src)
			}
		}(int64(1000 + i*100000))
	}

	var got []Window
	for {
		batch := c.Next(7)
		if len(batch) == 0 {
			break
		}
		got = append(got, batch...)
	}
	close(stop)
	wg.Wait()

	if len(got) != 100 {
		t.Fatalf("cursor saw %d windows, want 100", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i), 0)) || w.Count != 1 || w.Sum != float64(i) {
			t.Fatalf("window %d torn or duplicated: %+v", i, w)
		}
	}
}

func TestCursorEmpty(t *testing.T) {
	c := New(time.Minute).Cursor()
	if batch := c.Next(10); len(batch) != 0 {
		t.Fatalf("empty roller: got %v", batch)
	}
}

// TestBackfillKeepsTailPut verifies structurally that backfilling older
// windows does not grow or move the accepted tail of the main store.
func TestBackfillKeepsTailPut(t *testing.T) {
	a := New(time.Minute)
	if err := a.Add(time.Unix(0, 0), 0); err != nil {
		t.Fatal(err)
	}
	b := fillRoller(t, time.Minute, 0)
	for i := 100; i < 110; i++ {
		if err := b.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	tail := len(a.buckets)
	for i := 1; i < 100; i++ {
		if err := a.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatalf("backfill %d: %v", i, err)
		}
	}
	if len(a.buckets) != tail {
		t.Fatalf("backfill moved the accepted tail: %d buckets, want %d", len(a.buckets), tail)
	}
	// Statistics and order are still exact after the merge flattens the
	// side store back in.
	c := fillRoller(t, time.Minute, 0)
	for i := 0; i < 110; i++ {
		if err := c.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(c); err != nil {
		t.Fatal(err)
	}
	got := a.Windows()
	if len(got) != 110 {
		t.Fatalf("got %d windows, want 110", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i)*60, 0)) || w.Count != 2 || w.Sum != 2*float64(i) {
			t.Fatalf("window %d: %+v", i, w)
		}
	}
}
