package rollup

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAddBatchUnorderedAndDuplicates(t *testing.T) {
	r := New(time.Minute)
	base := time.Unix(0, 0).UTC()
	batch := []Sample{
		{At: base.Add(3 * time.Minute), Value: 3},
		{At: base.Add(time.Minute), Value: 1},
		{At: base.Add(2 * time.Minute), Value: 2},
		{At: base.Add(time.Minute), Value: 10}, // same window as the second
		{At: base.Add(time.Minute), Value: 10}, // same instant counts again
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	ws := r.Windows()
	if len(ws) != 3 {
		t.Fatalf("got %d windows, want 3", len(ws))
	}
	w, ok := r.Window(base.Add(time.Minute))
	if !ok {
		t.Fatal("missing window at 1m")
	}
	if w.Count != 3 || w.Sum != 21 || w.Min != 1 || w.Max != 10 {
		t.Fatalf("window at 1m = %+v", w)
	}
	if ws[0].Sum != 1+10+10 || ws[1].Sum != 2 || ws[2].Sum != 3 {
		t.Fatalf("windows = %+v", ws)
	}
}

func TestAddBatchAtomicRejection(t *testing.T) {
	r := New(time.Minute)
	base := time.Unix(0, 0).UTC()
	if err := r.Add(base.Add(5*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	before := r.Windows()
	err := r.AddBatch([]Sample{
		{At: base.Add(6 * time.Minute), Value: 2},
		{At: base.Add(4 * time.Minute), Value: 3}, // predates latest
	})
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err = %v, want ErrOutOfOrder", err)
	}
	after := r.Windows()
	if len(before) != len(after) {
		t.Fatalf("state changed: %v -> %v", before, after)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("state changed at %d: %v -> %v", i, before[i], after[i])
		}
	}
	// A sample at exactly the latest instant is accepted.
	if err := r.AddBatch([]Sample{{At: base.Add(5 * time.Minute), Value: 7}}); err != nil {
		t.Fatalf("AddBatch at latest: %v", err)
	}
	w, _ := r.Window(base.Add(5 * time.Minute))
	if w.Count != 2 || w.Sum != 8 {
		t.Fatalf("window at 5m = %+v", w)
	}
}

func TestAddBatchEmpty(t *testing.T) {
	r := New(time.Minute)
	if err := r.AddBatch(nil); err != nil {
		t.Fatalf("nil batch: %v", err)
	}
	if err := r.AddBatch([]Sample{}); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if ws := r.Windows(); len(ws) != 0 {
		t.Fatalf("empty batch changed state: %v", ws)
	}
	// The latest mark must not move: an empty batch followed by any
	// sample still accepts it.
	if err := r.Add(time.Unix(0, 0).UTC(), 1); err != nil {
		t.Fatalf("Add after empty batches: %v", err)
	}
}

func TestAddBatchLatestTracking(t *testing.T) {
	r := New(time.Minute)
	base := time.Unix(0, 0).UTC()
	if err := r.Add(base.Add(10*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	// Batch max is below latest but no sample predates it.
	if err := r.AddBatch([]Sample{{At: base.Add(10 * time.Minute), Value: 2}}); err != nil {
		t.Fatal(err)
	}
	// latest stays at 10m, so 9m is still rejected.
	if err := r.Add(base.Add(9*time.Minute), 3); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err = %v, want ErrOutOfOrder", err)
	}
	// A batch can advance the mark to its own maximum.
	if err := r.AddBatch([]Sample{
		{At: base.Add(12 * time.Minute), Value: 1},
		{At: base.Add(15 * time.Minute), Value: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(base.Add(14*time.Minute), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err = %v, want ErrOutOfOrder", err)
	}
}

func TestAddBatchExtremeTimes(t *testing.T) {
	r := New(time.Hour)
	far := time.Unix(1<<62, 0).UTC()
	if err := r.AddBatch([]Sample{
		{At: far.Add(90 * time.Minute), Value: 1},
		{At: far, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	ws := r.Windows()
	if len(ws) != 2 {
		t.Fatalf("got %d windows, want 2", len(ws))
	}
	// far is not hour-aligned; its window starts at the hour below it.
	if want := far.Truncate(time.Hour); !ws[0].Start.Equal(want) || ws[0].Sum != 2 {
		t.Fatalf("first window = %+v, want start %v sum 2", ws[0], want)
	}
	if want := far.Add(90 * time.Minute).Truncate(time.Hour); !ws[1].Start.Equal(want) || ws[1].Sum != 1 {
		t.Fatalf("second window = %+v, want start %v sum 1", ws[1], want)
	}
}

func TestAddBatchMergeAndBackfill(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	a := New(time.Minute)
	b := New(time.Minute)
	if err := a.Add(base.Add(10*time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.AddBatch([]Sample{
		{At: base, Value: 5},
		{At: base.Add(20 * time.Minute), Value: 6},
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	// Merge does not move the out-of-order mark: a sample at or after
	// latest is still accepted even though it lands before the newest
	// window the merge brought in.
	if err := a.AddBatch([]Sample{{At: base.Add(15 * time.Minute), Value: 7}}); err != nil {
		t.Fatal(err)
	}
	w, ok := a.Window(base)
	if !ok || w.Sum != 5 {
		t.Fatalf("window at 0 = %+v, %v", w, ok)
	}
	if w, ok = a.Window(base.Add(15 * time.Minute)); !ok || w.Sum != 7 {
		t.Fatalf("window at 15m = %+v, %v", w, ok)
	}
	if ws := a.Windows(); len(ws) != 4 {
		t.Fatalf("windows = %v", ws)
	}
}

func TestAddBatchCountSaturation(t *testing.T) {
	r := New(time.Minute)
	base := time.Unix(0, 0).UTC()
	if err := r.AddBatch([]Sample{{At: base, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.buckets[0].Count = math.MaxInt64
	r.mu.Unlock()
	if err := r.AddBatch([]Sample{{At: base, Value: 2}, {At: base, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	w, _ := r.Window(base)
	if w.Count != math.MaxInt64 {
		t.Fatalf("Count = %d, want saturated", w.Count)
	}
	if w.Sum != 6 || w.Min != 1 || w.Max != 3 {
		t.Fatalf("window = %+v", w)
	}
}

func TestAddBatchConcurrent(t *testing.T) {
	r := New(time.Second)
	base := time.Unix(0, 0).UTC()
	// Goroutines race with overlapping timestamps, so some batches lose
	// the out-of-order race and are rejected; that is expected. What must
	// hold is atomicity: every accepted batch contributes exactly two
	// samples to exactly one window, so every window count stays even.
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				at := base.Add(time.Duration(g*200+i) * time.Second)
				err := r.AddBatch([]Sample{
					{At: at, Value: 1},
					{At: at, Value: 1},
				})
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrOutOfOrder) {
					t.Errorf("AddBatch: %v", err)
					return
				}
				_ = r.Windows()
				_ = r.Range(base, at.Add(time.Second))
				c := r.Cursor(base, at.Add(time.Second))
				_ = c.Next(3)
			}
		}(g)
	}
	wg.Wait()
	var total int64
	for _, w := range r.Windows() {
		if w.Count%2 != 0 {
			t.Fatalf("torn batch: window %+v has odd count", w)
		}
		total += w.Count
	}
	if total != accepted.Load()*2 {
		t.Fatalf("total count = %d, want %d", total, accepted.Load()*2)
	}
}

func BenchmarkAddBatch(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			base := time.Unix(0, 0).UTC()
			batch := make([]Sample, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := New(time.Second)
				for j := range batch {
					batch[j] = Sample{At: base.Add(time.Duration(j) * time.Second), Value: float64(j)}
				}
				if err := r.AddBatch(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
