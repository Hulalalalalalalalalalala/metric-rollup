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

// TestAddBatchEmpty checks that nil and empty batches are accepted, change
// nothing, and do not advance the most-recent marker.
func TestAddBatchEmpty(t *testing.T) {
	r := New(time.Second)
	if err := r.AddBatch(nil); err != nil {
		t.Fatal(err)
	}
	if err := r.AddBatch([]Sample{}); err != nil {
		t.Fatal(err)
	}
	if got := r.Windows(); len(got) != 0 {
		t.Fatalf("empty batch changed state: %v", got)
	}
	// The marker must not have moved: a first real sample at any instant
	// is accepted, and only then does ordering apply.
	if err := r.Add(time.Unix(10, 0), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.AddBatch(nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(10, 0), 2); err != nil {
		t.Fatalf("empty batch advanced the marker: %v", err)
	}
	w, _ := r.Window(time.Unix(10, 0))
	if w.Count != 2 || w.Sum != 3 {
		t.Fatalf("window: %+v", w)
	}
}

// TestAddBatchUnordered files a shuffled batch with repeated instants and
// checks every sample is counted exactly once, in the right window.
func TestAddBatchUnordered(t *testing.T) {
	r := New(10 * time.Second)
	batch := []Sample{
		{At: time.Unix(25, 0), Value: 5},
		{At: time.Unix(5, 0), Value: 1},
		{At: time.Unix(15, 0), Value: 3},
		{At: time.Unix(5, 0), Value: 2}, // same instant, counted again
		{At: time.Unix(9, 0), Value: 4},
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	w, ok := r.Window(time.Unix(0, 0))
	if !ok || w.Count != 3 || w.Sum != 7 || w.Min != 1 || w.Max != 4 {
		t.Fatalf("window 0: %+v ok=%v", w, ok)
	}
	w, ok = r.Window(time.Unix(10, 0))
	if !ok || w.Count != 1 || w.Sum != 3 {
		t.Fatalf("window 10: %+v ok=%v", w, ok)
	}
	w, ok = r.Window(time.Unix(20, 0))
	if !ok || w.Count != 1 || w.Sum != 5 {
		t.Fatalf("window 20: %+v ok=%v", w, ok)
	}
	got := r.Windows()
	for i := 1; i < len(got); i++ {
		if !got[i-1].Start.Before(got[i].Start) {
			t.Fatalf("windows out of order: %v", got)
		}
	}
}

// TestAddBatchRejectsOutOfOrder checks that one stale sample rejects the
// whole batch and leaves the roller byte-for-byte unchanged.
func TestAddBatchRejectsOutOfOrder(t *testing.T) {
	r := New(10 * time.Second)
	if err := r.Add(time.Unix(100, 0), 1); err != nil {
		t.Fatal(err)
	}
	before := r.Windows()
	batch := []Sample{
		{At: time.Unix(200, 0), Value: 2},
		{At: time.Unix(99, 0), Value: 3}, // predates the accepted marker
		{At: time.Unix(300, 0), Value: 4},
	}
	if err := r.AddBatch(batch); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("want ErrOutOfOrder, got %v", err)
	}
	after := r.Windows()
	if len(after) != len(before) {
		t.Fatalf("rejected batch changed state: %v -> %v", before, after)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("rejected batch changed window %d: %+v -> %+v", i, before[i], after[i])
		}
	}
	// The marker did not move either: the boundary instant is still
	// accepted on its own.
	if err := r.Add(time.Unix(100, 0), 9); err != nil {
		t.Fatalf("marker moved after rejection: %v", err)
	}
}

// TestAddBatchMarker checks that samples within a batch are not ordered
// against each other, and that the marker afterwards is the newest
// instant in the batch.
func TestAddBatchMarker(t *testing.T) {
	r := New(10 * time.Second)
	// Strictly decreasing inside the batch: still accepted.
	batch := []Sample{
		{At: time.Unix(300, 0), Value: 1},
		{At: time.Unix(200, 0), Value: 2},
		{At: time.Unix(100, 0), Value: 3},
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(299, 0), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("marker should be the batch maximum: %v", err)
	}
	if err := r.Add(time.Unix(300, 0), 1); err != nil {
		t.Fatalf("sample at the marker should be accepted: %v", err)
	}
	// A batch whose newest instant is older than the current marker is
	// rejected even though the marker came from a merge-free source.
	if err := r.AddBatch([]Sample{{At: time.Unix(250, 0), Value: 1}}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("want ErrOutOfOrder, got %v", err)
	}
}

// TestAddBatchAfterMerge checks that a batch can backfill windows earlier
// than merged-in windows, and that merging does not count as out-of-order.
func TestAddBatchAfterMerge(t *testing.T) {
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
	// Backfill the gap in one batch, newest first.
	batch := make([]Sample, 0, 99)
	for i := 99; i >= 1; i-- {
		batch = append(batch, Sample{At: time.Unix(int64(i)*60, 0), Value: float64(i)})
	}
	if err := a.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	got := a.Windows()
	if len(got) != 110 {
		t.Fatalf("got %d windows, want 110", len(got))
	}
	for i, w := range got {
		if !w.Start.Equal(time.Unix(int64(i)*60, 0)) {
			t.Fatalf("window %d starts at %v", i, w.Start)
		}
		want := float64(i)
		if i == 0 {
			want = 1 // the pre-merge sample
		}
		if w.Count != 1 || w.Sum != want {
			t.Fatalf("window %d: %+v", i, w)
		}
	}
}

// TestAddBatchExtremeTimestamps checks that extreme instants in a batch
// still land in epoch-aligned windows, with no overflow into a wrong one.
func TestAddBatchExtremeTimestamps(t *testing.T) {
	const window = 10 * time.Second
	ats := []time.Time{
		time.Unix(1<<62, 123456789),
		time.Unix(0, 0),
		time.Unix(-1<<62, 987654321),
		time.Unix(0, math.MaxInt64),
		time.Unix(0, math.MinInt64),
	}
	// Submit in an order that keeps every sample at or after the running
	// marker: sort by time first.
	for i := 0; i < len(ats); i++ {
		for j := i + 1; j < len(ats); j++ {
			if ats[j].Before(ats[i]) {
				ats[i], ats[j] = ats[j], ats[i]
			}
		}
	}
	batch := make([]Sample, len(ats))
	for i, at := range ats {
		batch[i] = Sample{At: at, Value: 5}
	}
	r := New(window)
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	for _, at := range ats {
		want := wantStart(at, window)
		w, ok := r.Window(want)
		if !ok {
			t.Fatalf("AddBatch(%v): no window at aligned start %v", at, want)
		}
		if w.Count != 1 || w.Sum != 5 || w.Min != 5 || w.Max != 5 {
			t.Fatalf("AddBatch(%v): window %+v", at, w)
		}
	}
}

// TestAddBatchMinWindowSize checks batch submission with the smallest
// allowed window: boundary ownership matches single-sample submission.
func TestAddBatchMinWindowSize(t *testing.T) {
	r := New(1)
	batch := []Sample{
		{At: time.Unix(0, 2), Value: 2},
		{At: time.Unix(0, 0), Value: 0},
		{At: time.Unix(0, 1), Value: 1},
		{At: time.Unix(0, 0), Value: 0},
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	got := r.Windows()
	if len(got) != 3 {
		t.Fatalf("want 3 windows, got %d", len(got))
	}
	w, ok := r.Window(time.Unix(0, 0))
	if !ok || w.Count != 2 || w.Sum != 0 {
		t.Fatalf("window 0: %+v ok=%v", w, ok)
	}
}

// TestAddBatchCountSaturates checks that a batch into a saturated window
// keeps updating sum, min, and max without overflowing the count.
func TestAddBatchCountSaturates(t *testing.T) {
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
	if err := r.AddBatch([]Sample{
		{At: time.Unix(0, 0), Value: 20},
		{At: time.Unix(0, 0), Value: -5},
	}); err != nil {
		t.Fatal(err)
	}
	w, _ := r.Window(time.Unix(0, 0))
	if w.Count != math.MaxInt64 {
		t.Fatalf("count overflowed: %d", w.Count)
	}
	if w.Sum != 25 || w.Min != -5 || w.Max != 20 {
		t.Fatalf("stats did not keep updating: %+v", w)
	}
}

// TestAddBatchCursorStable checks that a cursor created before a batch
// still reads the snapshot from its creation, and that the batch is
// visible whole to snapshots taken after it.
func TestAddBatchCursorStable(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	c := r.Cursor(time.Unix(0, 0), time.Unix(600, 0))
	batch := make([]Sample, 0, 10)
	for i := 1; i <= 10; i++ {
		batch = append(batch, Sample{At: time.Unix(int64(i)*60, 0), Value: float64(i)})
	}
	if err := r.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	got := c.Next(100)
	if len(got) != 1 || got[0].Count != 1 {
		t.Fatalf("cursor saw the batch filed after its creation: %v", got)
	}
	if got := c.Next(100); len(got) != 0 {
		t.Fatalf("cursor did not terminate: %v", got)
	}
	if got := r.Windows(); len(got) != 11 {
		t.Fatalf("batch not visible whole afterwards: %d windows", len(got))
	}
}

// TestConcurrentAddBatch files batches from many goroutines while others
// read snapshots and advance cursors; every accepted batch must be
// accounted for exactly once, and no snapshot may show a partial batch.
// Run with -race to catch data races.
func TestConcurrentAddBatch(t *testing.T) {
	r := New(2500 * time.Millisecond)
	const writers = 8
	const perWriter = 100
	const batchSize = 4

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
				for _, w := range r.Windows() {
					if w.Count <= 0 || w.Min > w.Max {
						t.Errorf("inconsistent snapshot: %+v", w)
					}
				}
				c := r.Cursor(time.Unix(0, 0), time.Unix(1<<40, 0))
				for got := c.Next(7); len(got) != 0; got = c.Next(7) {
					for _, w := range got {
						if w.Count <= 0 || w.Min > w.Max {
							t.Errorf("inconsistent cursor window: %+v", w)
						}
					}
				}
			}
		}()
	}

	// Each writer submits batches of batchSize samples at one instant per
	// batch; every sample in an accepted batch lands in the same window.
	var accepted atomic.Int64
	var writersWg sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			for i := 0; i < perWriter; i++ {
				at := time.Unix(0, int64(i)*int64(2500*time.Millisecond)+int64(w))
				batch := make([]Sample, batchSize)
				for k := range batch {
					batch[k] = Sample{At: at, Value: float64(w)}
				}
				err := r.AddBatch(batch)
				switch {
				case err == nil:
					accepted.Add(batchSize)
				case errors.Is(err, ErrOutOfOrder):
					// Lost the race to a later batch; fine.
				default:
					t.Errorf("AddBatch: %v", err)
				}
			}
		}(w)
	}
	writersWg.Wait()
	close(stop)
	readers.Wait()

	var total int64
	for _, w := range r.Windows() {
		total += w.Count
	}
	if total != accepted.Load() {
		t.Fatalf("total count = %d, want %d accepted samples", total, accepted.Load())
	}
}

// BenchmarkAddBatch measures sustained batch submission; each batch opens
// new windows, and the cost should scale with the batch size.
func BenchmarkAddBatch(b *testing.B) {
	for _, size := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			r := New(time.Second)
			base := time.Unix(0, 0)
			batch := make([]Sample, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := base.Add(time.Duration(i*size) * time.Second)
				for k := range batch {
					batch[k] = Sample{At: start.Add(time.Duration(k) * time.Second), Value: float64(k)}
				}
				if err := r.AddBatch(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
