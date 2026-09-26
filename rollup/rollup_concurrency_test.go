package rollup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentAddAndRead files samples from many goroutines while others
// read windows and snapshots; the final statistics must account for every
// accepted sample exactly once. Run with -race to catch data races.
func TestConcurrentAddAndRead(t *testing.T) {
	r := New(2500 * time.Millisecond) // non-integer-second window
	const writers = 8
	const perWriter = 200

	// Readers run for the whole duration; every snapshot they see must be
	// internally consistent (Min <= Max, Count > 0 in every bucket).
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
				r.Window(time.Unix(0, 0))
			}
		}()
	}

	// Each writer files perWriter samples at ever-increasing instants.
	// Interleaved across writers, some legitimately lose the out-of-order
	// race; count how many were accepted and expect exactly that many in
	// the final statistics.
	var accepted atomic.Int64
	var writersWg sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			for i := 0; i < perWriter; i++ {
				at := time.Unix(0, int64(i)*int64(2500*time.Millisecond)+int64(w))
				err := r.Add(at, float64(w))
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

// TestSnapshotIsStable verifies that a slice returned by Windows is not
// affected by samples filed afterwards.
func TestSnapshotIsStable(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	snap := r.Windows()
	if len(snap) != 1 || snap[0].Count != 1 {
		t.Fatalf("snapshot before: %v", snap)
	}
	if err := r.Add(time.Unix(0, 0), 100); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(60, 0), 5); err != nil {
		t.Fatal(err)
	}
	if len(snap) != 1 || snap[0].Count != 1 || snap[0].Sum != 1 {
		t.Fatalf("snapshot changed after later writes: %v", snap)
	}
}

// TestConcurrentMutualMerge merges two rollers into each other at the same
// time; the outcome must match one of the two serial orders.
func TestConcurrentMutualMerge(t *testing.T) {
	for trial := 0; trial < 50; trial++ {
		a := New(time.Minute)
		b := New(time.Minute)
		if err := a.Add(time.Unix(0, 0), 1); err != nil {
			t.Fatal(err)
		}
		if err := b.Add(time.Unix(0, 0), 1); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a.Merge(b) }()
		go func() { defer wg.Done(); b.Merge(a) }()
		wg.Wait()

		wa, _ := a.Window(time.Unix(0, 0))
		wb, _ := b.Window(time.Unix(0, 0))
		// Serial orders: a-then-b gives (2, 3); b-then-a gives (3, 2).
		ab := wa.Count == 2 && wb.Count == 3
		ba := wa.Count == 3 && wb.Count == 2
		if !ab && !ba {
			t.Fatalf("trial %d: counts (%d, %d) match no serial order", trial, wa.Count, wb.Count)
		}
		if wa.Sum != float64(wa.Count) || wb.Sum != float64(wb.Count) {
			t.Fatalf("trial %d: sums (%v, %v) inconsistent with counts", trial, wa.Sum, wb.Sum)
		}
	}
}

// TestConcurrentMergeAtomicity merges into a roller while a reader pulls
// snapshots: every snapshot must show either all of the merge or none.
func TestConcurrentMergeAtomicity(t *testing.T) {
	dst := New(time.Minute)
	src := New(time.Minute)
	const windows = 64
	for i := 0; i < windows; i++ {
		at := time.Unix(int64(i)*60, 0)
		if err := src.Add(at, 1); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			got := dst.Windows()
			if len(got) != 0 && len(got) != windows {
				t.Errorf("partial merge visible: %d windows", len(got))
			}
		}
	}()
	if err := dst.Merge(src); err != nil {
		t.Fatal(err)
	}
	close(done)
	wg.Wait()
	if got := len(dst.Windows()); got != windows {
		t.Fatalf("after merge: %d windows, want %d", got, windows)
	}
	// src is unchanged by being merged from.
	if got := len(src.Windows()); got != windows {
		t.Fatalf("source changed by merge: %d windows", got)
	}
}

// TestConcurrentMergeMismatchLeavesState hammers a failing merge alongside
// adds and confirms the receiver keeps only its own samples.
func TestConcurrentMergeMismatchLeavesState(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Same instant on every goroutine: always accepted.
			if err := a.Add(time.Unix(0, 0), 1); err != nil {
				t.Errorf("Add: %v", err)
			}
			if err := a.Merge(b); !errors.Is(err, ErrWindowMismatch) {
				t.Errorf("Merge: %v", err)
			}
		}()
	}
	wg.Wait()
	var total int64
	for _, w := range a.Windows() {
		total += w.Count
	}
	if total != 4 {
		t.Fatalf("failed merges altered receiver: total count %d", total)
	}
}
