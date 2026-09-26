package rollup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentAddAndRead files samples and reads windows from many
// goroutines at once. Run with -race; the final counts must also add up.
func TestConcurrentAddAndRead(t *testing.T) {
	r := New(10 * time.Second)
	const writers = 8
	const perWriter = 500
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := int64(w * perWriter)
			for i := int64(0); i < perWriter; i++ {
				// Distinct increasing instants per writer; across
				// writers an earlier instant may lose the race and
				// be rejected, which is fine — count what lands.
				err := r.Add(time.Unix(0, base+i), float64(i))
				if err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrOutOfOrder) {
					t.Errorf("add: %v", err)
					return
				}
			}
		}(w)
	}
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = r.Windows()
				_, _ = r.Window(time.Unix(0, 0))
			}
		}()
	}
	wg.Wait()
	var total int64
	for _, w := range r.Windows() {
		total += w.Count
	}
	if total != accepted.Load() {
		t.Fatalf("total count = %d, accepted = %d", total, accepted.Load())
	}
}

// TestWindowsSnapshotIsStable: a slice returned by Windows is a moment-in-
// time snapshot; later writes must not reach into it.
func TestWindowsSnapshotIsStable(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	snap := r.Windows()
	if len(snap) != 1 || snap[0].Count != 1 {
		t.Fatalf("snapshot before: %+v", snap)
	}
	if err := r.Add(time.Unix(0, 0), 100); err != nil {
		t.Fatal(err)
	}
	if snap[0].Count != 1 || snap[0].Sum != 1 {
		t.Fatalf("snapshot mutated by later add: %+v", snap[0])
	}
}

// TestMergeAtomicity hammers a merge while readers watch: every observed
// window must be the pre-merge value or the post-merge value, never a
// half-folded one.
func TestMergeAtomicity(t *testing.T) {
	const windows = 64
	a := New(time.Minute)
	b := New(time.Minute)
	for i := 0; i < windows; i++ {
		at := time.Unix(int64(60*i), 0)
		if err := a.Add(at, 1); err != nil {
			t.Fatal(err)
		}
		if err := b.Add(at, 2); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, w := range a.Windows() {
				// Pre-merge every window has Sum 1; post-merge 3.
				// Anything else is a torn read.
				if w.Sum != 1 && w.Sum != 3 {
					t.Errorf("torn window: %+v", w)
					return
				}
			}
		}
	}()
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	for _, w := range a.Windows() {
		if w.Sum != 3 || w.Count != 2 {
			t.Fatalf("after merge: %+v", w)
		}
	}
}

// TestCrossMerge: two rollers merging each other concurrently must not
// deadlock and must equal some serial order of the two folds.
func TestCrossMerge(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	if err := a.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(time.Unix(0, 0), 10); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := a.Merge(b); err != nil {
			t.Errorf("a.Merge(b): %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := b.Merge(a); err != nil {
			t.Errorf("b.Merge(a): %v", err)
		}
	}()
	wg.Wait()
	// Serial orders: a.Merge(b) then b.Merge(a) gives a={11,2}, b={21,3};
	// b.Merge(a) then a.Merge(b) gives b={11,2}, a={12,3}. Either way the
	// first-folded roller ends at {11, 2} and the second at {11+its own
	// original sum, 3}.
	wa, _ := a.Window(time.Unix(0, 0))
	wb, _ := b.Window(time.Unix(0, 0))
	first := wa
	second := wb
	secondSum := float64(21)
	if wa.Count == 3 {
		first, second = wb, wa
		secondSum = 12
	}
	if first.Count != 2 || first.Sum != 11 {
		t.Fatalf("first-folded roller: %+v", first)
	}
	if second.Count != 3 || second.Sum != secondSum {
		t.Fatalf("second-folded roller: %+v, want sum %v", second, secondSum)
	}
}

// TestConcurrentMergeAndAdd: samples filed while a merge runs land either
// entirely before or entirely after the fold — never half counted.
func TestConcurrentMergeAndAdd(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	if err := b.Add(time.Unix(0, 0), 100); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := int64(0); i < 1000; i++ {
			_ = a.Add(time.Unix(i, 0), 1)
		}
	}()
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	<-done
	// Every sample the writer filed is either in a's own window or was
	// rejected as out of order; b's contribution is exactly 100 in the
	// first window regardless of interleaving.
	w, ok := a.Window(time.Unix(0, 0))
	if !ok {
		t.Fatal("window 0 missing after merge")
	}
	if w.Sum < 100 {
		t.Fatalf("merge contribution lost: %+v", w)
	}
}

// TestMergeMismatchConcurrentLeavesBothUnchanged: a failed merge under
// concurrency changes neither roller.
func TestMergeMismatchConcurrentLeavesBothUnchanged(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Hour)
	if err := a.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(time.Unix(0, 0), 2); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Merge(b); !errors.Is(err, ErrWindowMismatch) {
				t.Errorf("expected ErrWindowMismatch, got %v", err)
			}
		}()
	}
	wg.Wait()
	wa, _ := a.Window(time.Unix(0, 0))
	wb, _ := b.Window(time.Unix(0, 0))
	if wa.Sum != 1 || wb.Sum != 2 {
		t.Fatalf("failed merge mutated state: a=%+v b=%+v", wa, wb)
	}
}
