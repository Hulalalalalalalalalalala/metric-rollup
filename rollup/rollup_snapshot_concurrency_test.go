package rollup

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentMergeSourceBatchAtomicity pins the rule that a batch
// filed into the merge's source while Merge runs is either folded in
// completely or not at all: the destination can never show half a batch.
func TestConcurrentMergeSourceBatchAtomicity(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		src := New(time.Minute)
		dst := New(time.Minute)
		const before = 32
		for i := 0; i < before; i++ {
			if err := src.Add(time.Unix(int64(i)*60, 0), 1); err != nil {
				t.Fatal(err)
			}
		}
		// Windows newer than anything already in src, so the batch is
		// never rejected by ordering.
		batch := make([]Sample, 0, before)
		for i := before; i < 2*before; i++ {
			batch = append(batch, Sample{At: time.Unix(int64(i)*60, 0), Value: 2})
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.AddBatch(batch); err != nil {
				t.Errorf("AddBatch: %v", err)
			}
		}()
		if err := dst.Merge(src); err != nil {
			t.Fatal(err)
		}
		wg.Wait()

		got := dst.Windows()
		// The merge observed src either entirely before the batch
		// (before windows) or entirely after it (2*before windows);
		// anything in between is a torn merge.
		switch len(got) {
		case before, 2 * before:
		default:
			t.Fatalf("trial %d: destination shows %d windows, want %d or %d",
				trial, len(got), before, 2*before)
		}
		for i, w := range got {
			wantVal := float64(1)
			if i >= before {
				wantVal = 2
			}
			if w.Count != 1 || w.Sum != wantVal || w.Min != wantVal || w.Max != wantVal {
				t.Fatalf("trial %d: torn window %d stats %+v, want value %v",
					trial, i, w, wantVal)
			}
		}
		// The source was never modified by being merged from.
		if got := len(src.Windows()); got != 2*before {
			t.Fatalf("trial %d: source has %d windows, want %d", trial, got, 2*before)
		}
	}
}

// TestSnapshotReadsStableUnderWriters takes full, ranged, and cursor
// snapshots and then files new windows, backfills, batches, and merges
// while reading them: every returned window set stays the one frozen at
// snapshot time, window for window, with complete statistics.
func TestSnapshotReadsStableUnderWriters(t *testing.T) {
	r := New(time.Minute)
	for i := 0; i < 200; i++ {
		if err := r.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	full := r.Windows()
	ranged := r.Range(time.Unix(20*60, 0), time.Unix(120*60, 0))
	cursor := r.Cursor(time.Unix(20*60, 0), time.Unix(120*60, 0))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Newest-first samples land partly as backfills once a
				// far-ahead window is merged in.
				switch i % 4 {
				case 0:
					_ = r.Add(time.Unix(int64(10000+i)*60, 0), float64(w))
				case 1:
					_ = r.AddBatch([]Sample{
						{At: time.Unix(int64(20000+i)*60, 0), Value: 3},
						{At: time.Unix(int64(20000+i)*60+1, 0), Value: -3},
					})
				case 2:
					other := New(time.Minute)
					_ = other.Add(time.Unix(int64(30000+i)*60, 0), 7)
					_ = r.Merge(other)
				case 3:
					_ = r.Add(time.Unix(int64(150+i)*60, 0), float64(w))
				}
				i++
			}
		}(w)
	}

	// Readers keep verifying the frozen snapshots while writes run.
	for k := 0; k < 300; k++ {
		got := full
		if len(got) != 200 {
			t.Fatalf("full snapshot changed length: %d", len(got))
		}
		for i, w := range got {
			if !w.Start.Equal(time.Unix(int64(i)*60, 0)) || w.Count != 1 || w.Sum != float64(i) {
				t.Fatalf("full snapshot window %d changed: %+v", i, w)
			}
		}
		got = ranged
		if len(got) != 100 {
			t.Fatalf("range snapshot changed length: %d", len(got))
		}
		for j, w := range got {
			i := 20 + j
			if !w.Start.Equal(time.Unix(int64(i)*60, 0)) || w.Count != 1 || w.Sum != float64(i) {
				t.Fatalf("range snapshot window %d changed: %+v", j, w)
			}
		}
	}
	close(stop)
	wg.Wait()

	// The cursor ends deterministically: concurrent Next calls hand each
	// frozen window out exactly once, then only empty batches follow.
	var mu sync.Mutex
	var seen []Window
	var drain sync.WaitGroup
	for g := 0; g < 4; g++ {
		drain.Add(1)
		go func(g int) {
			defer drain.Done()
			for {
				n := []int{1, 7, 64, 1000}[g]
				batch := cursor.Next(n)
				if len(batch) == 0 {
					return
				}
				mu.Lock()
				seen = append(seen, batch...)
				mu.Unlock()
			}
		}(g)
	}
	drain.Wait()
	if len(seen) != len(ranged) {
		t.Fatalf("cursor handed out %d windows, want %d", len(seen), len(ranged))
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i].Start.Before(seen[j].Start) })
	for i := range seen {
		if seen[i] != ranged[i] {
			t.Fatalf("cursor window %d: %+v want %+v", i, seen[i], ranged[i])
		}
	}
	for i := 0; i < 10; i++ {
		if got := cursor.Next(1 + i); len(got) != 0 {
			t.Fatalf("exhausted cursor moved: %+v", got)
		}
	}
}

// TestViewReadsSelfConsistentUnderWriters derives a view, then pounds the
// roller with writes while every view read path runs: point lookups,
// ranges, and cursors must agree with one another and with the frozen
// coarse statistics; later writes never enter the view.
func TestViewReadsSelfConsistentUnderWriters(t *testing.T) {
	r := New(time.Minute)
	for i := 0; i < 600; i++ {
		if err := r.Add(time.Unix(int64(i)*60, 0), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	v := r.Rollup(5) // 120 coarse windows of five fine ones

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch i % 3 {
				case 0:
					_ = r.AddBatch([]Sample{
						{At: time.Unix(int64(5000+i)*60, 0), Value: 1},
						{At: time.Unix(int64(5000+i)*60+1, 0), Value: 2},
					})
				case 1:
					other := New(time.Minute)
					_ = other.Add(time.Unix(int64(9000+i)*60, 0), 9)
					_ = r.Merge(other)
				case 2:
					_ = r.Add(time.Unix(int64(600+i)*60, 0), float64(w))
				}
				i++
			}
		}(w)
	}

	want := v.Range(time.Unix(math.MinInt64/4, 0), time.Unix(math.MaxInt64/4, 0))
	if len(want) != 120 {
		t.Fatalf("view has %d coarse windows, want 120", len(want))
	}
	for k := 0; k < 200; k++ {
		// Every range read equals the frozen one exactly.
		got := v.Range(time.Unix(-100000*60, 0), time.Unix(100000*60, 0))
		if len(got) != len(want) {
			t.Fatalf("view range changed: %d windows, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("view range window %d changed: %+v want %+v", i, got[i], want[i])
			}
		}
		// Point lookups agree with the range entries, including the
		// coarse boundaries just outside them.
		for i := -1; i <= len(want); i++ {
			at := time.Unix(int64(i*5)*60, 0)
			pt := v.Window(at)
			switch {
			case i < 0 || i == len(want):
				if pt != (Window{}) {
					t.Fatalf("view lookup outside range non-zero at %d: %+v", i, pt)
				}
			default:
				if pt != want[i] {
					t.Fatalf("view lookup %d: %+v want %+v", i, pt, want[i])
				}
			}
		}
		// A cursor covers the frozen coarse range exactly once.
		c := v.Cursor(time.Unix(-100000*60, 0), time.Unix(100000*60, 0))
		n := 0
		for batch := c.Next(13); len(batch) != 0; batch = c.Next(13) {
			for _, w := range batch {
				if w != want[n] {
					t.Fatalf("view cursor window %d: %+v want %+v", n, w, want[n])
				}
				n++
			}
		}
		if n != len(want) {
			t.Fatalf("view cursor covered %d windows, want %d", n, len(want))
		}
		if tail := c.Next(1); len(tail) != 0 {
			t.Fatalf("view cursor did not end: %+v", tail)
		}
	}
	close(stop)
	wg.Wait()

	// After the writers stop the view is still the same frozen snapshot,
	// while a freshly derived view sees the later writes.
	if got := v.Range(time.Unix(-100000*60, 0), time.Unix(100000*60, 0)); len(got) != 120 {
		t.Fatalf("view changed after writers stopped: %d windows", len(got))
	}
	if fresh := r.Rollup(5); len(fresh.Range(time.Unix(-100000*60, 0), time.Unix(100000*60, 0))) <= 120 {
		t.Fatalf("fresh view should observe later writes: %d windows",
			len(fresh.Range(time.Unix(-100000*60, 0), time.Unix(100000*60, 0))))
	}
}

// TestBackfilledOutputDeterministic checks that folding a backfill map
// into the sorted slice gives the same ordered result on every run and
// through every read path, despite unordered map iteration underneath.
func TestBackfilledOutputDeterministic(t *testing.T) {
	build := func() *Roller {
		r := New(time.Minute)
		if err := r.Add(time.Unix(0, 0), 1); err != nil {
			t.Fatal(err)
		}
		ahead := New(time.Minute)
		if err := ahead.Add(time.Unix(200*60, 0), 1); err != nil {
			t.Fatal(err)
		}
		if err := r.Merge(ahead); err != nil {
			t.Fatal(err)
		}
		// Deliberately non-monotone within one batch (allowed: samples
		// are not checked against each other), leaving the gap windows in
		// the backfill map while bucket 200 sits at the sorted tail.
		if err := r.AddBatch([]Sample{
			{At: time.Unix(5*60, 0), Value: 5},
			{At: time.Unix(2*60, 0), Value: 2},
			{At: time.Unix(9*60, 0), Value: 9},
			{At: time.Unix(7*60, 0), Value: 7},
			{At: time.Unix(1*60, 0), Value: 1},
		}); err != nil {
			t.Fatal(err)
		}
		return r
	}

	var first []Window
	for rep := 0; rep < 10; rep++ {
		r := build()
		got := r.Windows()
		if rep == 0 {
			first = got
			if len(first) != 7 {
				t.Fatalf("got %d windows, want 7", len(first))
			}
			wantMin := []int64{0, 1, 2, 5, 7, 9, 200}
			for i, w := range got {
				if !w.Start.Equal(time.Unix(wantMin[i]*60, 0)) {
					t.Fatalf("rep %d: window %d starts at %v, want %dm", rep, i, w.Start, wantMin[i])
				}
			}
			continue
		}
		if len(got) != len(first) {
			t.Fatalf("rep %d: %d windows, want %d", rep, len(got), len(first))
		}
		for i := range got {
			if got[i] != first[i] {
				t.Fatalf("rep %d window %d: %+v want %+v", rep, i, got[i], first[i])
			}
		}
		// The ranged path and a derived view must be equally ordered and
		// stable, and exact lookups must find the backfilled windows.
		rng := r.Range(time.Unix(0, 0), time.Unix(10*60, 0))
		if len(rng) != 6 {
			t.Fatalf("rep %d: range got %d windows, want 6", rep, len(rng))
		}
		for i, w := range rng {
			if !w.Start.Equal(got[i].Start) {
				t.Fatalf("rep %d: range order mismatch at %d", rep, i)
			}
		}
		for _, m := range []int64{1, 2, 5, 7, 9} {
			w, ok := r.Window(time.Unix(m*60, 0))
			if !ok || w.Count != 1 || w.Sum != float64(m) {
				t.Fatalf("rep %d: lookup at %dm: %+v ok=%v", rep, m, w, ok)
			}
		}
		v := r.Rollup(2)
		vw := v.Range(time.Unix(math.MinInt64/4, 0), time.Unix(math.MaxInt64/4, 0))
		for i := 1; i < len(vw); i++ {
			if !vw[i-1].Start.Before(vw[i].Start) {
				t.Fatalf("rep %d: coarse windows out of order", rep)
			}
		}
	}
}

// TestConcurrentAddMergeQueryMillion runs the full mixed workload at the
// million scale: concurrent submission, folding merges racing the adds,
// and continuous snapshot/query traffic. Every sample is accounted for
// exactly once, no window ever tears, and the window set stays ordered.
// Run with -race to catch data races.
func TestConcurrentAddMergeQueryMillion(t *testing.T) {
	if testing.Short() {
		t.Skip("million-sample concurrency run")
	}
	r := New(time.Second)
	const shards = 8
	const rounds = 15625
	const batchSize = 8 // 8 * 15625 * 8 = 1,000,000 samples

	// Every round, all shards file one batch at the same round instant.
	// Per-round channels synchronize the shards: round r+1 is not
	// released until every shard finished round r, and equal instants are
	// always accepted, so all million samples land in round order.
	starts := make([]chan struct{}, rounds)
	for i := range starts {
		starts[i] = make(chan struct{})
	}
	var roundDone sync.WaitGroup
	var accepted atomic.Int64
	var mergedWindows atomic.Int64

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
				snap := r.Windows()
				for j := 1; j < len(snap); j++ {
					if !snap[j-1].Start.Before(snap[j].Start) {
						t.Errorf("window set out of order: %v then %v", snap[j-1].Start, snap[j].Start)
						return
					}
				}
				for _, w := range snap {
					if w.Count <= 0 || w.Min > w.Max {
						t.Errorf("torn window: %+v", w)
						return
					}
				}
				// A cursor over a snapshot moment must hand its windows
				// out exactly once and then end.
				c := r.Cursor(time.Unix(0, 0), time.Unix(1<<40, 0))
				for batch := c.Next(4096); len(batch) != 0; batch = c.Next(4096) {
					for _, w := range batch {
						if w.Count <= 0 || w.Min > w.Max {
							t.Errorf("torn cursor window: %+v", w)
							return
						}
					}
				}
				if tail := c.Next(8); len(tail) != 0 {
					t.Errorf("exhausted cursor moved: %+v", tail)
					return
				}
			}
		}()
	}

	var writers sync.WaitGroup
	for s := 0; s < shards; s++ {
		writers.Add(1)
		go func(s int) {
			defer writers.Done()
			for round := 0; round < rounds; round++ {
				<-starts[round]
				at := time.Unix(int64(round), 0)
				batch := make([]Sample, batchSize)
				for k := range batch {
					batch[k] = Sample{At: at, Value: float64(s)}
				}
				if err := r.AddBatch(batch); err != nil {
					t.Errorf("round %d AddBatch: %v", round, err)
					roundDone.Done()
					continue
				}
				accepted.Add(batchSize)
				// One shard per round folds a window far ahead of the
				// stream: the merge races the other writers, and from
				// then on new round windows arrive through the backfill
				// map instead of the append path.
				if s == 0 && round&511 == 0 {
					other := New(time.Second)
					if err := other.Add(time.Unix(int64(1<<30+round), 0), 1); err != nil {
						t.Errorf("other Add: %v", err)
					} else if err := r.Merge(other); err != nil {
						t.Errorf("Merge: %v", err)
					} else {
						mergedWindows.Add(1)
					}
				}
				roundDone.Done()
			}
		}(s)
	}
	for round := 0; round < rounds; round++ {
		roundDone.Add(shards)
		close(starts[round])
		roundDone.Wait()
	}
	writers.Wait()
	close(stop)
	readers.Wait()

	got := r.Windows()
	var total int64
	for _, w := range got {
		total += w.Count
	}
	if want := accepted.Load() + mergedWindows.Load(); total != want {
		t.Fatalf("total count %d, want %d accepted plus %d merged",
			total, accepted.Load(), mergedWindows.Load())
	}
	for i := 1; i < len(got); i++ {
		if !got[i-1].Start.Before(got[i].Start) {
			t.Fatalf("final windows out of order at %d: %v then %v",
				i, got[i-1].Start, got[i].Start)
		}
	}
}

// TestMergesAtomicUnderConcurrentReads extends the basic atomicity test
// with ranged and cursor reads while merges land in a tight loop: each
// visible window set must be one of the serial states, never a partial one.
func TestMergesAtomicUnderConcurrentReads(t *testing.T) {
	dst := New(time.Minute)
	src := New(time.Minute)
	const n = 64
	for i := 0; i < n; i++ {
		if err := src.Add(time.Unix(int64(i)*60, 0), 1); err != nil {
			t.Fatal(err)
		}
	}
	// Merge repeatedly; each merge after the first doubles every
	// overlapping window's count, so any read must show k*n uniform
	// windows for some k >= 0.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got := dst.Range(time.Unix(0, 0), time.Unix(n*60, 0))
				if len(got) == 0 {
					continue
				}
				if len(got) != n {
					t.Errorf("partial merge visible: %d of %d windows", len(got), n)
					return
				}
				count := got[0].Count
				for _, w := range got {
					if w.Count != count || w.Min != 1 || w.Max != 1 {
						t.Errorf("non-uniform window mid-merge: %+v", w)
						return
					}
				}
			}
		}()
	}
	for k := 0; k < 20; k++ {
		if err := dst.Merge(src); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
	got := dst.Windows()
	if len(got) != n {
		t.Fatalf("after merges: %d windows, want %d", len(got), n)
	}
	// dst started empty and the source is never modified by being merged
	// from, so each of the 20 merges adds its original count once: 20.
	const want = 20
	for _, w := range got {
		if w.Count != want {
			t.Fatalf("count %d, want %d (the original plus 20 whole merges)", w.Count, want)
		}
	}
}
