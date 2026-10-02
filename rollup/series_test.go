package rollup

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func ls(ns int64, v float64, labels map[string]string) LabeledSample {
	return LabeledSample{At: at(ns), Value: v, Labels: labels}
}

// sec scales whole seconds to nanosecond timestamps.
func sec(n int64) int64 { return n * 1_000_000_000 }

func TestNewSeriesSetRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name   string
		window time.Duration
		cfg    SeriesConfig
		want   string
	}{
		{"zero max", time.Second, SeriesConfig{0, OverflowAggregate}, "rollup: bad series config"},
		{"negative max", time.Second, SeriesConfig{-2, OverflowReject}, "rollup: bad series config"},
		{"bad policy", time.Second, SeriesConfig{1, OverflowPolicy(9)}, "rollup: bad series config"},
		{"zero window", 0, SeriesConfig{1, OverflowAggregate}, "rollup: bad window"},
		{"negative window", -time.Nanosecond, SeriesConfig{1, OverflowAggregate}, "rollup: bad window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			func() {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected panic %q", tc.want)
					}
					if r != tc.want {
						t.Fatalf("panic = %v, want %q", r, tc.want)
					}
				}()
				NewSeriesSet(tc.window, tc.cfg)
			}()
		})
	}
}

func TestSeriesSetValidationErrors(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 2, Overflow: OverflowAggregate})
	cases := []struct {
		name string
		smp  LabeledSample
		want error
	}{
		{"empty key", ls(0, 1, map[string]string{"": "v"}), ErrInvalidLabel},
		{"empty value", ls(0, 1, map[string]string{"k": ""}), ErrInvalidLabel},
		{"nan", ls(0, math.NaN(), map[string]string{"k": "v"}), ErrNonFinite},
		{"plus inf", ls(0, math.Inf(1), map[string]string{"k": "v"}), ErrNonFinite},
		{"minus inf", ls(0, math.Inf(-1), map[string]string{"k": "v"}), ErrNonFinite},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.Add(tc.smp); !errors.Is(err, tc.want) {
				t.Fatalf("Add = %v, want %v", err, tc.want)
			}
			if card := s.Cardinality(); card != (CardinalityReport{}) {
				t.Fatalf("rejection changed state: %+v", card)
			}
			if got := s.Snapshot(); len(got) != 0 {
				t.Fatalf("rejection filed windows: %+v", got)
			}
		})
	}
}

// TestSeriesSetDifferentSeriesDoNotConstrainOrder is covered directly in
// TestSeriesSetOutOfOrderAndSameInstant: series b files samples at
// timestamps before series a's marker without error.

func mustAdd(t *testing.T, s *SeriesSet, smp LabeledSample) {
	t.Helper()
	if err := s.Add(smp); err != nil {
		t.Fatalf("Add(%v): %v", smp, err)
	}
}

func TestSeriesSetOutOfOrderAndSameInstant(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 4, Overflow: OverflowAggregate})
	mustAdd(t, s, ls(1000, 1, map[string]string{"h": "a"}))
	mustAdd(t, s, ls(0, 10, map[string]string{"h": "b"}))
	mustAdd(t, s, ls(500, 2, map[string]string{"h": "b"})) // b: 500 >= 0, fine
	if err := s.Add(ls(500, 9, map[string]string{"h": "a"})); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("a at 500 after 1000: err = %v, want ErrOutOfOrder", err)
	}
	// Failed add changes nothing for series a.
	if card := s.Cardinality(); card.AcceptedSeries != 2 || card.OverflowSeries != 0 || card.RejectedSamples != 0 {
		t.Fatalf("cardinality = %+v", card)
	}
	// Same instant as the marker is accepted and counted again.
	mustAdd(t, s, ls(1000, 4, map[string]string{"h": "a"}))
	snap := s.Snapshot()
	// Series a: one window at 0s with count 2 (t=1000 twice), sum 5.
	got := findWindow(t, snap, map[string]string{"h": "a"}, false, 0)
	if got.Count != 2 || got.Sum != 5 || got.Min != 1 || got.Max != 4 {
		t.Fatalf("series a window = %+v", got)
	}
}

func findWindow(t *testing.T, snap []SeriesWindow, labels map[string]string, overflow bool, startNS int64) Window {
	t.Helper()
	for _, sw := range snap {
		if sw.Overflow != overflow || !sameLabelMaps(sw.Labels, labels) {
			continue
		}
		if sw.Window.Start.Equal(at(startNS)) {
			return sw.Window
		}
	}
	t.Fatalf("no window labels=%v overflow=%v start=%d in %+v", labels, overflow, startNS, snap)
	return Window{}
}

func sameLabelMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestSeriesSetMapOrderDoesNotMatter(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 2, Overflow: OverflowReject})
	mustAdd(t, s, ls(0, 1, map[string]string{"a": "1", "b": "2"}))
	// Same key/value set, different literal order: same series.
	mustAdd(t, s, ls(100, 2, map[string]string{"b": "2", "a": "1"}))
	if card := s.Cardinality(); card.AcceptedSeries != 1 {
		t.Fatalf("cardinality = %+v, want 1 accepted series", card)
	}
}

func TestSeriesSetAggregateOverflow(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 2, Overflow: OverflowAggregate})
	mustAdd(t, s, ls(0, 1, map[string]string{"h": "a"}))      // slot
	mustAdd(t, s, ls(sec(2), 2, map[string]string{"h": "b"})) // slot
	mustAdd(t, s, ls(0, 10, map[string]string{}))             // empty labels -> spill 1
	mustAdd(t, s, ls(sec(1), 7, map[string]string{"h": "c"})) // spill 2
	mustAdd(t, s, ls(sec(3), 5, map[string]string{"h": "c"})) // same spilled series, in-order
	mustAdd(t, s, ls(sec(4), 3, map[string]string{}))         // empty-label series again, not a new spill

	card := s.Cardinality()
	if card.AcceptedSeries != 2 || card.OverflowSeries != 2 || card.RejectedSamples != 0 {
		t.Fatalf("cardinality = %+v", card)
	}
	snap := s.Snapshot()

	// First-seen series order, overflow bucket last.
	if len(snap) == 0 {
		t.Fatal("empty snapshot")
	}
	if !sameLabelMaps(snap[0].Labels, map[string]string{"h": "a"}) || snap[0].Overflow {
		t.Fatalf("first entry = %+v, want series a", snap[0])
	}
	last := snap[len(snap)-1]
	if !last.Overflow || len(last.Labels) != 0 {
		t.Fatalf("last entry = %+v, want overflow bucket", last)
	}

	// The empty-label series and the h=c series merged in one bucket:
	// window 0s: sum 10 (empty labels); window 1s: sum 7; window 3s: sum 5;
	// window 4s: sum 3.
	if w := findWindow(t, snap, map[string]string{}, true, 0); w.Count != 1 || w.Sum != 10 {
		t.Fatalf("bucket 0s = %+v", w)
	}
	if w := findWindow(t, snap, map[string]string{}, true, sec(1)); w.Count != 1 || w.Sum != 7 {
		t.Fatalf("bucket 1s = %+v", w)
	}
	if w := findWindow(t, snap, map[string]string{}, true, sec(3)); w.Count != 1 || w.Sum != 5 {
		t.Fatalf("bucket 3s = %+v", w)
	}
	if w := findWindow(t, snap, map[string]string{}, true, sec(4)); w.Count != 1 || w.Sum != 3 {
		t.Fatalf("bucket 4s = %+v", w)
	}

	// The empty-label series is distinguishable from a slot series too:
	// it must not appear as a non-overflow entry.
	for _, sw := range snap {
		if !sw.Overflow && len(sw.Labels) == 0 {
			t.Fatalf("empty-label spilled series reported as accepted: %+v", sw)
		}
	}
}

func TestSeriesSetEmptyLabelsCanHoldASlot(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowAggregate})
	mustAdd(t, s, ls(0, 1, map[string]string{}))
	mustAdd(t, s, ls(100, 2, map[string]string{}))
	card := s.Cardinality()
	if card.AcceptedSeries != 1 || card.OverflowSeries != 0 {
		t.Fatalf("cardinality = %+v", card)
	}
	snap := s.Snapshot()
	if len(snap) != 1 || snap[0].Overflow || len(snap[0].Labels) != 0 {
		t.Fatalf("snapshot = %+v, want one non-overflow empty-label series", snap)
	}
}

func TestSeriesSetRejectOverflow(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowReject})
	mustAdd(t, s, ls(0, 1, map[string]string{"h": "a"}))
	// New series refused and counted per sample; repeated attempts count
	// again.
	for i := 0; i < 3; i++ {
		if err := s.Add(ls(int64(i), float64(i), map[string]string{"h": "b"})); !errors.Is(err, ErrSeriesLimit) {
			t.Fatalf("add %d: err = %v, want ErrSeriesLimit", i, err)
		}
	}
	// Samples of the accepted series still file.
	mustAdd(t, s, ls(500, 4, map[string]string{"h": "a"}))
	card := s.Cardinality()
	if card.AcceptedSeries != 1 || card.OverflowSeries != 0 || card.RejectedSamples != 3 {
		t.Fatalf("cardinality = %+v", card)
	}
	snap := s.Snapshot()
	if len(snap) != 1 || snap[0].Overflow {
		t.Fatalf("snapshot = %+v, want only the one accepted series", snap)
	}
	if w := snap[0].Window; w.Count != 2 || w.Sum != 5 {
		t.Fatalf("accepted window = %+v", w)
	}
}

func TestSeriesSetRejectBatchIsAllOrNothing(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowReject})
	mustAdd(t, s, ls(100, 1, map[string]string{"h": "a"}))

	// A batch whose second distinct series exceeds the quota is rejected
	// wholesale: no slot claimed, no rejection counted, no window filed.
	batch := []LabeledSample{
		ls(200, 2, map[string]string{"h": "a"}),
		ls(0, 9, map[string]string{"h": "b"}),
	}
	if err := s.AddBatch(batch); !errors.Is(err, ErrSeriesLimit) {
		t.Fatalf("AddBatch = %v, want ErrSeriesLimit", err)
	}
	if card := s.Cardinality(); card != (CardinalityReport{AcceptedSeries: 1}) {
		t.Fatalf("cardinality after rejected batch = %+v", card)
	}
	if w := s.Snapshot()[0].Window; w.Count != 1 || w.Sum != 1 {
		t.Fatalf("accepted series changed: %+v", w)
	}

	// A later single add of series a still works, proving the marker did
	// not advance to the rejected batch's t=200... it must accept t>=100.
	mustAdd(t, s, ls(150, 3, map[string]string{"h": "a"}))

	// Validation failures are all-or-nothing as well.
	for name, bad := range map[string]LabeledSample{
		"label": ls(300, 1, map[string]string{"": "x"}),
		"nan":   ls(300, math.NaN(), map[string]string{"h": "a"}),
	} {
		good := ls(300, 5, map[string]string{"h": "a"})
		if err := s.AddBatch([]LabeledSample{good, bad}); err == nil {
			t.Fatalf("%s: batch unexpectedly accepted", name)
		}
	}
	if w := s.Snapshot()[0].Window; w.Count != 2 || w.Sum != 4 {
		t.Fatalf("failed validation batches changed state: %+v", w)
	}

	// Out-of-order against the live marker rejects the batch too.
	if err := s.AddBatch([]LabeledSample{
		ls(500, 1, map[string]string{"h": "a"}),
		ls(120, 1, map[string]string{"h": "a"}),
	}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("stale batch: err = %v, want ErrOutOfOrder", err)
	}
	if w := s.Snapshot()[0].Window; w.Count != 2 {
		t.Fatalf("stale batch filed samples: %+v", w)
	}
}

func TestSeriesSetBatchFilesUnorderedWithinSeries(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 2, Overflow: OverflowAggregate})
	// Batch samples may be in any order and are not checked against each
	// other; the marker advances to the newest one.
	batch := []LabeledSample{
		ls(sec(2), 4, map[string]string{"h": "a"}),
		ls(0, 1, map[string]string{"h": "a"}),
		ls(500_000_000, 2, map[string]string{"h": "a"}),
		ls(0, 9, map[string]string{"h": "b"}), // second new series claims slot 2
	}
	if err := s.AddBatch(batch); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	snap := s.Snapshot()
	if w := findWindow(t, snap, map[string]string{"h": "a"}, false, 0); w.Count != 2 || w.Sum != 3 {
		t.Fatalf("a 0s = %+v", w)
	}
	if w := findWindow(t, snap, map[string]string{"h": "a"}, false, sec(2)); w.Count != 1 || w.Sum != 4 {
		t.Fatalf("a 2s = %+v", w)
	}
	// Marker is at 2s: an older add is now out of order.
	if err := s.Add(ls(sec(1)+500_000_000, 1, map[string]string{"h": "a"})); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("post-batch stale add: err = %v", err)
	}
	mustAdd(t, s, ls(sec(2), 8, map[string]string{"h": "a"})) // equal instant fine
}

func TestSeriesSetBatchOverflowCountedOnce(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowAggregate})
	batch := []LabeledSample{
		ls(0, 1, map[string]string{"h": "a"}), // slot
		ls(0, 2, map[string]string{"h": "b"}), // spill, twice
		ls(sec(1), 3, map[string]string{"h": "b"}),
		ls(0, 4, map[string]string{"h": "c"}), // another spill
	}
	if err := s.AddBatch(batch); err != nil {
		t.Fatalf("AddBatch: %v", err)
	}
	card := s.Cardinality()
	if card.AcceptedSeries != 1 || card.OverflowSeries != 2 {
		t.Fatalf("cardinality = %+v", card)
	}
	snap := s.Snapshot()
	// Bucket: b samples at 0 and 1s, c at 0.
	if w := findWindow(t, snap, map[string]string{}, true, 0); w.Count != 2 || w.Sum != 6 {
		t.Fatalf("bucket 0s = %+v", w)
	}
	if w := findWindow(t, snap, map[string]string{}, true, sec(1)); w.Count != 1 || w.Sum != 3 {
		t.Fatalf("bucket 1s = %+v", w)
	}

	// Batch entirely of a second spill after quota is full: still one
	// spill for that series, and its within-series ordering applies even
	// though the bucket merges alongside other spilled series.
	s2 := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowAggregate})
	mustAdd(t, s2, ls(0, 1, map[string]string{"h": "a"}))
	if err := s2.AddBatch([]LabeledSample{
		ls(sec(5), 1, map[string]string{"h": "b"}),
	}); err != nil {
		t.Fatalf("AddBatch b: %v", err)
	}
	mustAdd(t, s2, ls(sec(6), 1, map[string]string{"h": "c"})) // another spill, later
	if err := s2.Add(ls(sec(4), 1, map[string]string{"h": "b"})); !errors.Is(err, ErrOutOfOrder) {
		// b's own marker is 5s even though the shared bucket saw 6s from c.
		t.Fatalf("spilled per-series order: err = %v, want ErrOutOfOrder", err)
	}
	mustAdd(t, s2, ls(sec(5)+500_000_000, 1, map[string]string{"h": "b"}))
}

func TestSeriesSetEmptyBatchChangesNothing(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowReject})
	if err := s.AddBatch(nil); err != nil {
		t.Fatalf("nil batch: %v", err)
	}
	if err := s.AddBatch([]LabeledSample{}); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if card := s.Cardinality(); card != (CardinalityReport{}) {
		t.Fatalf("cardinality = %+v", card)
	}
}

func TestSeriesSetSnapshotStableOrder(t *testing.T) {
	s := NewSeriesSet(2*time.Second, SeriesConfig{MaxSeries: 3, Overflow: OverflowAggregate})
	mustAdd(t, s, ls(sec(9), 1, map[string]string{"h": "c"}))
	mustAdd(t, s, ls(sec(1), 1, map[string]string{"h": "a"}))
	mustAdd(t, s, ls(0, 1, map[string]string{"h": "b"}))
	mustAdd(t, s, ls(sec(10), 1, map[string]string{"h": "x"})) // spill
	snap := s.Snapshot()
	// Series in first-seen order c, a, b; windows inside each in time
	// order; overflow last.
	wantSeries := []map[string]string{
		{"h": "c"}, {"h": "a"}, {"h": "b"}, {},
	}
	gotSeries := []map[string]string{}
	prevOverflow := false
	for _, sw := range snap {
		if n := len(gotSeries); n == 0 ||
			!sameLabelMaps(gotSeries[n-1], sw.Labels) || prevOverflow != sw.Overflow {
			gotSeries = append(gotSeries, sw.Labels)
		}
		prevOverflow = sw.Overflow
	}
	if len(gotSeries) != len(wantSeries) {
		t.Fatalf("series order = %v, want %v (overflow last)", gotSeries, wantSeries)
	}
	for i := range wantSeries {
		if !sameLabelMaps(gotSeries[i], wantSeries[i]) {
			t.Fatalf("series %d = %v, want %v", i, gotSeries[i], wantSeries[i])
		}
	}
	// First entry of series c is the 8s-aligned window (9s lands in [8,10)).
	if !snap[0].Window.Start.Equal(at(sec(8))) {
		t.Fatalf("c window start = %v, want 8s", snap[0].Window.Start)
	}
}

func TestSeriesSetSnapshotIsImmutableCopy(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 1, Overflow: OverflowAggregate})
	mustAdd(t, s, ls(0, 1, map[string]string{"h": "a"}))
	snap := s.Snapshot()
	snap[0].Labels["h"] = "MUTATED"
	snap[0].Window.Count = 99
	// Another read must not observe the caller's writes.
	snap2 := s.Snapshot()
	if snap2[0].Labels["h"] != "a" || snap2[0].Window.Count != 1 {
		t.Fatalf("snapshot shares storage: %+v", snap2[0])
	}
}

func TestSeriesSetConcurrent(t *testing.T) {
	s := NewSeriesSet(time.Second, SeriesConfig{MaxSeries: 8, Overflow: OverflowAggregate})
	var writers sync.WaitGroup
	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				_ = s.Snapshot()
				_ = s.Cardinality()
				return
			default:
			}
			_ = s.Snapshot()
			_ = s.Cardinality()
		}
	}()
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			// Each worker keeps to its own three label identities with
			// non-decreasing timestamps, so its adds are never stale.
			for i := 0; i < 400; i++ {
				labels := map[string]string{"h": fmt.Sprintf("w%d-g%d", worker, i%3)}
				if err := s.Add(ls(int64(i), float64(i), labels)); err != nil {
					t.Errorf("worker %d add %d: %v", worker, i, err)
					return
				}
			}
		}(w)
	}
	writers.Wait()
	close(stop)
	reader.Wait()

	card := s.Cardinality()
	if card.AcceptedSeries != 8 {
		t.Fatalf("accepted %d series, want 8", card.AcceptedSeries)
	}
	if card.OverflowSeries != 4 {
		t.Fatalf("overflow series = %d, want 4", card.OverflowSeries)
	}
	for _, sw := range s.Snapshot() {
		if sw.Window.Count <= 0 {
			t.Fatalf("empty window in snapshot: %+v", sw)
		}
	}
}
