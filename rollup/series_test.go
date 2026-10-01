package rollup

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestSeriesKeyCanonicalization(t *testing.T) {
	// Keys and values that imitate the internal encoding's separators
	// must not collapse two label sets into one series.
	sets := []map[string]string{
		{"a": "1", "b": "2"},
		{"a:1|1": "1;1:b|1:2"},
		{"a|1:1;1": "b|1:2"},
	}
	s := NewSeriesSet(time.Second, len(sets), OverflowReject)
	for i, labels := range sets {
		if err := s.Add(LabeledSample{At: at(int64(i)), Value: 1, Labels: labels}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if c := s.Cardinality(); c.AcceptedSeries != len(sets) {
		t.Fatalf("distinct label sets collapsed: %+v", c)
	}
	// The same set expressed with different iteration order is one series.
	if err := s.Add(LabeledSample{At: at(9), Value: 1, Labels: map[string]string{"b": "2", "a": "1"}}); err != nil {
		t.Fatal(err)
	}
	if c := s.Cardinality(); c.AcceptedSeries != len(sets) {
		t.Fatalf("reordered pairs opened a new series: %+v", c)
	}
}

func TestNewSeriesSetBadConfig(t *testing.T) {
	cases := []struct {
		name      string
		window    time.Duration
		maxSeries int
		policy    OverflowPolicy
	}{
		{"zero window", 0, 1, OverflowAggregate},
		{"negative window", -time.Nanosecond, 1, OverflowAggregate},
		{"zero max", time.Nanosecond, 0, OverflowAggregate},
		{"negative max", time.Nanosecond, -2, OverflowAggregate},
		{"zero policy", time.Nanosecond, 1, 0},
		{"unknown policy", time.Nanosecond, 1, OverflowPolicy(99)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r != "rollup: bad series config" {
					t.Fatalf("panic = %v, want rollup: bad series config", r)
				}
			}()
			NewSeriesSet(tc.window, tc.maxSeries, tc.policy)
		})
	}
}

func TestSeriesIdentity(t *testing.T) {
	s := NewSeriesSet(time.Second, 4, OverflowAggregate)
	samples := []LabeledSample{
		{At: at(0), Value: 1, Labels: map[string]string{"host": "a", "env": "p"}},
		{At: at(1), Value: 2, Labels: map[string]string{"env": "p", "host": "a"}}, // same series
		{At: at(2), Value: 4, Labels: map[string]string{"host": "b", "env": "p"}},
		{At: at(3), Value: 8, Labels: map[string]string{"host": "a", "env": "q"}},
		{At: at(4), Value: 16, Labels: nil},
		{At: at(5), Value: 32, Labels: map[string]string{}},
	}
	for _, sm := range samples {
		if err := s.Add(sm); err != nil {
			t.Fatalf("Add %v: %v", sm.Labels, err)
		}
	}
	got, card := s.Snapshot()
	if card.AcceptedSeries != 4 {
		t.Fatalf("accepted = %d, want 4: %+v", card.AcceptedSeries, got)
	}
	want := []struct {
		labels map[string]string
		count  int64
		sum    float64
	}{
		{map[string]string{"host": "a", "env": "p"}, 2, 3},
		{map[string]string{"host": "b", "env": "p"}, 1, 4},
		{map[string]string{"host": "a", "env": "q"}, 1, 8},
		{map[string]string{}, 2, 48}, // nil and empty maps share one series
	}
	if len(got) != len(want) {
		t.Fatalf("series = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if !sameLabels(got[i].Labels, w.labels) {
			t.Fatalf("series %d labels = %v, want %v", i, got[i].Labels, w.labels)
		}
		if got[i].Overflow {
			t.Fatalf("series %d must not be the overflow bucket", i)
		}
		ws := got[i].Windows
		if len(ws) != 1 || ws[0].Count != w.count || ws[0].Sum != w.sum {
			t.Fatalf("series %d windows = %+v, want count %d sum %v", i, ws, w.count, w.sum)
		}
	}
}

func sameLabels(a, b map[string]string) bool {
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

func TestSeriesLabelErrors(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	cases := []map[string]string{
		{"": "v"},
		{"k": ""},
		{"": ""},
		{"a": "1", "": "v"},
	}
	for i, labels := range cases {
		err := s.Add(LabeledSample{At: at(int64(i)), Value: 1, Labels: labels})
		if !errors.Is(err, ErrInvalidLabel) {
			t.Fatalf("case %d: err = %v, want ErrInvalidLabel", i, err)
		}
	}
	if got, card := s.Snapshot(); len(got) != 0 || card.AcceptedSeries != 0 {
		t.Fatalf("invalid labels changed state: %+v %+v", got, card)
	}

	// Mutating the caller's map after a successful Add cannot change the
	// series identity or the report.
	labels := map[string]string{"host": "a"}
	if err := s.Add(LabeledSample{At: at(0), Value: 1, Labels: labels}); err != nil {
		t.Fatal(err)
	}
	labels["host"] = "b"
	got, _ := s.Snapshot()
	if len(got) != 1 || got[0].Labels["host"] != "a" {
		t.Fatalf("report aliases caller map: %+v", got)
	}
	got[0].Labels["host"] = "c"
	again, _ := s.Snapshot()
	if again[0].Labels["host"] != "a" {
		t.Fatalf("report map is shared state: %+v", again)
	}
}

func TestSeriesNonFinite(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		err := s.Add(LabeledSample{At: at(0), Value: v, Labels: map[string]string{"k": "v"}})
		if !errors.Is(err, ErrNonFinite) {
			t.Fatalf("err = %v, want ErrNonFinite", err)
		}
	}
	if got, _ := s.Snapshot(); len(got) != 0 {
		t.Fatalf("non-finite samples created a series: %+v", got)
	}
}

func TestSeriesOutOfOrderPerSeries(t *testing.T) {
	s := NewSeriesSet(time.Second, 3, OverflowAggregate)
	order := []LabeledSample{
		{At: at(100), Value: 1, Labels: map[string]string{"h": "a"}},
		{At: at(50), Value: 1, Labels: map[string]string{"h": "b"}}, // new series: fine
		{At: at(60), Value: 2, Labels: map[string]string{"h": "a"}}, // a: 60 < 100
		{At: at(40), Value: 2, Labels: map[string]string{"h": "b"}}, // b: 40 < 50
	}
	if err := s.Add(order[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(order[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(order[2]); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("a stale: err = %v, want ErrOutOfOrder", err)
	}
	if err := s.Add(order[3]); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("b stale: err = %v, want ErrOutOfOrder", err)
	}
	// A later sample on each series still files; 100ns and 200ns both
	// fall in the same 1s window, so series a ends with two samples.
	if err := s.Add(LabeledSample{At: at(200), Value: 9, Labels: map[string]string{"h": "a"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Snapshot()
	if len(got) != 2 {
		t.Fatalf("series = %+v", got)
	}
	if c := got[0].Windows[0].Count; c != 2 {
		t.Fatalf("rejected sample changed series a: count %d", c)
	}
	if sum := got[0].Windows[0].Sum; sum != 10 {
		t.Fatalf("series a sum = %v, want 10 (1 + 9)", sum)
	}
}

func TestOverflowAggregate(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	// a, b take the two slots; c and d overflow; empty-label series is a
	// real slot-less distinction check: put it in a slot here.
	lines := []struct {
		ns     int64
		v      float64
		labels map[string]string
	}{
		{0, 1, map[string]string{"h": "a"}},
		{0, 10, map[string]string{}}, // empty-label real series takes a slot
		{0, 100, map[string]string{"h": "c"}},
		{500000000, 200, map[string]string{"h": "d"}},
		{1500000000, 300, map[string]string{"h": "c"}}, // back to an overflowed origin
	}
	for _, l := range lines {
		if err := s.Add(LabeledSample{At: at(l.ns), Value: l.v, Labels: l.labels}); err != nil {
			t.Fatalf("%v: %v", l.labels, err)
		}
	}
	got, card := s.Snapshot()
	if card.AcceptedSeries != 2 || card.OverflowSeries != 2 || card.RejectedSamples != 0 {
		t.Fatalf("card = %+v", card)
	}
	if len(got) != 3 || !got[2].Overflow {
		t.Fatalf("overflow bucket missing or misplaced: %+v", got)
	}
	if !sameLabels(got[1].Labels, map[string]string{}) || got[1].Overflow {
		t.Fatalf("empty-label series not distinguishable from overflow: %+v", got[1])
	}
	ow := got[2].Windows
	if len(ow) != 2 {
		t.Fatalf("overflow windows = %+v, want two time-sorted windows", ow)
	}
	if ow[0].Start != at(0) || ow[0].Count != 2 || ow[0].Sum != 300 || ow[0].Min != 100 || ow[0].Max != 200 {
		t.Fatalf("overflow window 0 = %+v", ow[0])
	}
	if ow[1].Start != at(1000000000) || ow[1].Count != 1 || ow[1].Sum != 300 {
		t.Fatalf("overflow window 1 = %+v", ow[1])
	}
}

func TestOverflowReject(t *testing.T) {
	s := NewSeriesSet(time.Second, 1, OverflowReject)
	if err := s.Add(LabeledSample{At: at(0), Value: 1, Labels: map[string]string{"h": "a"}}); err != nil {
		t.Fatal(err)
	}
	// A new series is refused and counted.
	if err := s.Add(LabeledSample{At: at(1), Value: 2, Labels: map[string]string{"h": "b"}}); !errors.Is(err, ErrSeriesLimit) {
		t.Fatalf("err = %v, want ErrSeriesLimit", err)
	}
	// Refusing it never created the series, so a different new series is
	// also refused rather than admitted.
	if err := s.Add(LabeledSample{At: at(2), Value: 3, Labels: map[string]string{"h": "c"}}); !errors.Is(err, ErrSeriesLimit) {
		t.Fatalf("err = %v, want ErrSeriesLimit", err)
	}
	// The admitted series keeps accepting samples.
	if err := s.Add(LabeledSample{At: at(3), Value: 4, Labels: map[string]string{"h": "a"}}); err != nil {
		t.Fatal(err)
	}
	got, card := s.Snapshot()
	if len(got) != 1 || got[0].Labels["h"] != "a" {
		t.Fatalf("series = %+v", got)
	}
	if w := got[0].Windows[0]; w.Count != 2 || w.Sum != 5 {
		t.Fatalf("admitted window = %+v", w)
	}
	if card.AcceptedSeries != 1 || card.OverflowSeries != 0 || card.RejectedSamples != 2 {
		t.Fatalf("card = %+v", card)
	}
}

func TestOverflowOriginsKeepIndependentMarkers(t *testing.T) {
	s := NewSeriesSet(time.Second, 1, OverflowAggregate)
	// a takes the slot. b and c overflow. Their samples interleave in
	// time, and c's first sample is earlier than b's latest; that is not
	// out of order because they are different original series.
	seq := []LabeledSample{
		{At: at(0), Value: 1, Labels: map[string]string{"h": "a"}},
		{At: at(2000000000), Value: 2, Labels: map[string]string{"h": "b"}},
		{At: at(1000000000), Value: 4, Labels: map[string]string{"h": "c"}},
		{At: at(500000000), Value: 8, Labels: map[string]string{"h": "b"}}, // stale for b
		{At: at(1500000000), Value: 16, Labels: map[string]string{"h": "c"}},
	}
	if err := s.Add(seq[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(seq[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(seq[2]); err != nil {
		t.Fatalf("different overflow origin at an earlier instant must be allowed: %v", err)
	}
	if err := s.Add(seq[3]); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("same origin stale: err = %v, want ErrOutOfOrder", err)
	}
	if err := s.Add(seq[4]); err != nil {
		t.Fatal(err)
	}
	got, card := s.Snapshot()
	if card.AcceptedSeries != 1 || card.OverflowSeries != 2 {
		t.Fatalf("card = %+v", card)
	}
	if len(got) != 2 || !got[1].Overflow {
		t.Fatalf("series = %+v", got)
	}
	// c(1s)=4 and c(1.5s)=16 land in one window; b only at 2s. The
	// bucket combines the two origin rollers window for window.
	ow := got[1].Windows
	if len(ow) != 2 {
		t.Fatalf("overflow windows = %+v, want two", ow)
	}
	if ow[0].Start != at(1000000000) || ow[0].Count != 2 || ow[0].Sum != 20 {
		t.Fatalf("overflow window 1s = %+v", ow[0])
	}
	if ow[1].Start != at(2000000000) || ow[1].Count != 1 || ow[1].Sum != 2 {
		t.Fatalf("overflow window 2s = %+v", ow[1])
	}
}

func TestEmptySetSnapshot(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	got := s.Windows()
	if got == nil || len(got) != 0 {
		t.Fatalf("Windows = %#v, want empty non-nil slice", got)
	}
	if c := s.Cardinality(); c != (CardinalityReport{}) {
		t.Fatalf("card = %+v", c)
	}
}

func TestSeriesBatchAllOrNothing(t *testing.T) {
	t.Run("invalid label", func(t *testing.T) {
		s := NewSeriesSet(time.Second, 4, OverflowAggregate)
		batch := []LabeledSample{
			{At: at(0), Value: 1, Labels: map[string]string{"h": "a"}},
			{At: at(1), Value: 2, Labels: map[string]string{"h": "b", "": "x"}},
		}
		if err := s.AddBatch(batch); !errors.Is(err, ErrInvalidLabel) {
			t.Fatalf("err = %v", err)
		}
		if got, _ := s.Snapshot(); len(got) != 0 {
			t.Fatalf("batch changed state: %+v", got)
		}
	})
	t.Run("non-finite", func(t *testing.T) {
		s := NewSeriesSet(time.Second, 4, OverflowAggregate)
		batch := []LabeledSample{
			{At: at(0), Value: 1, Labels: map[string]string{"h": "a"}},
			{At: at(1), Value: math.NaN(), Labels: map[string]string{"h": "b"}},
		}
		if err := s.AddBatch(batch); !errors.Is(err, ErrNonFinite) {
			t.Fatalf("err = %v", err)
		}
		if got, _ := s.Snapshot(); len(got) != 0 {
			t.Fatalf("batch changed state: %+v", got)
		}
	})
	t.Run("out of order against marker", func(t *testing.T) {
		s := NewSeriesSet(time.Second, 4, OverflowAggregate)
		if err := s.Add(LabeledSample{At: at(100), Value: 1, Labels: map[string]string{"h": "a"}}); err != nil {
			t.Fatal(err)
		}
		batch := []LabeledSample{
			{At: at(101), Value: 2, Labels: map[string]string{"h": "b"}},
			{At: at(99), Value: 3, Labels: map[string]string{"h": "a"}},
		}
		if err := s.AddBatch(batch); !errors.Is(err, ErrOutOfOrder) {
			t.Fatalf("err = %v, want ErrOutOfOrder", err)
		}
		// Neither the stale sample nor the new series b survived.
		if got, _ := s.Snapshot(); len(got) != 1 {
			t.Fatalf("batch changed state: %+v", got)
		}
	})
	t.Run("reject policy", func(t *testing.T) {
		s := NewSeriesSet(time.Second, 1, OverflowReject)
		if err := s.Add(LabeledSample{At: at(0), Value: 1, Labels: map[string]string{"h": "a"}}); err != nil {
			t.Fatal(err)
		}
		batch := []LabeledSample{
			{At: at(1), Value: 2, Labels: map[string]string{"h": "a"}},
			{At: at(2), Value: 3, Labels: map[string]string{"h": "b"}},
		}
		if err := s.AddBatch(batch); !errors.Is(err, ErrSeriesLimit) {
			t.Fatalf("err = %v, want ErrSeriesLimit", err)
		}
		got, card := s.Snapshot()
		if card.AcceptedSeries != 1 || card.RejectedSamples != 0 {
			t.Fatalf("rejected batch counted or admitted: %+v %+v", got, card)
		}
		if w := got[0].Windows[0]; w.Count != 1 {
			t.Fatalf("rejected batch filed a sample: %+v", w)
		}
	})
	t.Run("empty batch", func(t *testing.T) {
		s := NewSeriesSet(time.Second, 1, OverflowReject)
		if err := s.AddBatch(nil); err != nil {
			t.Fatalf("nil batch: %v", err)
		}
		if err := s.AddBatch([]LabeledSample{}); err != nil {
			t.Fatalf("empty batch: %v", err)
		}
	})
}

func TestSeriesBatchAdmitsAndOverflows(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	// One slot is already held by a; the batch introduces b (admits), c
	// and d (overflow), revisits a, and carries c twice. Nothing in the
	// batch is earlier within any one target series.
	batch := []LabeledSample{
		{At: at(0), Value: 1, Labels: map[string]string{"h": "b"}},
		{At: at(0), Value: 2, Labels: map[string]string{"h": "c"}},
		{At: at(1000000000), Value: 4, Labels: map[string]string{"h": "d"}},
		{At: at(2000000000), Value: 8, Labels: map[string]string{"h": "a"}},
		{At: at(1000000003), Value: 16, Labels: map[string]string{"h": "c"}},
	}
	if err := s.Add(LabeledSample{At: at(0), Value: 32, Labels: map[string]string{"h": "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddBatch(batch); err != nil {
		t.Fatal(err)
	}
	got, card := s.Snapshot()
	if card.AcceptedSeries != 2 || card.OverflowSeries != 2 {
		t.Fatalf("card = %+v", card)
	}
	if len(got) != 3 {
		t.Fatalf("series = %+v", got)
	}
	if got[0].Labels["h"] != "a" || got[1].Labels["h"] != "b" || !got[2].Overflow {
		t.Fatalf("ordering = %+v", got)
	}
	ow := got[2].Windows
	if len(ow) != 2 || ow[0].Count != 1 || ow[0].Sum != 2 || ow[1].Count != 2 || ow[1].Sum != 20 {
		t.Fatalf("overflow windows = %+v", ow)
	}
}

func TestSeriesWindowsSortedAcrossSeries(t *testing.T) {
	s := NewSeriesSet(2*time.Second, 3, OverflowAggregate)
	// Scrambled across series but in non-decreasing time order within
	// each one.
	seq := []LabeledSample{
		{At: at(0), Value: 2, Labels: map[string]string{"h": "b"}},
		{At: at(0), Value: 16, Labels: map[string]string{"h": "a"}},
		{At: at(1000000000), Value: 4, Labels: map[string]string{"h": "b"}},
		{At: at(5000000000), Value: 1, Labels: map[string]string{"h": "a"}},
		{At: at(2000000000), Value: 8, Labels: map[string]string{"h": "b"}},
	}
	for _, sm := range seq {
		if err := s.Add(sm); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Snapshot()
	if len(got) != 2 {
		t.Fatalf("series = %+v", got)
	}
	var b SeriesWindow
	for _, sw := range got {
		if sw.Labels["h"] == "b" {
			b = sw
		}
		for j := 1; j < len(sw.Windows); j++ {
			if !sw.Windows[j-1].Start.Before(sw.Windows[j].Start) {
				t.Fatalf("series %v windows not sorted: %+v", sw.Labels, sw.Windows)
			}
		}
	}
	starts := []int64{b.Windows[0].Start.UnixNano(), b.Windows[1].Start.UnixNano()}
	if starts[0] != 0 || starts[1] != 2000000000 {
		t.Fatalf("b starts = %v, want [0 2s] (2s windows)", starts)
	}
}

func TestSeriesSnapshotIsStable(t *testing.T) {
	s := NewSeriesSet(time.Second, 2, OverflowAggregate)
	for _, h := range []string{"a", "b"} {
		if err := s.Add(LabeledSample{At: at(0), Value: 1, Labels: map[string]string{"h": h}}); err != nil {
			t.Fatal(err)
		}
	}
	first, _ := s.Snapshot()
	if err := s.Add(LabeledSample{At: at(1), Value: 9, Labels: map[string]string{"h": "a"}}); err != nil {
		t.Fatal(err)
	}
	if len(first[0].Windows) != 1 || first[0].Windows[0].Count != 1 {
		t.Fatalf("snapshot changed after a later Add: %+v", first)
	}
}

func TestSeriesConcurrent(t *testing.T) {
	s := NewSeriesSet(time.Nanosecond, 8, OverflowAggregate)
	const writers = 8
	const each = 200
	var writersDone sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersDone.Add(1)
		// Each writer owns its own series, so the lock-acquisition order
		// can never put one writer's older sample after another's.
		labels := map[string]string{"h": fmt.Sprintf("w%d", w)}
		go func() {
			defer writersDone.Done()
			for i := 0; i < each; i++ {
				if err := s.Add(LabeledSample{
					At:     at(int64(i)),
					Value:  float64(i),
					Labels: labels,
				}); err != nil {
					t.Errorf("Add: %v", err)
					return
				}
			}
		}()
	}
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.Snapshot()
			}
		}
	}()
	writersDone.Wait()
	close(stop)
	<-readerDone
	got, card := s.Snapshot()
	if card.AcceptedSeries != 8 {
		t.Fatalf("accepted = %d, want 8", card.AcceptedSeries)
	}
	var total int64
	for _, sw := range got {
		for _, w := range sw.Windows {
			total += w.Count
		}
	}
	if total != writers*each {
		t.Fatalf("filed %d samples, want %d", total, writers*each)
	}
}
