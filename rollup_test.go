package rollup

import (
	"errors"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return at
}

func TestNewPanicsOnNonPositiveWindow(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, -time.Minute} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%v) did not panic", d)
				}
			}()
			New(d)
		}()
	}
}

func TestAddSingleSample(t *testing.T) {
	r := New(time.Minute)
	at := mustTime(t, "2026-01-02T15:04:05Z")
	if err := r.Add(at, 2.5); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got := r.Windows()
	if len(got) != 1 {
		t.Fatalf("got %d windows, want 1", len(got))
	}
	w := got[0]
	wantStart := mustTime(t, "2026-01-02T15:04:00Z")
	if !w.Start.Equal(wantStart) || w.Start.Location() != time.UTC {
		t.Errorf("Start = %v, want %v UTC", w.Start, wantStart)
	}
	if w.Count != 1 || w.Sum != 2.5 || w.Min != 2.5 || w.Max != 2.5 {
		t.Errorf("window = %+v, want count 1 sum/min/max 2.5", w)
	}
}

func TestAddAggregatesOneWindow(t *testing.T) {
	r := New(time.Minute)
	at := mustTime(t, "2026-01-02T15:04:00Z")
	samples := []float64{2.5, -1.0, 4.0, 0.5}
	for i, v := range samples {
		if err := r.Add(at.Add(time.Duration(i)*time.Second), v); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	got := r.Windows()
	if len(got) != 1 {
		t.Fatalf("got %d windows, want 1", len(got))
	}
	w := got[0]
	if w.Count != 4 || w.Sum != 6.0 || w.Min != -1.0 || w.Max != 4.0 {
		t.Errorf("window = %+v, want count 4 sum 6 min -1 max 4", w)
	}
}

func TestAddAlignsToUnixEpochAndSortsByStart(t *testing.T) {
	r := New(5 * time.Minute)
	// Samples are offered in time order; Windows() must return bucket order.
	if err := r.Add(mustTime(t, "2025-12-31T23:59:30Z"), 9); err != nil { // earlier window
		t.Fatal(err)
	}
	// 00:07 lands in the epoch-aligned 00:05 bucket, not a 00:02-anchored one.
	if err := r.Add(mustTime(t, "2026-01-02T00:07:00Z"), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(mustTime(t, "2026-01-02T00:14:00Z"), 2); err != nil {
		t.Fatal(err)
	}
	got := r.Windows()
	wantStarts := []string{
		"2025-12-31T23:55:00Z",
		"2026-01-02T00:05:00Z",
		"2026-01-02T00:10:00Z",
	}
	if len(got) != len(wantStarts) {
		t.Fatalf("got %d windows %v, want %d", len(got), got, len(wantStarts))
	}
	for i, want := range wantStarts {
		if got[i].Start.Format(time.RFC3339Nano) != want {
			t.Errorf("window %d start = %s, want %s", i, got[i].Start.Format(time.RFC3339Nano), want)
		}
	}
}

func TestAddBucketsBeforeEpoch(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(mustTime(t, "1969-12-31T23:58:05Z"), 3); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(mustTime(t, "1969-12-31T23:59:10Z"), 4); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(mustTime(t, "1970-01-01T00:00:20Z"), 5); err != nil {
		t.Fatal(err)
	}
	got := r.Windows()
	wantStarts := []string{
		"1969-12-31T23:58:00Z",
		"1969-12-31T23:59:00Z",
		"1970-01-01T00:00:00Z",
	}
	if len(got) != 3 {
		t.Fatalf("got %d windows, want 3: %v", len(got), got)
	}
	for i, want := range wantStarts {
		if got[i].Start.Format(time.RFC3339Nano) != want {
			t.Errorf("window %d = %s, want %s", i, got[i].Start.Format(time.RFC3339Nano), want)
		}
	}
}

func TestAddDuplicateCountedOnce(t *testing.T) {
	r := New(time.Minute)
	at := mustTime(t, "2026-01-02T15:04:05.5Z")
	for i := 0; i < 3; i++ {
		if err := r.Add(at, 2.5); err != nil {
			t.Fatalf("duplicate Add %d: %v", i, err)
		}
	}
	got := r.Windows()
	if len(got) != 1 {
		t.Fatalf("got %d windows, want 1", len(got))
	}
	if w := got[0]; w.Count != 1 || w.Sum != 2.5 {
		t.Errorf("window = %+v, want duplicate ignored", w)
	}
}

func TestAddSameTimeDifferentValueOutOfOrder(t *testing.T) {
	r := New(time.Minute)
	at := mustTime(t, "2026-01-02T15:04:05Z")
	if err := r.Add(at, 2.5); err != nil {
		t.Fatal(err)
	}
	err := r.Add(at, 2.6)
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err = %v, want ErrOutOfOrder", err)
	}
	// The rejected sample must not change the bucket.
	w := r.Windows()[0]
	if w.Count != 1 || w.Sum != 2.5 {
		t.Errorf("window = %+v, rejected sample was recorded", w)
	}
}

func TestAddEarlierSampleOutOfOrder(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(mustTime(t, "2026-01-02T15:04:05Z"), 1); err != nil {
		t.Fatal(err)
	}
	// Same bucket but earlier timestamp.
	err := r.Add(mustTime(t, "2026-01-02T15:04:04Z"), 2)
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("same-bucket err = %v, want ErrOutOfOrder", err)
	}
	// Earlier bucket.
	err = r.Add(mustTime(t, "2026-01-02T15:03:00Z"), 2)
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("earlier-bucket err = %v, want ErrOutOfOrder", err)
	}
}

func TestAddDifferentTimezonesSameInstant(t *testing.T) {
	r := New(time.Minute)
	// Same instant expressed in different zones must still be a duplicate.
	utc := mustTime(t, "2026-01-02T15:04:05Z")
	plus2 := mustTime(t, "2026-01-02T17:04:05+02:00")
	if err := r.Add(utc, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(plus2, 1); err != nil {
		t.Fatalf("same instant in +02:00: %v", err)
	}
	if got := r.Windows(); len(got) != 1 || got[0].Count != 1 {
		t.Errorf("got %+v, want one window with one sample", got)
	}
}

func TestWindowLookup(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(mustTime(t, "2026-01-02T15:04:30Z"), 2.5); err != nil {
		t.Fatal(err)
	}
	t.Run("aligned start exists", func(t *testing.T) {
		w, ok := r.Window(mustTime(t, "2026-01-02T15:04:00Z"))
		if !ok {
			t.Fatal("ok = false, want true")
		}
		if w.Count != 1 || w.Sum != 2.5 {
			t.Errorf("window = %+v", w)
		}
	})
	t.Run("interior time not a start", func(t *testing.T) {
		if w, ok := r.Window(mustTime(t, "2026-01-02T15:04:30Z")); ok || w != (Window{}) {
			t.Errorf("got %+v %v, want zero window false", w, ok)
		}
	})
	t.Run("aligned start absent", func(t *testing.T) {
		if _, ok := r.Window(mustTime(t, "2026-01-02T15:05:00Z")); ok {
			t.Error("ok = true for empty window")
		}
	})
}

func TestWindowsEmpty(t *testing.T) {
	r := New(time.Minute)
	got := r.Windows()
	if got == nil {
		t.Error("Windows() = nil, want empty non-nil slice")
	}
	if len(got) != 0 {
		t.Errorf("Windows() = %v, want empty", got)
	}
}

func TestMergeDisjointWindows(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	if err := a.Add(mustTime(t, "2026-01-02T15:04:10Z"), 2); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(mustTime(t, "2026-01-02T15:05:10Z"), 4); err != nil {
		t.Fatal(err)
	}
	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got := a.Windows()
	if len(got) != 2 {
		t.Fatalf("got %d windows, want 2: %v", len(got), got)
	}
	if got[0].Start.Format("15:04") != "15:04" || got[0].Sum != 2 {
		t.Errorf("window 0 = %+v", got[0])
	}
	if got[1].Start.Format("15:04") != "15:05" || got[1].Sum != 4 {
		t.Errorf("window 1 = %+v", got[1])
	}
}

func TestMergeOverlappingWindows(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	for _, v := range []float64{1, 7} {
		if err := a.Add(mustTime(t, "2026-01-02T15:04:00Z").Add(time.Duration(v)*time.Second), v); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []float64{-3, 2, 9} {
		if err := b.Add(mustTime(t, "2026-01-02T15:04:10Z").Add(time.Duration(v+3)*time.Second), v); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got := a.Windows()
	if len(got) != 1 {
		t.Fatalf("got %d windows, want 1: %v", len(got), got)
	}
	w := got[0]
	if w.Count != 5 {
		t.Errorf("Count = %d, want 5", w.Count)
	}
	if w.Sum != 16.0 { // 1 + 7 - 3 + 2 + 9
		t.Errorf("Sum = %v, want 16", w.Sum)
	}
	if w.Min != -3.0 {
		t.Errorf("Min = %v, want -3", w.Min)
	}
	if w.Max != 9.0 {
		t.Errorf("Max = %v, want 9", w.Max)
	}
	// The source roller is untouched.
	if bw := b.Windows()[0]; bw.Count != 3 {
		t.Errorf("source window changed after merge: %+v", bw)
	}
}

func TestMergeErrors(t *testing.T) {
	r := New(time.Minute)
	if err := r.Merge(nil); !errors.Is(err, ErrWindowMismatch) {
		t.Errorf("Merge(nil) = %v, want ErrWindowMismatch", err)
	}
	if err := r.Merge(New(2 * time.Minute)); !errors.Is(err, ErrWindowMismatch) {
		t.Errorf("Merge(other size) = %v, want ErrWindowMismatch", err)
	}
}

func TestMergeSameRoller(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(mustTime(t, "2026-01-02T15:04:00Z"), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Merge(r); err != nil {
		t.Errorf("Merge(self) = %v, want nil", err)
	}
	if got := r.Windows(); len(got) != 1 || got[0].Count != 1 {
		t.Errorf("self-merge changed data: %+v", got)
	}
}
