package rollup

import (
	"errors"
	"math"
	"testing"
	"time"
)

func utc(t string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return parsed.UTC()
}

func TestNewPanicsOnNonPositiveWindow(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Minute} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%v) did not panic", d)
				}
			}()
			New(d)
		}()
	}
}

func TestAddAggregatesOneWindow(t *testing.T) {
	r := New(time.Minute)
	samples := []struct {
		at    string
		value float64
	}{
		{"2026-01-01T00:00:00Z", 1},
		{"2026-01-01T00:00:15Z", 2.5},
		{"2026-01-01T00:00:59.999999999Z", 4},
	}
	for _, s := range samples {
		if err := r.Add(utc(s.at), s.value); err != nil {
			t.Fatalf("Add(%s, %v): %v", s.at, s.value, err)
		}
	}

	ws := r.Windows()
	if len(ws) != 1 {
		t.Fatalf("got %d windows, want 1", len(ws))
	}
	w := ws[0]
	if want := utc("2026-01-01T00:00:00Z"); !w.Start.Equal(want) {
		t.Errorf("Start = %v, want %v", w.Start, want)
	}
	if w.Count != 3 {
		t.Errorf("Count = %d, want 3", w.Count)
	}
	if w.Sum != 7.5 {
		t.Errorf("Sum = %v, want 7.5", w.Sum)
	}
	if w.Min != 1 {
		t.Errorf("Min = %v, want 1", w.Min)
	}
	if w.Max != 4 {
		t.Errorf("Max = %v, want 4", w.Max)
	}
}

func TestAddAlignsToEpoch(t *testing.T) {
	r := New(2 * time.Minute)
	if err := r.Add(utc("2026-01-01T00:01:59Z"), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(utc("2026-01-01T00:02:00Z"), 2); err != nil {
		t.Fatal(err)
	}

	ws := r.Windows()
	if len(ws) != 2 {
		t.Fatalf("got %d windows, want 2", len(ws))
	}
	if got, want := ws[0].Start, utc("2026-01-01T00:00:00Z"); !got.Equal(want) {
		t.Errorf("first start = %v, want %v", got, want)
	}
	if got, want := ws[1].Start, utc("2026-01-01T00:02:00Z"); !got.Equal(want) {
		t.Errorf("second start = %v, want %v", got, want)
	}
}

func TestAddAlignsPreEpochDownward(t *testing.T) {
	r := New(time.Minute)
	// 30 seconds before the epoch floors to the minute containing it.
	if err := r.Add(time.Unix(-30, 0).UTC(), 1); err != nil {
		t.Fatal(err)
	}
	ws := r.Windows()
	if len(ws) != 1 {
		t.Fatalf("got %d windows, want 1", len(ws))
	}
	if got, want := ws[0].Start, time.Unix(-60, 0).UTC(); !got.Equal(want) {
		t.Errorf("Start = %v, want %v", got, want)
	}
}

func TestWindowsSortedByStart(t *testing.T) {
	r := New(time.Minute)
	// Insert buckets out of key order directly; map iteration order is
	// randomized, so Windows() must sort regardless of insertion order.
	for _, at := range []string{
		"2026-01-01T00:04:00Z",
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:02:00Z",
		"2026-01-01T00:01:00Z",
	} {
		st := utc(at)
		r.buckets[st.UnixNano()] = Window{Start: st, Count: 1, Sum: 1, Min: 1, Max: 1}
	}

	ws := r.Windows()
	if len(ws) != 4 {
		t.Fatalf("got %d windows, want 4", len(ws))
	}
	for i := 1; i < len(ws); i++ {
		if !ws[i-1].Start.Before(ws[i].Start) {
			t.Fatalf("windows not sorted at index %d: %v then %v", i, ws[i-1].Start, ws[i].Start)
		}
	}
}

func TestEmptyWindows(t *testing.T) {
	r := New(time.Minute)
	ws := r.Windows()
	if len(ws) != 0 {
		t.Fatalf("got %d windows, want 0", len(ws))
	}
	if ws == nil {
		t.Error("Windows() is nil, want empty non-nil slice")
	}
}

func TestDuplicateSampleCountedOnce(t *testing.T) {
	r := New(time.Minute)
	at := utc("2026-01-01T00:00:30Z")
	for i := 0; i < 3; i++ {
		if err := r.Add(at, 2.5); err != nil {
			t.Fatalf("duplicate Add #%d: %v", i, err)
		}
	}
	if err := r.Add(utc("2026-01-01T00:00:45Z"), 2.5); err != nil {
		t.Fatal(err)
	}

	ws := r.Windows()
	if ws[0].Count != 2 {
		t.Errorf("Count = %d, want 2 (duplicates counted once)", ws[0].Count)
	}
	if ws[0].Sum != 5 {
		t.Errorf("Sum = %v, want 5", ws[0].Sum)
	}
	if ws[0].Min != 2.5 || ws[0].Max != 2.5 {
		t.Errorf("Min/Max = %v/%v, want 2.5/2.5", ws[0].Min, ws[0].Max)
	}
}

func TestSameTimestampDifferentValueOutOfOrder(t *testing.T) {
	r := New(time.Minute)
	at := utc("2026-01-01T00:00:30Z")
	if err := r.Add(at, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(at, 2); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("got %v, want ErrOutOfOrder", err)
	}

	// The rejected value must not have been folded in.
	ws := r.Windows()
	if ws[0].Count != 1 || ws[0].Sum != 1 || ws[0].Min != 1 || ws[0].Max != 1 {
		t.Errorf("window changed after rejected add: %+v", ws[0])
	}
}

func TestEarlierSampleOutOfOrder(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(utc("2026-01-01T00:01:00Z"), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(utc("2026-01-01T00:00:59Z"), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("got %v, want ErrOutOfOrder", err)
	}

	// A value equal to the later one is still out of order.
	if err := r.Add(utc("2026-01-01T00:00:30Z"), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("got %v, want ErrOutOfOrder", err)
	}
}

func TestWindowLookup(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(utc("2026-01-01T00:00:30Z"), 2.5); err != nil {
		t.Fatal(err)
	}

	start := utc("2026-01-01T00:00:00Z")
	w, ok := r.Window(start)
	if !ok {
		t.Fatal("Window(existing start) returned ok = false")
	}
	if w.Count != 1 || w.Sum != 2.5 || w.Min != 2.5 || w.Max != 2.5 {
		t.Errorf("unexpected window: %+v", w)
	}
	if !w.Start.Equal(start) {
		t.Errorf("Start = %v, want %v", w.Start, start)
	}

	// Middle of the bucket, other windows and the zero roller all miss.
	for _, miss := range []time.Time{
		utc("2026-01-01T00:00:30Z"),
		utc("2026-01-01T00:01:00Z"),
	} {
		if w, ok := r.Window(miss); ok {
			t.Errorf("Window(%v) = %+v, true; want zero, false", miss, w)
		} else if w != (Window{}) {
			t.Errorf("Window(%v) miss returned %+v, want zero value", miss, w)
		}
	}

	if w, ok := New(time.Minute).Window(start); ok || w != (Window{}) {
		t.Errorf("empty roller lookup = %+v, %v; want zero, false", w, ok)
	}
}

func TestMergeCombinesAndSorts(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)

	mustAdd := func(r *Roller, at string, v float64) {
		t.Helper()
		if err := r.Add(utc(at), v); err != nil {
			t.Fatal(err)
		}
	}
	// Shared 00:00 bucket plus one bucket each on either side.
	mustAdd(a, "2026-01-01T00:00:10Z", 2)
	mustAdd(a, "2026-01-01T00:02:00Z", 10)
	mustAdd(b, "2026-01-01T00:00:50Z", 4)
	mustAdd(b, "2026-01-01T00:01:05Z", -3)

	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	ws := a.Windows()
	if len(ws) != 3 {
		t.Fatalf("got %d windows, want 3", len(ws))
	}
	wantStarts := []string{
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:01:00Z",
		"2026-01-01T00:02:00Z",
	}
	for i, want := range wantStarts {
		if !ws[i].Start.Equal(utc(want)) {
			t.Errorf("window %d start = %v, want %v", i, ws[i].Start, want)
		}
	}

	shared := ws[0]
	if shared.Count != 2 || shared.Sum != 6 || shared.Min != 2 || shared.Max != 4 {
		t.Errorf("shared bucket = %+v, want count 2 sum 6 min 2 max 4", shared)
	}
	if ws[1].Min != -3 || ws[1].Max != -3 {
		t.Errorf("b-only bucket = %+v, want min/max -3", ws[1])
	}
}

func TestMergeLeavesOtherUntouched(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	if err := a.Add(utc("2026-01-01T00:00:00Z"), 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(utc("2026-01-01T00:00:00Z"), 2); err != nil {
		t.Fatal(err)
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	ws := b.Windows()
	if len(ws) != 1 || ws[0].Count != 1 || ws[0].Sum != 2 {
		t.Fatalf("other mutated by Merge: %+v", ws)
	}
}

func TestMergeNil(t *testing.T) {
	a := New(time.Minute)
	if err := a.Merge(nil); !errors.Is(err, ErrWindowMismatch) {
		t.Fatalf("Merge(nil) = %v, want ErrWindowMismatch", err)
	}
}

func TestMergeDifferentWindowSize(t *testing.T) {
	a := New(time.Minute)
	b := New(2 * time.Minute)
	if err := a.Merge(b); !errors.Is(err, ErrWindowMismatch) {
		t.Fatalf("Merge(size 2m) = %v, want ErrWindowMismatch", err)
	}
	if ws := a.Windows(); len(ws) != 0 {
		t.Fatalf("receiver changed after mismatched merge: %+v", ws)
	}
}

func TestFloat64Extremes(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(utc("2026-01-01T00:00:00Z"), math.Inf(1)); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(utc("2026-01-01T00:00:01Z"), math.Inf(-1)); err != nil {
		t.Fatal(err)
	}
	ws := r.Windows()
	if !math.IsInf(ws[0].Max, 1) || !math.IsInf(ws[0].Min, -1) {
		t.Errorf("Min/Max = %v/%v, want -Inf/+Inf", ws[0].Min, ws[0].Max)
	}
}
