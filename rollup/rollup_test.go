package rollup

import (
	"errors"
	"testing"
	"time"
)

func TestWindowAlignment(t *testing.T) {
	r := New(10 * time.Second)
	// Boundaries are left-closed, right-open: a sample exactly at a window
	// end belongs to the next window.
	for _, at := range []time.Time{
		time.Unix(0, 0),
		time.Unix(5, 0),
		time.Unix(9, 0),
	} {
		if err := r.Add(at, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Add(time.Unix(10, 0), 2); err != nil {
		t.Fatal(err)
	}
	w, ok := r.Window(time.Unix(0, 0))
	if !ok {
		t.Fatal("window at 0 missing")
	}
	if w.Count != 3 || w.Sum != 3 || w.Min != 1 || w.Max != 1 {
		t.Fatalf("window 0: %+v", w)
	}
	w, ok = r.Window(time.Unix(10, 0))
	if !ok {
		t.Fatal("window at 10 missing")
	}
	if w.Count != 1 || w.Sum != 2 {
		t.Fatalf("window 10: %+v", w)
	}
	if _, ok = r.Window(time.Unix(1, 0)); ok {
		t.Fatal("no bucket starts at 1s; exact match expected")
	}
}

func TestPreEpochFloorsDown(t *testing.T) {
	r := New(10 * time.Second)
	if err := r.Add(time.Unix(-1, 0), 5); err != nil {
		t.Fatal(err)
	}
	w, ok := r.Window(time.Unix(-10, 0))
	if !ok {
		t.Fatal("sample at -1s should fall in window starting at -10s")
	}
	if w.Count != 1 || w.Sum != 5 {
		t.Fatalf("window -10: %+v", w)
	}
}

func TestOutOfOrder(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(120, 0), 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(time.Unix(119, 0), 1); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
	// State unchanged by the rejection.
	w, _ := r.Window(time.Unix(120, 0))
	if w.Count != 1 {
		t.Fatalf("rejected sample mutated state: %+v", w)
	}
	// Same instant is accepted and counted again.
	if err := r.Add(time.Unix(120, 0), 2); err != nil {
		t.Fatal(err)
	}
	w, _ = r.Window(time.Unix(120, 0))
	if w.Count != 2 || w.Sum != 3 {
		t.Fatalf("duplicate instant: %+v", w)
	}
}

func TestWindowsOrderedAndEmpty(t *testing.T) {
	r := New(time.Hour)
	if got := r.Windows(); len(got) != 0 {
		t.Fatalf("empty roller: %v", got)
	}
	for _, ts := range []int64{0, 3600, 7200} {
		if err := r.Add(time.Unix(ts, 0), float64(ts)); err != nil {
			t.Fatal(err)
		}
	}
	got := r.Windows()
	if len(got) != 3 {
		t.Fatalf("want 3 windows, got %d", len(got))
	}
	for i, want := range []int64{0, 3600, 7200} {
		if !got[i].Start.Equal(time.Unix(want, 0)) {
			t.Fatalf("window %d starts at %v, want %v", i, got[i].Start, want)
		}
	}
}

func TestMerge(t *testing.T) {
	a := New(time.Minute)
	b := New(time.Minute)
	a.Add(time.Unix(0, 0), 1)
	a.Add(time.Unix(0, 0), 3)
	b.Add(time.Unix(0, 0), 10)
	b.Add(time.Unix(60, 0), 7)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	w, _ := a.Window(time.Unix(0, 0))
	if w.Count != 3 || w.Sum != 14 || w.Min != 1 || w.Max != 10 {
		t.Fatalf("merged window: %+v", w)
	}
	if _, ok := a.Window(time.Unix(60, 0)); !ok {
		t.Fatal("other-only window not carried over")
	}
	// Merging the same stats twice counts them twice.
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	w, _ = a.Window(time.Unix(0, 0))
	if w.Count != 4 || w.Sum != 24 {
		t.Fatalf("double merge: %+v", w)
	}
	// Merge does not affect out-of-order tracking.
	if err := a.Add(time.Unix(0, 0), 0); err != nil {
		t.Fatalf("add after merge: %v", err)
	}
}

func TestMergeMismatchAndNil(t *testing.T) {
	a := New(time.Minute)
	a.Add(time.Unix(0, 0), 1)
	if err := a.Merge(New(time.Hour)); !errors.Is(err, ErrWindowMismatch) {
		t.Fatalf("expected ErrWindowMismatch, got %v", err)
	}
	if got := len(a.Windows()); got != 1 {
		t.Fatalf("failed merge mutated receiver: %d windows", got)
	}
	defer func() {
		if r := recover(); r != "rollup: nil roller" {
			t.Fatalf("panic = %v", r)
		}
	}()
	a.Merge(nil)
}

func TestBadWindowPanics(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if r := recover(); r != "rollup: bad window" {
					t.Fatalf("panic = %v", r)
				}
			}()
			New(d)
		}()
	}
}
