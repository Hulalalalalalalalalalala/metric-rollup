package rollup

import (
	"testing"
	"time"
)

// cloneRoller duplicates a roller's windows so merge benchmarks start
// from an untouched receiver every iteration.
func cloneRoller(src *Roller) *Roller {
	dst := New(src.window)
	dst.chunks = make([][]slot, len(src.chunks))
	for i, ch := range src.chunks {
		cp := make([]slot, len(ch), cap(ch))
		copy(cp, ch)
		dst.chunks[i] = cp
	}
	dst.n = src.n
	return dst
}

// buildWindows makes a roller with n one-per-window samples starting at
// window index offset.
func buildWindows(n int, offset int64) *Roller {
	r := New(time.Minute)
	for i := 0; i < n; i++ {
		if err := r.Add(time.Unix((offset+int64(i))*60, 0), float64(i)); err != nil {
			panic(err)
		}
	}
	return r
}

// BenchmarkAddMonotone files samples at increasing instants; most opens a
// new window (append path), one in 60 lands in an existing window.
func BenchmarkAddMonotone(b *testing.B) {
	r := New(time.Minute)
	for i := 0; i < b.N; i++ {
		if err := r.Add(time.Unix(int64(i), 0), 1.0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddSameWindow files every sample at the same instant, so every
// operation is an in-place update with no allocation.
func BenchmarkAddSameWindow(b *testing.B) {
	r := New(time.Hour)
	at := time.Unix(0, 0)
	for i := 0; i < b.N; i++ {
		if err := r.Add(at, float64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddBackfill files windows into the middle of the range after a
// merge, the case a single sorted slice would handle quadratically. Each
// iteration backfills 100k windows; setup is excluded from timing.
func BenchmarkAddBackfill(b *testing.B) {
	const later = 100000
	b.ReportAllocs()
	for k := 0; k < b.N; k++ {
		b.StopTimer()
		r := New(time.Minute)
		r.Add(time.Unix(0, 0), 0)
		// Merge in the far windows later+1..2*later; the merge leaves the
		// out-of-order watermark at 0, so the gaps 1..later can still be
		// backfilled as interior inserts.
		r.Merge(buildWindows(later, int64(later)+1))
		b.StartTimer()
		for i := 0; i < later; i++ {
			if err := r.Add(time.Unix(int64(i+1)*60, 0), 1); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkWindows100k takes a full snapshot of a 100k-window roller.
func BenchmarkWindows100k(b *testing.B) {
	r := buildWindows(100000, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		got := r.Windows()
		if len(got) != 100000 {
			b.Fatalf("got %d windows", len(got))
		}
	}
}

// BenchmarkMergeDisjoint merges two 100k-window rollers with no overlap.
func BenchmarkMergeDisjoint(b *testing.B) {
	const n = 100000
	src := buildWindows(n, n) // keys n..2n-1
	tmpl := buildWindows(n, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dst := cloneRoller(tmpl)
		b.StartTimer()
		if err := dst.Merge(src); err != nil {
			b.Fatal(err)
		}
		if dst.n != 2*n {
			b.Fatalf("got %d windows", dst.n)
		}
	}
}

// BenchmarkMergeOverlap merges two 100k-window rollers keyed identically.
func BenchmarkMergeOverlap(b *testing.B) {
	const n = 100000
	src := buildWindows(n, 0)
	tmpl := buildWindows(n, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dst := cloneRoller(tmpl)
		b.StartTimer()
		if err := dst.Merge(src); err != nil {
			b.Fatal(err)
		}
		if dst.n != n {
			b.Fatalf("got %d windows", dst.n)
		}
	}
}
