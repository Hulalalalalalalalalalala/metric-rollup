package rollup

import (
	"fmt"
	"testing"
	"time"
)

var sinkWindows []Window
var sinkWindow Window
var sinkOK bool

// benchRoller builds a roller with n windows of one sample each, starting
// at the Unix epoch.
func benchRoller(window time.Duration, n int) *Roller {
	r := New(window)
	base := time.Unix(0, 0)
	for i := 0; i < n; i++ {
		if err := r.Add(base.Add(time.Duration(i)*window), float64(i)); err != nil {
			panic(err)
		}
	}
	return r
}

// BenchmarkAdd measures sustained sample submission; each sample opens a
// new window, the append-heavy hot path.
func BenchmarkAdd(b *testing.B) {
	r := New(time.Second)
	base := time.Unix(0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Add(base.Add(time.Duration(i)*time.Second), float64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddSameWindow measures submission into an already-open window,
// which must not allocate or grow memory per sample.
func BenchmarkAddSameWindow(b *testing.B) {
	r := New(time.Hour)
	at := time.Unix(0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Add(at, float64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddParallel measures concurrent submission at a single instant,
// where every sample is accepted.
func BenchmarkAddParallel(b *testing.B) {
	r := New(time.Second)
	at := time.Unix(0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := r.Add(at, 1); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkAddBackfill measures filing windows older than the newest
// bucket after a merge; the cost must not grow with the number of
// accepted windows.
func BenchmarkAddBackfill(b *testing.B) {
	r := New(time.Second)
	if err := r.Add(time.Unix(0, 0), 0); err != nil {
		b.Fatal(err)
	}
	src := benchRoller(time.Second, 0)
	base := time.Unix(1_000_000_000_000, 0)
	for i := 0; i < 100000; i++ {
		if err := src.Add(base.Add(time.Duration(i)*time.Second), float64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := r.Merge(src); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Add(time.Unix(int64(1+i), 0), float64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWindows measures reading out the full window set.
func BenchmarkWindows(b *testing.B) {
	for _, n := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			r := benchRoller(time.Second, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sinkWindows = r.Windows()
			}
		})
	}
}

// BenchmarkWindow measures point lookups by window start.
func BenchmarkWindow(b *testing.B) {
	const n = 100000
	r := benchRoller(time.Second, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkWindow, sinkOK = r.Window(time.Unix(int64(i%n), 0))
	}
}

// BenchmarkMerge measures folding two rollers of n windows each together,
// both fully overlapping and fully disjoint.
func BenchmarkMerge(b *testing.B) {
	for _, n := range []int{1000, 100000} {
		src := benchRoller(time.Second, n)
		b.Run(fmt.Sprintf("overlap/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dst := benchRoller(time.Second, n)
				b.StartTimer()
				if err := dst.Merge(src); err != nil {
					b.Fatal(err)
				}
			}
		})
		offset := benchRoller(time.Second, 0)
		base := time.Unix(int64(n), 0)
		for i := 0; i < n; i++ {
			if err := offset.Add(base.Add(time.Duration(i)*time.Second), float64(i)); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("disjoint/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dst := benchRoller(time.Second, n)
				b.StartTimer()
				if err := dst.Merge(offset); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
