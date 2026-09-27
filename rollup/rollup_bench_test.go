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

// BenchmarkMassBackfill measures the post-merge backfill path, the one
// write path that used to be quadratic: one merged-in window far ahead of
// the accepted marker, then every gap window filed newest-first in a
// single batch. Time must scale near-linearly with the window count and
// the temporary working set stay bounded by the windows themselves.
func BenchmarkMassBackfill(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			batch := make([]Sample, 0, n)
			for i := n; i >= 1; i-- {
				batch = append(batch, Sample{At: time.Unix(int64(i)*60, 0), Value: 1})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for k := 0; k < b.N; k++ {
				a := New(time.Minute)
				if err := a.Add(time.Unix(0, 0), 1); err != nil {
					b.Fatal(err)
				}
				src := New(time.Minute)
				if err := src.Add(time.Unix(int64(n+1)*60, 0), 1); err != nil {
					b.Fatal(err)
				}
				if err := a.Merge(src); err != nil {
					b.Fatal(err)
				}
				if err := a.AddBatch(batch); err != nil {
					b.Fatal(err)
				}
				if got := len(a.Windows()); got != n+2 {
					b.Fatalf("got %d windows, want %d", got, n+2)
				}
			}
		})
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
