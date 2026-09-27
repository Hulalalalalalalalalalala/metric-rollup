package rollup

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// negZero is the negative-zero float64.
var negZero = math.Copysign(0, -1)

func bitsEq(a, b float64) bool { return math.Float64bits(a) == math.Float64bits(b) }

// TestNonFiniteRejected covers the rule that NaN and either infinity are
// never filed: Add and AddBatch return ErrNonFinite, and the roller and
// its most-recent marker are left exactly as they were.
func TestNonFiniteRejected(t *testing.T) {
	r := New(time.Minute)
	if err := r.Add(time.Unix(100, 0), 1); err != nil {
		t.Fatal(err)
	}
	// Each rejected non-finite Add is followed by an accepted sample at
	// the marker, proving the rejection did not move it. accepted tracks
	// how many samples the window must hold.
	accepted := int64(1)
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000001)} {
		if err := r.Add(time.Unix(200, 0), v); !errors.Is(err, ErrNonFinite) {
			t.Fatalf("Add(%v): want ErrNonFinite, got %v", v, err)
		}
		if err := r.Add(time.Unix(100, 0), 2); err != nil {
			t.Fatalf("rejection moved the marker: %v", err)
		}
		accepted++
	}
	w, _ := r.Window(time.Unix(60, 0))
	if w.Count != accepted {
		t.Fatalf("rejected Add mutated state: %+v want count %d", w, accepted)
	}

	// A batch is rejected as a whole wherever the non-finite value sits,
	// and the marker does not move.
	for _, batch := range [][]Sample{
		{{At: time.Unix(200, 0), Value: math.NaN()}, {At: time.Unix(300, 0), Value: 1}},
		{{At: time.Unix(300, 0), Value: 1}, {At: time.Unix(200, 0), Value: math.Inf(1)}},
		{{At: time.Unix(50, 0), Value: math.Inf(-1)}}, // also older than the marker
	} {
		if err := r.AddBatch(batch); !errors.Is(err, ErrNonFinite) {
			t.Fatalf("AddBatch(%v): want ErrNonFinite, got %v", batch, err)
		}
		if err := r.Add(time.Unix(100, 0), 1); err != nil {
			t.Fatalf("rejected batch moved the marker: %v", err)
		}
		accepted++
		w, _ = r.Window(time.Unix(60, 0))
		if w.Count != accepted {
			t.Fatalf("rejected batch mutated state: %+v want count %d", w, accepted)
		}
	}
}

// TestSumIndependentOfOrder files one multiset through many submission
// histories - shuffled single Adds, one shuffled batch, and batches of
// random cuts - and requires sum, minimum, and maximum to come out bit for
// bit identical every time. All samples share one instant, so any order is
// accepted.
func TestSumIndependentOfOrder(t *testing.T) {
	values := []float64{
		1e20, -1e20, 1, 150000, 0.5, -0.25, 3, -7, 1e-300, negZero, 0,
		123456789.125, -987654321.5, 1e100, -1e100, 2.5,
	}
	at := time.Unix(0, 0)

	build := func(history func(r *Roller)) Window {
		r := New(time.Minute)
		history(r)
		w, _ := r.Window(at)
		return w
	}

	var ref Window
	for trial := 0; trial < 60; trial++ {
		rng := rand.New(rand.NewSource(int64(trial)))
		perm := rng.Perm(len(values))
		var got Window
		switch trial % 3 {
		case 0:
			got = build(func(r *Roller) {
				for _, i := range perm {
					if err := r.Add(at, values[i]); err != nil {
						t.Fatal(err)
					}
				}
			})
		case 1:
			batch := make([]Sample, len(values))
			for k, i := range perm {
				batch[k] = Sample{At: at, Value: values[i]}
			}
			got = build(func(r *Roller) {
				if err := r.AddBatch(batch); err != nil {
					t.Fatal(err)
				}
			})
		default:
			got = build(func(r *Roller) {
				for start := 0; start < len(perm); {
					n := 1 + rng.Intn(4)
					if start+n > len(perm) {
						n = len(perm) - start
					}
					batch := make([]Sample, n)
					for k := 0; k < n; k++ {
						batch[k] = Sample{At: at, Value: values[perm[start+k]]}
					}
					if err := r.AddBatch(batch); err != nil {
						t.Fatal(err)
					}
					start += n
				}
			})
		}
		if trial == 0 {
			ref = got
			continue
		}
		if got.Count != ref.Count ||
			!bitsEq(got.Sum, ref.Sum) ||
			!bitsEq(got.Min, ref.Min) ||
			!bitsEq(got.Max, ref.Max) {
			t.Fatalf("trial %d: %+v does not match reference %+v", trial, got, ref)
		}
	}
}

// TestZeroSigns pins the signed-zero rules for sum, minimum, and maximum
// against the sample set.
func TestZeroSigns(t *testing.T) {
	cases := []struct {
		name        string
		vals        []float64
		sum, mn, mx float64
	}{
		{"all negative zero", []float64{negZero, negZero}, negZero, negZero, negZero},
		{"one negative zero", []float64{negZero}, negZero, negZero, negZero},
		{"negative and positive zero", []float64{negZero, 0}, 0, negZero, 0},
		{"positive then negative zero", []float64{0, negZero}, 0, negZero, 0},
		{"all positive zero", []float64{0, 0}, 0, 0, 0},
		{"negative zero below positives", []float64{negZero, 1, 2}, 3, negZero, 2},
		{"negatives below negative zero", []float64{-1, -2, negZero}, -3, -2, negZero},
		{"negatives then positive zero", []float64{-1, 0}, -1, -1, 0},
		{"positive zero flips negative-zero max", []float64{negZero, negZero, 0}, 0, negZero, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := New(time.Minute)
			if err := r.AddBatch(toBatch(c.vals)); err != nil {
				t.Fatal(err)
			}
			w, _ := r.Window(time.Unix(0, 0))
			if !bitsEq(w.Sum, c.sum) || !bitsEq(w.Min, c.mn) || !bitsEq(w.Max, c.mx) {
				t.Fatalf("got sum %b min %b max %b; want sum %b min %b max %b",
					math.Float64bits(w.Sum), math.Float64bits(w.Min), math.Float64bits(w.Max),
					math.Float64bits(c.sum), math.Float64bits(c.mn), math.Float64bits(c.mx))
			}
		})
	}
}

// toBatch packs values into a same-instant batch.
func toBatch(vals []float64) []Sample {
	batch := make([]Sample, len(vals))
	for i, v := range vals {
		batch[i] = Sample{At: time.Unix(0, 0), Value: v}
	}
	return batch
}

// TestSumOverflowAndInfinity covers +/-Inf sums and the NaN produced when
// opposite infinities combine, including the distinction between a window
// grown purely through Add (exact, recoverable) and a merge (sticky).
func TestSumOverflowAndInfinity(t *testing.T) {
	// Two maxima overflow the positive range.
	r := New(time.Minute)
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	w, _ := r.Window(time.Unix(0, 0))
	if !math.IsInf(w.Sum, 1) {
		t.Fatalf("two MaxFloat64 sum = %v, want +Inf", w.Sum)
	}
	// Two minima overflow the negative range.
	n := New(time.Minute)
	n.Add(time.Unix(0, 0), -math.MaxFloat64)
	n.Add(time.Unix(0, 0), -math.MaxFloat64)
	wn, _ := n.Window(time.Unix(0, 0))
	if !math.IsInf(wn.Sum, -1) {
		t.Fatalf("two -MaxFloat64 sum = %v, want -Inf", wn.Sum)
	}

	// A window grown purely by Add keeps the exact set: a compensating
	// finite sample brings the set's sum back to a finite value.
	r.Add(time.Unix(0, 0), -math.MaxFloat64)
	w, _ = r.Window(time.Unix(0, 0))
	if w.Sum != math.MaxFloat64 {
		t.Fatalf("exact lineage should recover to MaxFloat64, got %v", w.Sum)
	}

	// Two windows whose sums are opposite infinities combine to NaN,
	// regardless of merge direction.
	infWin := func(sign int) *Roller {
		x := New(time.Minute)
		v := math.MaxFloat64
		if sign < 0 {
			v = -v
		}
		x.Add(time.Unix(0, 0), v)
		x.Add(time.Unix(0, 0), v)
		return x
	}
	a, b := infWin(1), infWin(-1)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	wa, _ := a.Window(time.Unix(0, 0))
	if !math.IsNaN(wa.Sum) {
		t.Fatalf("+Inf + -Inf = %v, want NaN", wa.Sum)
	}
	c, d := infWin(-1), infWin(1)
	if err := c.Merge(d); err != nil {
		t.Fatal(err)
	}
	wc, _ := c.Window(time.Unix(0, 0))
	if !math.IsNaN(wc.Sum) {
		t.Fatalf("-Inf + +Inf = %v, want NaN", wc.Sum)
	}

	// One-sided infinity stays infinite with a finite window folded in.
	p, q := infWin(1), New(time.Minute)
	q.Add(time.Unix(0, 0), -1e300)
	if err := p.Merge(q); err != nil {
		t.Fatal(err)
	}
	wp, _ := p.Window(time.Unix(0, 0))
	if !math.IsInf(wp.Sum, 1) {
		t.Fatalf("+Inf + finite = %v, want +Inf", wp.Sum)
	}
	// A finite sample filed afterwards cannot undo a sticky infinity.
	if err := p.Add(time.Unix(0, 0), -math.MaxFloat64); err != nil {
		t.Fatal(err)
	}
	wp, _ = p.Window(time.Unix(0, 0))
	if !math.IsInf(wp.Sum, 1) {
		t.Fatalf("+Inf with a later finite sample = %v, want +Inf", wp.Sum)
	}
	// NaN stays NaN with a finite sample.
	if err := a.Add(time.Unix(0, 0), 1); err != nil {
		t.Fatal(err)
	}
	wa, _ = a.Window(time.Unix(0, 0))
	if !math.IsNaN(wa.Sum) {
		t.Fatalf("NaN with a later finite sample = %v, want NaN", wa.Sum)
	}
}

// TestSelfMergeOverflow keeps an overflowed window infinite when the
// roller merges itself, while count and extrema keep their rules.
func TestSelfMergeOverflow(t *testing.T) {
	r := New(time.Minute)
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	if err := r.Merge(r); err != nil {
		t.Fatal(err)
	}
	w, _ := r.Window(time.Unix(0, 0))
	if !math.IsInf(w.Sum, 1) || w.Count != 4 {
		t.Fatalf("self-merge: %+v", w)
	}
}

// TestInfinityAndZeroMergeSigns checks that an all-negative-zero window
// and an opposite-infinity window can coexist through a merge.
func TestInfinityAndZeroMergeSigns(t *testing.T) {
	a := New(time.Minute)
	a.Add(time.Unix(0, 0), negZero)
	a.Add(time.Unix(60, 0), math.MaxFloat64)
	a.Add(time.Unix(60, 0), math.MaxFloat64)
	b := New(time.Minute)
	b.Add(time.Unix(0, 0), negZero)
	b.Add(time.Unix(60, 0), -math.MaxFloat64)
	b.Add(time.Unix(60, 0), -math.MaxFloat64)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	w0, _ := a.Window(time.Unix(0, 0))
	if !bitsEq(w0.Sum, negZero) || !bitsEq(w0.Min, negZero) || !bitsEq(w0.Max, negZero) {
		t.Fatalf("all-negative-zero window after merge: %+v", w0)
	}
	w1, _ := a.Window(time.Unix(60, 0))
	if !math.IsNaN(w1.Sum) {
		t.Fatalf("opposite infinities in window 60: %+v", w1)
	}
}

// TestViewCoarseInfinityAndZeros checks that a derived view combines
// covered windows with the same infinity and zero rules as merging.
func TestViewCoarseInfinityAndZeros(t *testing.T) {
	r := New(time.Second)
	// Two fine windows overflowing opposite ways inside one 3s coarse
	// window.
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	r.Add(time.Unix(0, 0), math.MaxFloat64)
	r.Add(time.Unix(1, 0), -math.MaxFloat64)
	r.Add(time.Unix(1, 0), -math.MaxFloat64)
	// An all-negative-zero window in the next coarse window.
	r.Add(time.Unix(3, 0), negZero)
	v := r.Rollup(3)
	w := v.Window(time.Unix(0, 0))
	if !math.IsNaN(w.Sum) {
		t.Fatalf("coarse opposite infinities: %+v", w)
	}
	z := v.Window(time.Unix(3, 0))
	if !bitsEq(z.Sum, negZero) || !bitsEq(z.Min, negZero) || !bitsEq(z.Max, negZero) {
		t.Fatalf("coarse all-negative-zero: %+v", z)
	}
}

// TestDeterminismUnderMerge checks that the same set of contributions
// reached through different merge histories yields identical window stats.
func TestDeterminismUnderMerge(t *testing.T) {
	sortedWindows := func(r *Roller) []Window {
		ws := r.Windows()
		sort.Slice(ws, func(i, j int) bool { return ws[i].Start.Before(ws[j].Start) })
		return ws
	}

	a1 := New(time.Minute)
	a1.Add(time.Unix(0, 0), 1e20)
	a1.Add(time.Unix(60, 0), -1e20)
	a1.Add(time.Unix(120, 0), 5)
	b1 := New(time.Minute)
	b1.Add(time.Unix(0, 0), 1)
	b1.Add(time.Unix(60, 0), 150000)
	a1.Merge(b1)
	first := sortedWindows(a1)

	a2 := New(time.Minute)
	a2.Add(time.Unix(120, 0), 5)
	b2 := New(time.Minute)
	b2.Add(time.Unix(0, 0), 1)
	b2.Add(time.Unix(60, 0), 150000)
	c2 := New(time.Minute)
	c2.Add(time.Unix(0, 0), 1e20)
	c2.Add(time.Unix(60, 0), -1e20)
	a2.Merge(b2)
	a2.Merge(c2)
	second := sortedWindows(a2)

	if len(first) != len(second) {
		t.Fatalf("window counts differ: %v vs %v", first, second)
	}
	for i := range first {
		if first[i].Start != second[i].Start || first[i].Count != second[i].Count ||
			!bitsEq(first[i].Sum, second[i].Sum) ||
			!bitsEq(first[i].Min, second[i].Min) ||
			!bitsEq(first[i].Max, second[i].Max) {
			t.Fatalf("window %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}

// TestExactSumFuzz drives random finite values - including subnormals,
// values near the overflow boundary, and signed zeros - through three
// histories and requires bit-identical statistics: single Adds in a
// shuffled order, one batch, and two rollers split by parity then merged.
func TestExactSumFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(20260927))
	randomVal := func() float64 {
		switch rng.Intn(8) {
		case 0:
			return math.Float64frombits(rng.Uint64() & 0x000fffffffffffff) // subnormal
		case 1:
			return math.Float64frombits(0x0010000000000000 | (rng.Uint64() & 0x000fffffffffffff)) // smallest normals
		case 2:
			return math.MaxFloat64 / math.Exp2(float64(rng.Intn(10)))
		case 3:
			return 0
		case 4:
			return negZero
		case 5:
			return math.Float64frombits(0x8000000000000000 | (rng.Uint64() & 0x000fffffffffffff)) // negative subnormal
		default:
			// Any finite normal: keep the exponent below 2047.
			b := rng.Uint64()
			b &^= 0x7ff0000000000000
			b |= uint64(rng.Intn(2046)+1) << 52
			return math.Float64frombits(b)
		}
	}
	at := time.Unix(0, 0)
	for iter := 0; iter < 300; iter++ {
		n := 1 + rng.Intn(40)
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = randomVal()
		}
		// History A: shuffled single adds.
		a := New(time.Minute)
		perm := rng.Perm(n)
		for _, i := range perm {
			if err := a.Add(at, vals[i]); err != nil {
				t.Fatal(err)
			}
		}
		wa, _ := a.Window(at)

		// History B: one batch, reverse order.
		b := New(time.Minute)
		batch := make([]Sample, n)
		for i := range vals {
			batch[i] = Sample{At: at, Value: vals[n-1-i]}
		}
		if err := b.AddBatch(batch); err != nil {
			t.Fatal(err)
		}
		wb, _ := b.Window(at)
		if !bitsEq(wa.Sum, wb.Sum) || !bitsEq(wa.Min, wb.Min) || !bitsEq(wa.Max, wb.Max) {
			t.Fatalf("iter %d: adds %+v vs batch %+v", iter, wa, wb)
		}

		// History C: split into two rollers, then merge. When either
		// part's sum has already overflowed, merging combines the rounded
		// window sums by the IEEE rules (pinned separately), so it need not
		// equal the exact all-samples result; only compare the finite case.
		c1 := New(time.Minute)
		c2 := New(time.Minute)
		for i, v := range vals {
			if i%2 == 0 {
				c1.Add(at, v)
			} else {
				c2.Add(at, v)
			}
		}
		s1, _ := c1.Window(at)
		s2, _ := c2.Window(at)
		if !math.IsInf(s1.Sum, 0) && !math.IsNaN(s1.Sum) &&
			!math.IsInf(s2.Sum, 0) && !math.IsNaN(s2.Sum) {
			if err := c1.Merge(c2); err != nil {
				t.Fatal(err)
			}
			wc, _ := c1.Window(at)
			if !bitsEq(wa.Sum, wc.Sum) || !bitsEq(wa.Min, wc.Min) || !bitsEq(wa.Max, wc.Max) {
				t.Fatalf("iter %d: adds %+v vs merged %+v (parts %v, %v)", iter, wa, wc, s1.Sum, s2.Sum)
			}
		}
	}
}

// TestMergeFinitePartsOverflow checks that two windows finite on their
// own combine to the same overflowed sum as filing every sample into one
// window, on both sides of the sign.
func TestMergeFinitePartsOverflow(t *testing.T) {
	at := time.Unix(0, 0)
	check := func(v float64, wantInfSign int) {
		whole := New(time.Minute)
		whole.Add(at, v)
		whole.Add(at, v)
		ww, _ := whole.Window(at)

		p := New(time.Minute)
		p.Add(at, v)
		q := New(time.Minute)
		q.Add(at, v)
		if err := p.Merge(q); err != nil {
			t.Fatal(err)
		}
		mw, _ := p.Window(at)
		if !bitsEq(ww.Sum, mw.Sum) {
			t.Fatalf("v=%v: whole sum %v vs merged %v", v, ww.Sum, mw.Sum)
		}
		if !math.IsInf(mw.Sum, wantInfSign) {
			t.Fatalf("v=%v: want infinity sign %d, got %v", v, wantInfSign, mw.Sum)
		}
	}
	check(math.MaxFloat64, 1)
	check(-math.MaxFloat64, -1)
}
