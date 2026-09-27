package rollup

import (
	"math"
	"math/big"
	"sync"
)

// stats holds the exact bookkeeping behind one Window's Sum, Min, and Max.
// The Window values stay populated on every write and hold the latest
// canonical result; stats keeps what the float64 values alone cannot:
//
//   - sum is the exact real sum of every finite sample filed into the
//     window while its lineage is exact, as one integer at a fixed binary
//     scale: the sum equals sum*2^scale. Every finite float64 is an
//     integer multiple of a power of two, so the representation is exact;
//     there is no denominator and no reduction. Float64 addition rounds
//     after every step and therefore depends on submission order, while
//     the exact integer does not, so it is rounded to float64 once, at
//     write time, and every interleaving of the same sample set rounds to
//     the same bits. The integer is kept even while the current set
//     rounds to an infinity, because later samples can bring the exact
//     sum back into the finite range.
//   - negZero/posZero/nonZero record the kinds of values the window
//     holds, so the sign of a zero sum and of zero extrema is fixed by
//     the sample set rather than by arithmetic residue.
//   - nan/infPlus/infMinus are sticky IEEE states entered only by merging
//     already-rounded window sums (+Inf and -Inf combine to NaN, an
//     infinity stays an infinity once finite values are folded in). A
//     window grown purely through Add never enters them: its overflow is
//     read off the exact integer and remains recoverable.
//
// Windows built directly in the package's tests carry no stats, so a
// virgin stats seeded from such a window absorbs its stored Sum on first
// touch.
type stats struct {
	sum         *big.Int
	scale       int
	initialized bool
	negZero     bool // the window holds at least one -0 sample
	posZero     bool // the window holds at least one +0 sample
	nonZero     bool // the window holds at least one non-zero sample
	infMinus    bool // a merge fixed the sum at -Inf
	infPlus     bool // a merge fixed the sum at +Inf
	nan         bool // a merge combined opposite infinities to NaN
}

// negZeroVal is the negative zero float64.
var negZeroVal = math.Copysign(0, -1)

// intPool recycles the temporary integer each accumulate needs, so filing
// into a steady-state window allocates no scratch operand.
var intPool = sync.Pool{New: func() any { return new(big.Int) }}

// floatPool recycles the big.Float used only while rounding an exact sum.
var floatPool = sync.Pool{New: func() any { return new(big.Float) }}

// seedStats returns bookkeeping for a window freshly created from one
// finite sample.
func seedStats(value float64) stats {
	var st stats
	st.initialized = true
	st.accumulate(value)
	return st
}

// decompose returns the finite float64 value as an odd integer mantissa m
// and exponent e with value = m*2^e, along with the sign. Zero is not
// handled here.
func decompose(value float64) (m uint64, e int, neg bool) {
	bits := math.Float64bits(value)
	neg = bits>>63 == 1
	expb := int((bits >> 52) & 0x7ff)
	mant := bits & (1<<52 - 1)
	switch expb {
	case 0:
		// Subnormal: value = mant*2^-1074. Normalize to an odd m.
		m = mant
		e = -1074
		for m != 0 && m&1 == 0 {
			m >>= 1
			e++
		}
	default:
		m = mant | 1<<52
		e = expb - 1023 - 52
	}
	return m, e, neg
}

// accumulate folds one finite sample into the exact sum. A sticky
// non-finite state from a merge is unchanged: an infinity stays infinite
// and NaN stays NaN when a finite value is added.
func (st *stats) accumulate(value float64) {
	if value == 0 {
		if math.Signbit(value) {
			st.negZero = true
		} else {
			st.posZero = true
		}
		return
	}
	st.nonZero = true
	if st.infMinus || st.infPlus || st.nan {
		return
	}
	mu, e, neg := decompose(value)
	if st.sum == nil {
		st.sum = new(big.Int).SetUint64(mu)
		if neg {
			st.sum.Neg(st.sum)
		}
		st.scale = e
		return
	}
	tmp := intPool.Get().(*big.Int).SetUint64(mu)
	if neg {
		tmp.Neg(tmp)
	}
	if e > st.scale {
		tmp.Lsh(tmp, uint(e-st.scale))
	} else if e < st.scale {
		st.sum.Lsh(st.sum, uint(st.scale-e))
		st.scale = e
	}
	st.sum.Add(st.sum, tmp)
	intPool.Put(tmp)
}

// roundedSum rounds the exact integer sum to float64.
func (st *stats) roundedSum() float64 {
	f := floatPool.Get().(*big.Float)
	f.SetInt(st.sum)
	f.SetMantExp(f, st.scale)
	v, _ := f.Float64()
	floatPool.Put(f)
	return v
}

// absorb seeds bookkeeping from a Window assembled without stats (the
// package's tests build such windows directly). It is called only on a
// virgin stats.
func (st *stats) absorb(w Window) {
	st.initialized = true
	switch {
	case math.IsNaN(w.Sum):
		st.nan = true
	case math.IsInf(w.Sum, 1):
		st.infPlus = true
	case math.IsInf(w.Sum, -1):
		st.infMinus = true
	case w.Sum != 0:
		st.nonZero = true
		mu, e, neg := decompose(w.Sum)
		st.sum = new(big.Int).SetUint64(mu)
		if neg {
			st.sum.Neg(st.sum)
		}
		st.scale = e
	}
	if w.Min == 0 && math.Signbit(w.Min) {
		st.negZero = true
	}
	if w.Max == 0 && !math.Signbit(w.Max) {
		st.posZero = true
	}
}

// kind classifies a sum state for IEEE-style combination by a merge.
type sumKind int

const (
	kindFinite sumKind = iota
	kindPlusInf
	kindMinusInf
	kindNaN
)

// kindOf reports how the sum currently rounds. A window with no non-zero
// samples is a finite zero. Overflow is classified by the exact integer's
// magnitude, so the common finite case never rounds through big.Float.
func (st *stats) kindOf() sumKind {
	switch {
	case st.nan:
		return kindNaN
	case st.infPlus:
		return kindPlusInf
	case st.infMinus:
		return kindMinusInf
	case st.sum == nil:
		return kindFinite
	}
	// |sum| lies in [2^(B-1), 2^B), so its value spans
	// [2^(B-1+scale), 2^(B+scale)). Values at or above 2^1024 round to
	// infinity; below that band they round finite (possibly to zero).
	hi := st.sum.BitLen() - 1 + st.scale
	if hi >= 1024 {
		if st.sum.Sign() < 0 {
			return kindMinusInf
		}
		return kindPlusInf
	}
	return kindFinite
}

// combine folds other's bookkeeping into st, as merging two windows over
// the same start does. When both windows' sums are still finite, the
// union stays exact: merging finite rollers reads the same as filing all
// their samples into one window. Once either side's sum has overflowed to
// a rounded +/-Inf, the two rounded sums combine by the IEEE rules
// (+Inf with -Inf is NaN, an infinity absorbs a finite value), and that
// result sticks. A zero-only window carries the finite zero.
func (st *stats) combine(other *stats) {
	if !other.initialized {
		return
	}
	if !st.initialized {
		*st = *other
		if other.sum != nil {
			st.sum = new(big.Int).Set(other.sum)
		}
		return
	}
	st.negZero = st.negZero || other.negZero
	st.posZero = st.posZero || other.posZero
	st.nonZero = st.nonZero || other.nonZero

	ka, kb := st.kindOf(), other.kindOf()
	switch {
	case ka == kindNaN || kb == kindNaN:
		st.setSticky(kindNaN)
	case ka == kindPlusInf && kb == kindMinusInf, ka == kindMinusInf && kb == kindPlusInf:
		st.setSticky(kindNaN)
	case ka == kindPlusInf || kb == kindPlusInf:
		st.setSticky(kindPlusInf)
	case ka == kindMinusInf || kb == kindMinusInf:
		st.setSticky(kindMinusInf)
	default:
		// Both sides finite: keep the union exact at the finer scale.
		st.infMinus, st.infPlus, st.nan = false, false, false
		switch {
		case st.sum != nil && other.sum != nil:
			target := st.scale
			if other.scale < target {
				target = other.scale
			}
			tmp := intPool.Get().(*big.Int)
			if other.scale == target {
				tmp.Set(other.sum)
			} else {
				tmp.Lsh(other.sum, uint(other.scale-target))
			}
			if st.scale != target {
				st.sum.Lsh(st.sum, uint(st.scale-target))
				st.scale = target
			}
			st.sum.Add(st.sum, tmp)
			intPool.Put(tmp)
		case other.sum != nil:
			st.sum = new(big.Int).Set(other.sum)
			st.scale = other.scale
		}
	}
}

// setSticky forces the named non-finite sticky state and drops the exact
// integer; kindFinite merely clears the sticky flags.
func (st *stats) setSticky(k sumKind) {
	switch k {
	case kindNaN:
		st.nan, st.infPlus, st.infMinus = true, false, false
		st.sum = nil
	case kindPlusInf:
		st.nan, st.infPlus, st.infMinus = false, true, false
		st.sum = nil
	case kindMinusInf:
		st.nan, st.infPlus, st.infMinus = false, false, true
		st.sum = nil
	default:
		st.nan, st.infPlus, st.infMinus = false, false, false
	}
}

// double folds a window's sum with itself, as merging a roller into
// itself does. An exact integer doubles exactly (it may round to
// infinity, which canonicalSum reports); a sticky non-finite sum follows
// the IEEE rules (+Inf doubled is +Inf, NaN doubled is NaN).
func (st *stats) double() {
	if st.sum != nil {
		st.sum.Lsh(st.sum, 1)
		return
	}
	switch st.stickyKind() {
	case kindPlusInf:
		st.setSticky(kindPlusInf)
	case kindMinusInf:
		st.setSticky(kindMinusInf)
	case kindNaN:
		st.setSticky(kindNaN)
	}
}

// stickyKind reports the forced non-finite state of a window with no
// exact integer, or kindFinite when it is merely zero-only.
func (st *stats) stickyKind() sumKind {
	switch {
	case st.nan:
		return kindNaN
	case st.infPlus:
		return kindPlusInf
	case st.infMinus:
		return kindMinusInf
	default:
		return kindFinite
	}
}

// canonicalSum rounds the exact sum to the float64 value the contract
// fixes: NaN from opposite infinities, +/-Inf from one-sided overflow, a
// negative zero only when every sample was a negative zero, and otherwise
// the nearest float64 to the exact sum. A nonzero exact sum that rounds
// to zero still comes out positive zero: only an all-negative-zero
// window may carry the sign.
func (st *stats) canonicalSum() float64 {
	switch {
	case st.nan:
		return math.NaN()
	case st.infPlus:
		return math.Inf(1)
	case st.infMinus:
		return math.Inf(-1)
	case !st.nonZero:
		if st.negZero && !st.posZero {
			return negZeroVal
		}
		return 0
	case st.sum == nil:
		return 0
	}
	v := st.roundedSum()
	if v == 0 {
		return 0
	}
	return v
}

// canonicalMin fixes a minimum that is zero: it is a negative zero when
// the window holds a negative-zero sample, and a positive zero otherwise.
func canonicalMin(v float64, negZero bool) float64 {
	if v == 0 && negZero {
		return negZeroVal
	}
	return v
}

// canonicalMax fixes a maximum that is zero: a positive zero wins
// whenever a positive-zero sample is present; with none, a negative zero
// keeps its sign. A strictly positive maximum keeps its value.
func canonicalMax(v float64, negZero, posZero bool) float64 {
	if v == 0 {
		switch {
		case posZero:
			return 0
		case negZero:
			return negZeroVal
		}
	}
	return v
}

// addTo folds one finite sample into w and its bookkeeping st. Count
// saturates at math.MaxInt64 instead of overflowing; the sum and extrema
// keep updating afterwards under their fixed semantics.
func addTo(w *Window, st *stats, value float64) {
	if w.Count < math.MaxInt64 {
		w.Count++
	}
	st.accumulate(value)
	if value < w.Min {
		w.Min = value
	}
	if value > w.Max {
		w.Max = value
	}
	w.Sum = st.canonicalSum()
	w.Min = canonicalMin(w.Min, st.negZero)
	w.Max = canonicalMax(w.Max, st.negZero, st.posZero)
}

// combineWindow folds src into dst as merging two windows over the same
// start does: counts saturate, the sum states combine, and the extrema
// take the smaller minimum and larger maximum under the zero-sign rules.
func combineWindow(dst *Window, dstSt *stats, src *Window, srcSt *stats) {
	dst.Count = satAdd(dst.Count, src.Count)
	dstSt.combine(srcSt)
	if src.Min < dst.Min {
		dst.Min = src.Min
	}
	if src.Max > dst.Max {
		dst.Max = src.Max
	}
	dst.Sum = dstSt.canonicalSum()
	dst.Min = canonicalMin(dst.Min, dstSt.negZero)
	dst.Max = canonicalMax(dst.Max, dstSt.negZero, dstSt.posZero)
}

// canonicalWindow applies the fixed sum and extrema semantics to w.
func canonicalWindow(w Window, st *stats) Window {
	if !st.initialized {
		return w
	}
	w.Sum = st.canonicalSum()
	w.Min = canonicalMin(w.Min, st.negZero)
	w.Max = canonicalMax(w.Max, st.negZero, st.posZero)
	return w
}
