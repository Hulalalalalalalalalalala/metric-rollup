// Exact, order-independent accumulation of the samples of one window.
//
// A float64 sum changes with the order of addition because every add
// rounds; to make a window's sum a function of its sample set alone, the
// positive and negative magnitudes of a window's finite samples are kept
// exactly as integer totals at powers of two, and rounded to float64 once
// when the window is read. Each finite float64 is mant*2^scale with an
// integer mant, so the totals stay exact at any sample count and through
// any merge, and the single final rounding is the same no matter the
// order samples or whole windows arrived in.

package rollup

import (
	"math"
	"math/big"
	"math/bits"
	"sync/atomic"
)

// overflowThreshold is the midpoint between math.MaxFloat64
// (2^1024 - 2^971) and 2^1024: an exact magnitude at or above it rounds
// to +Inf, and a smaller one rounds to MaxFloat64. It equals
// (2^54 - 1) * 2^970.
var overflowThreshold = func() *big.Int {
	t := new(big.Int).Lsh(big.NewInt(1), 54)
	t.Sub(t, big.NewInt(1))
	return t.Lsh(t, 970)
}()

// uncachedSentinel marks an accumulator whose finalized sum is stale. It
// is a quiet NaN bit pattern distinct from the canonical NaN a sum can
// take (math.NaN has payload bit 1), so it is never mistaken for a
// cached result.
const uncachedSentinel = 0x7ff8000000000002

// agg holds one window's exact totals, apart from the Window the roller
// stores. It is the mutable counterpart that Add and Merge update; reads
// finalize it into the Sum/Min/Max fields of a Window copy.
//
// cache holds the Float64bits of the finalized sum, recomputed only after
// a write. Reads run under the roller's read lock and may overlap, so the
// cache is updated atomically; a stale cache computes the same value.
type agg struct {
	// pos and neg are the exact sums of the magnitudes of the positive
	// and negative finite non-zero samples. posZero/negZero record whether
	// a positive/negative zero was among the samples, which decides the
	// sign of an exact-zero result and of zero extrema.
	pos, neg         exactMag
	posZero, negZero bool
	cache            atomic.Uint64
}

// newAgg returns an accumulator with no cached sum.
func newAgg() *agg {
	a := &agg{}
	a.cache.Store(uncachedSentinel)
	return a
}

// invalidate drops the cached finalized sum after a change.
func (a *agg) invalidate() {
	a.cache.Store(uncachedSentinel)
}

// add folds one finite value into a.
func (a *agg) add(v float64) {
	switch {
	case v > 0:
		mant, scale := frexpInt(v)
		a.pos.add(mant, scale)
	case v < 0:
		mant, scale := frexpInt(v)
		a.neg.add(mant, scale)
	case math.Signbit(v):
		a.negZero = true
	default:
		a.posZero = true
	}
	a.invalidate()
}

// double folds a window into itself, as a self-merge does.
func (a *agg) double() {
	a.pos.double()
	a.neg.double()
	a.invalidate()
}

// take replaces a with an independent deep copy of b. Unlike merge's
// copy-on-write adoption, the copy shares no coefficient with b, so it is
// used to carry another roller's source-only windows into the receiver.
func (a *agg) take(b *agg) {
	a.pos.setFrom(&b.pos)
	a.neg.setFrom(&b.neg)
	a.posZero, a.negZero = b.posZero, b.negZero
	a.invalidate()
}

// merge folds every sample recorded in b into a.
func (a *agg) merge(b *agg) {
	if b == nil {
		return
	}
	a.pos.merge(&b.pos)
	a.neg.merge(&b.neg)
	a.posZero = a.posZero || b.posZero
	a.negZero = a.negZero || b.negZero
	a.invalidate()
}

// seedAgg builds an agg from an already-aggregated Window, used when a
// window entered the roller through a test seam without one. A finite
// sum is imported as a single term; an imported infinity keeps its sticky
// sign and an imported NaN counts as both signs, matching how finalize
// would have read it. The extrema keep their stored signs, which is
// exact for the finite, non-negative-zero values the seams use.
func seedAgg(w Window) *agg {
	a := newAgg()
	switch {
	case math.IsNaN(w.Sum):
		a.pos.inf, a.neg.inf = true, true
	case math.IsInf(w.Sum, 1):
		a.pos.inf = true
	case math.IsInf(w.Sum, -1):
		a.neg.inf = true
	case w.Sum != 0:
		mant, scale := frexpInt(w.Sum)
		if w.Sum > 0 {
			a.pos.add(mant, scale)
		} else {
			a.neg.add(mant, scale)
		}
	}
	return a
}

// finalize stamps the exact sum and the fixed zero signs of the extrema
// into a copy of w.
func finalize(w Window, a *agg) Window {
	if a != nil {
		w.Sum = a.sum()
		if w.Min == 0 && a.negZero {
			w.Min = math.Copysign(0, -1)
		}
		if w.Max == 0 {
			if a.posZero {
				w.Max = 0
			} else if a.negZero {
				w.Max = math.Copysign(0, -1)
			}
		}
	}
	return w
}

// sum returns the window total under the fixed conventions:
//
//   - a positive or negative total past the float64 range is +Inf or -Inf,
//   - both sign totals out of range give NaN,
//   - an exact zero is negative zero only when every sample was a negative
//     zero, and positive zero otherwise,
//   - everything else is the exact difference of the two totals rounded
//     once to nearest, ties to even.
func (a *agg) sum() float64 {
	if c := a.cache.Load(); c != uncachedSentinel {
		return math.Float64frombits(c)
	}
	v := a.compute()
	a.cache.Store(math.Float64bits(v))
	return v
}

// compute applies the fixed conventions without touching the cache.
func (a *agg) compute() float64 {
	switch {
	case a.pos.inf && a.neg.inf:
		return math.NaN()
	case a.pos.inf:
		return math.Inf(1)
	case a.neg.inf:
		return math.Inf(-1)
	case !a.pos.has && !a.neg.has:
		return a.zero()
	}
	// Fast path: both sign totals stay in int64 at one scale. Align to
	// the smaller scale and subtract exactly; fall back to big integers
	// the moment an intermediate value does not fit.
	if pc, ps, pok := a.pos.intValue(); pok {
		if nc, ns, nok := a.neg.intValue(); nok {
			scale := ps
			if ns < scale {
				scale = ns
			}
			if p, ok := shlChecked(pc, uint(ps-scale)); ok {
				if n, ok := shlChecked(nc, uint(ns-scale)); ok {
					// p and n are non-negative, so p-n stays in int64.
					d := p - n
					if d == 0 {
						return a.zero()
					}
					if f, ok := intScaledFloat(d, scale); ok {
						return f
					}
				}
			}
		}
	}
	// Slow path: combine the two exact integer totals at the smaller
	// scale, so cancellation loses nothing before the single rounding.
	d := new(big.Int)
	scale := a.pos.scale
	if a.neg.has && (!a.pos.has || a.neg.scale < scale) {
		scale = a.neg.scale
	}
	if a.pos.has {
		d.Add(d, a.pos.bigAligned(scale))
	}
	if a.neg.has {
		d.Sub(d, a.neg.bigAligned(scale))
	}
	switch d.Sign() {
	case 0:
		return a.zero()
	case -1:
		return -roundMagnitude(new(big.Int).Neg(d), scale)
	default:
		return roundMagnitude(d, scale)
	}
}

// zero returns the signed zero for a window whose non-zero samples total
// exactly zero (or which holds only zeros): negative zero only when
// every sample was a negative zero, positive zero in every other case.
func (a *agg) zero() float64 {
	if a.negZero && !a.posZero && !a.pos.has && !a.neg.has {
		return math.Copysign(0, -1)
	}
	return 0
}

// exactMag is an exact sum of nonnegative magnitudes, each an integer
// mantissa times a power of two: the magnitude is coeff*2^scale, with
// scale the smallest term scale seen so far. The coefficient stays an
// int64 while it fits and only becomes a big integer after overflow, so
// the hot path allocates nothing. When owned is false, a non-nil big
// coefficient is shared with another roller and must be copied before a
// write; an int64 coefficient is copied along with the struct, so sharing
// costs nothing there.
type exactMag struct {
	i     int64
	scale int
	bigc  *big.Int
	has   bool
	owned bool
	inf   bool
}

// writable returns the big coefficient, copying a shared one first.
func (m *exactMag) writable() *big.Int {
	if m.bigc != nil && !m.owned {
		m.bigc = new(big.Int).Set(m.bigc)
		m.owned = true
	}
	return m.bigc
}

// promote moves an overflowing int64 coefficient into a big integer.
func (m *exactMag) promote() {
	m.bigc = big.NewInt(m.i)
	m.owned = true
}

// add adds mant*2^scale with mant > 0.
func (m *exactMag) add(mant, scale int) {
	if !m.has {
		m.i, m.scale, m.has, m.owned = int64(mant), scale, true, true
		m.refreshInf()
		return
	}
	if m.bigc == nil {
		if m.addInt(int64(mant), scale) {
			m.refreshInf()
			return
		}
		m.promote()
	}
	m.addBig(big.NewInt(int64(mant)), scale, true)
	m.refreshInf()
}

// refreshInf latches the sticky infinity flag once the exact magnitude
// rounds past the finite float64 range. A magnitude only grows, so the
// flag never clears. The leading-bit exponent decides almost every value
// with no allocation; only a magnitude within one power of two of the
// boundary is compared exactly.
func (m *exactMag) refreshInf() {
	if m.inf || !m.has {
		return
	}
	var lead int
	if m.bigc == nil {
		lead = bits.Len64(uint64(m.i)) - 1 + m.scale
	} else {
		lead = m.bigc.BitLen() - 1 + m.scale
	}
	switch {
	case lead >= 1024:
		m.inf = true
	case lead == 1023:
		m.inf = m.atLeastOverflowThreshold()
	}
}

// atLeastOverflowThreshold compares the exact magnitude to
// overflowThreshold; used only when its leading exponent is exactly 1023.
func (m *exactMag) atLeastOverflowThreshold() bool {
	c := m.bigc
	if c == nil {
		c = big.NewInt(m.i)
	}
	if m.scale >= 0 {
		return new(big.Int).Lsh(c, uint(m.scale)).Cmp(overflowThreshold) >= 0
	}
	return c.Cmp(new(big.Int).Lsh(overflowThreshold, uint(-m.scale))) >= 0
}

// addInt folds one term on the int64 fast path; the second result is
// false when an intermediate value overflows int64.
func (m *exactMag) addInt(mant int64, scale int) bool {
	return m.addIntTerm(mant, scale)
}

// addIntTerm aligns and adds mant*2^scale on the int64 fast path.
func (m *exactMag) addIntTerm(mant int64, scale int) bool {
	if scale < m.scale {
		v, ok := shlChecked(m.i, uint(m.scale-scale))
		if !ok {
			return false
		}
		m.i, m.scale, m.owned = v, scale, true
	} else if scale > m.scale {
		t, ok := shlChecked(mant, uint(scale-m.scale))
		if !ok {
			return false
		}
		mant = t
	}
	sum := m.i + mant
	if sum < m.i { // both positive; overflow wraps negative
		return false
	}
	m.i = sum
	return true
}

// addBig folds one term whose coefficient is c, optionally taking
// ownership of c.
func (m *exactMag) addBig(c *big.Int, scale int, ownC bool) {
	if scale < m.scale {
		m.writable().Lsh(m.bigc, uint(m.scale-scale))
		m.scale = scale
	}
	t := c
	if scale > m.scale {
		if ownC {
			t.Lsh(t, uint(scale-m.scale))
		} else {
			t = new(big.Int).Lsh(c, uint(scale-m.scale))
		}
	} else if !ownC {
		t = new(big.Int).Set(c)
	}
	m.writable().Add(m.bigc, t)
}

// double multiplies the exact magnitude by two.
func (m *exactMag) double() {
	if !m.has || m.inf {
		return
	}
	if m.bigc == nil {
		if v, ok := shlChecked(m.i, 1); ok {
			m.i = v
			m.refreshInf()
			return
		}
		m.promote()
	}
	m.writable().Lsh(m.bigc, 1)
	m.refreshInf()
}

// merge adds every term of o, which is never mutated.
func (m *exactMag) merge(o *exactMag) {
	if o.inf {
		m.inf = true
	}
	if !o.has {
		return
	}
	if !m.has {
		m.i, m.scale, m.bigc, m.has, m.inf = o.i, o.scale, o.bigc, true, o.inf
		// An adopted big coefficient is shared with o; both sides copy on
		// their next write. Both rollers are locked by the caller.
		m.owned = o.bigc == nil
		if o.bigc != nil {
			o.owned = false
		}
		return
	}
	if m.bigc == nil && o.bigc == nil {
		if m.addIntTerm(o.i, o.scale) {
			m.refreshInf()
			return
		}
		m.promote()
	}
	m.ensureBig()
	oc := o.bigc
	if oc == nil {
		oc = big.NewInt(o.i)
	}
	m.addBig(oc, o.scale, false)
	m.refreshInf()
}

// ensureBig promotes an int64-only magnitude before big-path work.
func (m *exactMag) ensureBig() {
	if m.bigc == nil {
		m.promote()
	}
}

// setFrom replaces m with an independent copy of o.
func (m *exactMag) setFrom(o *exactMag) {
	m.inf = o.inf
	m.has = o.has
	m.scale = o.scale
	m.i = o.i
	if o.bigc != nil {
		m.bigc = new(big.Int).Set(o.bigc)
		m.owned = true
	} else {
		m.bigc = nil
		m.owned = true
	}
}

// intValue returns the int64 coefficient and its scale when the magnitude
// never left the fast path; aligned callers shift from scale themselves.
func (m exactMag) intValue() (coeff int64, scale int, ok bool) {
	if !m.has || m.bigc != nil || m.inf {
		return 0, 0, false
	}
	return m.i, m.scale, true
}

// bigAligned returns the magnitude as a big integer at targetScale <=
// scale, without mutating m.
func (m *exactMag) bigAligned(targetScale int) *big.Int {
	if m.bigc != nil {
		t := new(big.Int).Set(m.bigc)
		if m.scale > targetScale {
			t.Lsh(t, uint(m.scale-targetScale))
		}
		return t
	}
	t := big.NewInt(m.i)
	if m.scale > targetScale {
		t.Lsh(t, uint(m.scale-targetScale))
	}
	return t
}

// shlChecked returns i<<n and false when it would overflow int64.
func shlChecked(i int64, n uint) (int64, bool) {
	if i < 0 || n >= 63 {
		if i == 0 {
			return 0, true
		}
		return 0, false
	}
	if i > (1<<(63-n))-1 {
		return 0, false
	}
	return i << n, true
}

// intScaledFloat returns coeff*2^scale rounded once. The coefficient
// converts to float64 exactly and scaling by a power of two is exact
// apart from the one underflow/overflow rounding Ldexp performs, so its
// result is the correctly rounded exact value.
func intScaledFloat(coeff int64, scale int) (float64, bool) {
	abs := coeff
	if abs < 0 {
		abs = -abs
		if abs < 0 { // MinInt64 negation does not fit
			return 0, false
		}
	}
	// Larger coefficients cannot convert to float64 exactly, so let the
	// big-integer path round the significand itself.
	if abs > 1<<53 {
		return 0, false
	}
	return math.Ldexp(float64(coeff), scale), true
}

// roundMagnitude rounds the positive integer magnitude t*2^scale to the
// nearest float64, ties to even. A magnitude at or above the overflow
// threshold returns +Inf; one below the smallest subnormal returns 0.
func roundMagnitude(t *big.Int, scale int) float64 {
	bl := t.BitLen() - 1
	lead := bl + scale // exponent of t's leading bit
	if lead >= 1024 {
		return math.Inf(1)
	}
	// Lowest bit of the retained significand: 53-bit precision in the
	// normal range, down to the subnormal grid at exponent -1074.
	grid := lead - 52
	if grid < -1074 {
		grid = -1074
	}
	drop := grid - scale
	var q *big.Int
	exp := scale
	if drop <= 0 {
		q = t
	} else {
		q = roundHalfEven(t, uint(drop))
		exp = grid
	}
	if q.Sign() == 0 {
		return 0
	}
	// A carry out of the significand at the top exponent reaches 2^1024,
	// which rounds to infinity.
	if q.BitLen() == 54 && lead == 1023 {
		return math.Inf(1)
	}
	return math.Ldexp(float64(q.Int64()), exp)
}

// roundHalfEven returns t rounded to a multiple of 2^s, ties to even.
func roundHalfEven(t *big.Int, s uint) *big.Int {
	mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), s), big.NewInt(1))
	low := new(big.Int).And(t, mask)
	q := new(big.Int).Rsh(t, s)
	half := new(big.Int).Lsh(big.NewInt(1), s-1)
	switch low.Cmp(half) {
	case 1:
		q.Add(q, big.NewInt(1))
	case 0:
		if q.Bit(0) != 0 {
			q.Add(q, big.NewInt(1))
		}
	}
	return q
}

// frexpInt decomposes a non-zero finite float64 into integers with
// v == mant*2^scale. It mirrors math.Frexp with an integer mantissa.
func frexpInt(v float64) (mant, scale int) {
	b := math.Float64bits(v)
	m := int64(b & (1<<52 - 1))
	if (b>>52)&0x7ff == 0 {
		// Subnormal: value is m*2^-1074 with m in [1, 2^52).
		return int(m), -1074
	}
	return int(m | 1<<52), int((b>>52)&0x7ff) - 1023 - 52
}
