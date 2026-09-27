package rollup

import (
	"math"
	"testing"
	"time"
)

// TestRollupFactorOverflow checks that a factor whose product with the
// window width overflows the nanosecond range panics with the same message
// as an illegal factor, instead of silently wrapping to a wrong width.
func TestRollupFactorOverflow(t *testing.T) {
	cases := []struct {
		name   string
		window time.Duration
		factor int
	}{
		{"hour windows at max factor", time.Hour, math.MaxInt64},
		{"product just past the limit", 2 * time.Nanosecond, math.MaxInt64/2 + 1},
		{"microsecond windows past the limit", time.Microsecond, math.MaxInt64/int(time.Microsecond) + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := New(tc.window)
			if err := r.Add(time.Unix(0, 0), 1); err != nil {
				t.Fatal(err)
			}
			func() {
				defer func() {
					if got := recover(); got != "rollup: bad factor" {
						t.Fatalf("factor %d: panic = %v, want %q", tc.factor, got, "rollup: bad factor")
					}
				}()
				r.Rollup(tc.factor)
				t.Fatalf("factor %d did not panic", tc.factor)
			}()
			// The panic changed nothing.
			if got := r.Windows(); len(got) != 1 {
				t.Fatalf("overflow panic mutated the roller: %v", got)
			}
		})
	}
}

// TestRollupFactorAtLimit checks that the largest factor whose product
// still fits is accepted and behaves like a window-for-window view while
// the roller is empty of fine windows past the single one.
func TestRollupFactorAtLimit(t *testing.T) {
	r := New(time.Second)
	factor := math.MaxInt64 / int64(time.Second/time.Nanosecond)
	v := r.Rollup(int(factor))
	if v == nil {
		t.Fatal("Rollup returned nil at the in-range factor limit")
	}
}
