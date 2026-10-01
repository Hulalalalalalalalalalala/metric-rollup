package rollup

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrInvalidLabel is returned by SeriesSet.Add and AddBatch when a label
// set contains an empty key or an empty value. Labels identify a series,
// so a sample carrying such a label is never filed: on Add the set is
// left unchanged, and on AddBatch the whole batch is rejected as one
// unit.
var ErrInvalidLabel = errors.New("rollup: invalid label")

// ErrSeriesLimit is returned when a sample names a series the set cannot
// admit under a reject overflow policy: all slots already belong to other
// series. On Add the sample is counted as a rejected sample without being
// filed; on AddBatch the whole batch is rejected as one unit and no
// rejection is counted.
var ErrSeriesLimit = errors.New("rollup: series limit reached")

// OverflowPolicy selects what a SeriesSet does with a sample that names a
// distinct series once every series slot is taken.
type OverflowPolicy int

const (
	// OverflowAggregate merges every series past the limit into one
	// bucket that holds no slot of its own.
	OverflowAggregate OverflowPolicy = iota + 1
	// OverflowReject refuses samples that name a series past the limit.
	OverflowReject
)

// LabeledSample is one record in a batch passed to SeriesSet.AddBatch: a
// value observed at one instant on the series its labels identify. The
// empty label set (a nil map or an empty one) is a legal series of its
// own, distinct from the overflow bucket.
type LabeledSample struct {
	At     time.Time
	Value  float64
	Labels map[string]string
}

// SeriesWindow is one reported series: the labels that identify it,
// whether it is the overflow bucket, and that series's window statistics
// in time order. Overflow is the only thing that distinguishes the
// overflow bucket from a real series carrying an empty label set.
type SeriesWindow struct {
	Labels   map[string]string
	Overflow bool
	Windows  []Window
}

// CardinalityReport counts how series capacity was used: AcceptedSeries
// is the number of real series holding a slot (distinct label sets seen
// while a slot was free), OverflowSeries is the number of distinct real
// series merged into the overflow bucket, and RejectedSamples is the
// number of individual samples refused because no slot was free under a
// reject policy.
type CardinalityReport struct {
	AcceptedSeries  int
	OverflowSeries  int
	RejectedSamples int64
}

// seriesState is one admitted series: its labels and the roller that
// files its samples.
type seriesState struct {
	labels map[string]string
	roller *Roller
}

// SeriesSet files labeled samples into per-series rollers that all share
// one window size, bounding how many distinct series it tracks.
//
// Two samples belong to the same series exactly when their label maps
// hold the same keys and values; the empty label set is a series of its
// own. Real series claim slots in first-seen order. Once every slot is
// taken, an aggregate policy merges samples of any further distinct
// series into one reported overflow bucket that holds no slot; each
// overflowed origin keeps its own roller and marker, so samples of
// different origins may interleave in time exactly as slotted series
// do, and the bucket is combined only when it is read. A reject policy
// refuses such samples instead.
//
// The zero value is not usable; build one with NewSeriesSet. All methods
// are safe to call concurrently from multiple goroutines, and every read
// observes one internally consistent snapshot, as with a Roller.
type SeriesSet struct {
	// mu guards every field below. window, maxSeries, and policy are
	// fixed at construction and read without the lock.
	//
	// order lists admitted real series in first-seen order and indexes
	// series. Overflowed origins are tracked the same way in
	// overflowOrder/overflow: each keeps its own roller, so one origin's
	// out-of-order marker never judges another origin's samples, and the
	// overflow bucket reported by reads is the merge of those rollers.
	mu            sync.RWMutex
	window        time.Duration
	maxSeries     int
	policy        OverflowPolicy
	order         []seriesKey
	series        map[seriesKey]*seriesState
	overflowOrder []seriesKey
	overflow      map[seriesKey]*seriesState
	rejected      int64
}

// seriesKey is the canonical identity of a label set: a length-prefixed
// rendering of its pairs in sorted key order. The prefixes make the
// encoding collision-free for any strings, separators included.
type seriesKey string

// keyOf renders one label set to its canonical key.
func keyOf(labels map[string]string) seriesKey {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v := labels[k]
		fmt.Fprintf(&b, "%d:%s|%d:%s;", len(k), k, len(v), v)
	}
	return seriesKey(b.String())
}

// copyLabels returns an independent non-nil copy of labels, so later
// mutation of the caller's map can neither merge two series nor alter a
// report.
func copyLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// validateLabels rejects an empty key or value. An empty or nil map is
// the legal empty label set.
func validateLabels(labels map[string]string) error {
	for k, v := range labels {
		if k == "" || v == "" {
			return ErrInvalidLabel
		}
	}
	return nil
}

// NewSeriesSet builds a set that files into windows of the given size and
// admits at most maxSeries distinct real series. It panics with
// "rollup: bad series config" if window is not positive, maxSeries is not
// positive, or overflow is neither OverflowAggregate nor OverflowReject.
func NewSeriesSet(window time.Duration, maxSeries int, overflow OverflowPolicy) *SeriesSet {
	if window <= 0 || maxSeries <= 0 ||
		(overflow != OverflowAggregate && overflow != OverflowReject) {
		panic("rollup: bad series config")
	}
	return &SeriesSet{
		window:    window,
		maxSeries: maxSeries,
		policy:    overflow,
		series:    make(map[seriesKey]*seriesState),
		overflow:  make(map[seriesKey]*seriesState),
	}
}

// newStateLocked builds the state for a series admitted just now.
func (s *SeriesSet) newStateLocked(labels map[string]string) *seriesState {
	return &seriesState{labels: copyLabels(labels), roller: New(s.window)}
}

// Add files one labeled sample into its series's window. It returns
// ErrInvalidLabel for an empty label key or value, ErrNonFinite for a
// NaN or infinity value, and ErrOutOfOrder when the sample predates the
// most recent sample already accepted for the same label set; a sample
// of a different series is never judged against that marker, including
// another series merged into the overflow bucket. Under a reject policy
// a sample naming a distinct series once every slot is taken is counted
// as a rejected sample and returns ErrSeriesLimit.
//
// Every rejection leaves the filed samples and the series roster
// unchanged; the rejected-sample counter only moves on an ErrSeriesLimit
// from Add itself. Concurrent Adds are serialized and take effect in the
// order they acquire the set.
func (s *SeriesSet) Add(sample LabeledSample) error {
	if err := validateLabels(sample.Labels); err != nil {
		return err
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return ErrNonFinite
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.routeLocked(keyOf(sample.Labels), sample.Labels)
	if target == nil {
		// Reject policy and no slot for this distinct series. Nothing is
		// filed, so the sample itself is the one rejection counted.
		s.rejected = satAdd(s.rejected, 1)
		return ErrSeriesLimit
	}
	return target.roller.Add(sample.At, sample.Value)
}

// routeLocked returns the roller that samples of an existing or freshly
// seen series go to, admitting the series as a slot holder or as a new
// overflow origin on first contact. A nil result means the reject policy
// refuses the series. The first sample of a newly created roller cannot
// fail, so registering it now never leaves an empty origin behind. The
// caller must hold mu.
func (s *SeriesSet) routeLocked(key seriesKey, labels map[string]string) *seriesState {
	if st, ok := s.series[key]; ok {
		return st
	}
	if st, ok := s.overflow[key]; ok {
		return st
	}
	st := s.newStateLocked(labels)
	if len(s.order) < s.maxSeries {
		s.series[key] = st
		s.order = append(s.order, key)
		return st
	}
	if s.policy == OverflowReject {
		return nil
	}
	s.overflow[key] = st
	s.overflowOrder = append(s.overflowOrder, key)
	return st
}

// AddBatch files a batch of labeled samples as one atomic unit: either
// every sample takes effect or none does. Samples within one label set
// may arrive in any order and are not checked against each other,
// exactly as Roller.AddBatch; different label sets, including different
// overflow origins, never constrain each other. New distinct series
// claim free slots in the order of their first appearance in the batch;
// under an aggregate policy the new series past the remaining slots all
// become overflow origins as one batch, and under a reject policy even
// one such series rejects the whole batch with ErrSeriesLimit, counting
// no rejection.
//
// A batch holding an invalid label returns ErrInvalidLabel, one holding a
// non-finite value ErrNonFinite, and one whose sample predates the most
// recent sample of the same label set ErrOutOfOrder. Any rejection
// leaves the set unchanged. A nil or empty batch is accepted, changes
// nothing, and does not advance any marker.
//
// The batch is atomic with respect to every other call on the set:
// concurrent readers and adds observe it wholly before or wholly after.
func (s *SeriesSet) AddBatch(samples []LabeledSample) error {
	if len(samples) == 0 {
		return nil
	}
	for _, sm := range samples {
		if err := validateLabels(sm.Labels); err != nil {
			return err
		}
	}
	for _, sm := range samples {
		if math.IsNaN(sm.Value) || math.IsInf(sm.Value, 0) {
			return ErrNonFinite
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Classify the distinct label sets the batch introduces, in the
	// order of their first appearance, before touching any state.
	newKeys := make([]seriesKey, 0)
	newLabels := make(map[seriesKey]map[string]string)
	plan := make([]seriesKey, len(samples))
	for i, sm := range samples {
		key := keyOf(sm.Labels)
		plan[i] = key
		if _, ok := s.series[key]; ok {
			continue
		}
		if _, ok := s.overflow[key]; ok {
			continue
		}
		if _, ok := newLabels[key]; ok {
			continue
		}
		newLabels[key] = copyLabels(sm.Labels)
		newKeys = append(newKeys, key)
	}

	free := s.maxSeries - len(s.order)
	if free < 0 {
		free = 0
	}
	admit := newKeys
	var overflowKeys []seriesKey
	if len(newKeys) > free {
		admit = newKeys[:free]
		overflowKeys = newKeys[free:]
		if s.policy == OverflowReject {
			// The batch never takes effect, so no rejection is counted.
			return ErrSeriesLimit
		}
	}

	// Roller states the batch introduces stay unregistered until every
	// check has passed; groups collect each target's samples in order.
	groups := make(map[*seriesState][]Sample)
	groupOrder := make([]*seriesState, 0)
	targetOf := make(map[seriesKey]*seriesState, len(newKeys))
	for _, key := range admit {
		targetOf[key] = s.newStateLocked(newLabels[key])
	}
	for _, key := range overflowKeys {
		targetOf[key] = s.newStateLocked(newLabels[key])
	}
	for i, sm := range samples {
		key := plan[i]
		target := s.series[key]
		if target == nil {
			target = s.overflow[key]
		}
		if target == nil {
			target = targetOf[key]
		}
		if _, ok := groups[target]; !ok {
			groupOrder = append(groupOrder, target)
		}
		groups[target] = append(groups[target], Sample{At: sm.At, Value: sm.Value})
	}

	// Out-of-order validation runs against the current markers of
	// rollers that already exist; rollers the batch creates have no
	// marker. Nothing has been filed yet, so a failure needs no rollback.
	for _, st := range groupOrder {
		r := st.roller
		if !r.hasAny {
			continue
		}
		for _, sm := range groups[st] {
			if sm.At.Before(r.latest) {
				return ErrOutOfOrder
			}
		}
	}

	// Commit the roster, then file each group.
	for _, key := range admit {
		st := targetOf[key]
		s.series[key] = st
		s.order = append(s.order, key)
	}
	for _, key := range overflowKeys {
		st := targetOf[key]
		s.overflow[key] = st
		s.overflowOrder = append(s.overflowOrder, key)
	}
	for _, st := range groupOrder {
		if err := st.roller.AddBatch(groups[st]); err != nil {
			// Unreachable after the validation above: values are finite
			// and the marker check already passed.
			panic(err)
		}
	}
	return nil
}

// Snapshot returns every series in first-seen order with the combined
// overflow bucket last, each with its windows in time order, together
// with the cardinality counts measured at the same instant. The result
// is a snapshot of one moment: samples filed after the call do not
// alter it.
func (s *SeriesSet) Snapshot() ([]SeriesWindow, CardinalityReport) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

// snapshotLocked builds the read result. The caller must hold mu, at
// least for reading; every label map and window slice in the result is a
// fresh copy.
func (s *SeriesSet) snapshotLocked() ([]SeriesWindow, CardinalityReport) {
	out := make([]SeriesWindow, 0, len(s.order)+1)
	for _, key := range s.order {
		st := s.series[key]
		out = append(out, SeriesWindow{
			Labels:  copyLabels(st.labels),
			Windows: st.roller.Windows(),
		})
	}
	if len(s.overflowOrder) > 0 {
		// Each overflow origin is its own roller; combine their windows
		// under the same sum and extrema semantics as Roller.Merge, so
		// one origin's marker never affected another origin's samples.
		merged := New(s.window)
		for _, key := range s.overflowOrder {
			if err := merged.Merge(s.overflow[key].roller); err != nil {
				// Same window size by construction.
				panic(err)
			}
		}
		out = append(out, SeriesWindow{
			Labels:   map[string]string{},
			Overflow: true,
			Windows:  merged.Windows(),
		})
	}
	report := CardinalityReport{
		AcceptedSeries:  len(s.order),
		OverflowSeries:  len(s.overflowOrder),
		RejectedSamples: s.rejected,
	}
	return out, report
}

// Windows returns every series in first-seen order with the overflow
// bucket last, each with its windows in time order. An empty set yields
// an empty slice. The result is a snapshot of one moment.
func (s *SeriesSet) Windows() []SeriesWindow {
	windows, _ := s.Snapshot()
	return windows
}

// Cardinality returns the slot, overflow, and rejection counts measured
// at one instant.
func (s *SeriesSet) Cardinality() CardinalityReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, report := s.snapshotLocked()
	return report
}
