package rollup

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ErrInvalidLabel is returned by SeriesSet.Add and SeriesSet.AddBatch when
// a sample carries an empty label key or an empty label value. Such a
// sample is never filed: on Add the set is left unchanged, and on AddBatch
// the whole batch is rejected as one unit.
var ErrInvalidLabel = errors.New("rollup: invalid label")

// ErrSeriesLimit is returned, by a SeriesSet built with OverflowReject,
// when a sample belongs to a series the quota has no slot for: the slots
// are full and the sample is the first of a series not already known. The
// sample is not filed; on AddBatch the whole batch is rejected as one
// unit.
var ErrSeriesLimit = errors.New("rollup: series limit reached")

// OverflowPolicy says what happens to a sample whose series first appears
// once all maxSeries slots are taken.
type OverflowPolicy int

const (
	// OverflowAggregate files such samples into one shared overflow
	// bucket, which occupies no slot of its own; each originating series is
	// counted once in the cardinality report, at its first spill.
	OverflowAggregate OverflowPolicy = iota
	// OverflowReject refuses such samples with ErrSeriesLimit and counts
	// each refused sample in the cardinality report.
	OverflowReject
)

// SeriesConfig configures a SeriesSet: a positive slot quota and the
// policy for a series that first appears once the quota is full.
type SeriesConfig struct {
	MaxSeries int
	Overflow  OverflowPolicy
}

// LabeledSample is one record of a labeled stream: a value observed at one
// instant under a set of labels. Samples whose label maps hold the same
// keys and values belong to one series, whatever iteration order the maps
// happen to have; an empty label map names one series distinct from every
// labeled one and from the overflow bucket.
type LabeledSample struct {
	At     time.Time
	Value  float64
	Labels map[string]string
}

// SeriesWindow is one window of one series in a SeriesSet report. Labels
// is the series' label set, always non-nil and empty for both the
// empty-label series and the overflow bucket; Overflow distinguishes the
// bucket from that series. Window holds the same statistics a Roller
// reports for one window.
type SeriesWindow struct {
	Labels   map[string]string
	Overflow bool
	Window   Window
}

// CardinalityReport accounts for how the slot quota was spent.
type CardinalityReport struct {
	// AcceptedSeries counts series occupying a slot.
	AcceptedSeries int
	// OverflowSeries counts distinct series whose samples have spilled
	// into the overflow bucket; each is counted once, at its first spill.
	OverflowSeries int
	// RejectedSamples counts individual samples refused under
	// OverflowReject.
	RejectedSamples int64
}

// seriesEntry is one known series. A slot series owns its roller; a
// spilled series shares the set-wide bucket roller, so it keeps its own
// most-recent marker here: ordering is enforced per originating series,
// not across the merged bucket. labels belongs to the entry and is only
// stored for slot series; the bucket carries none.
type seriesEntry struct {
	roller *Roller
	labels map[string]string

	latest time.Time
	hasAny bool
}

// SeriesSet aggregates many labeled series into windows of one fixed size,
// bounding the number of series that occupy slots. Series claim slots in
// first-seen order; a series first appearing once the slots are full
// follows the overflow policy: its samples merge into one shared overflow
// bucket (OverflowAggregate) or are refused (OverflowReject). The empty
// label set names one ordinary series, distinct from the overflow bucket.
//
// Order within a series is exactly a Roller's: a sample earlier than that
// series' most recent accepted sample is rejected with ErrOutOfOrder and
// changes nothing, while samples of different series never constrain each
// other. Validation failures never change any state, and AddBatch files
// its samples as one all-or-nothing unit.
//
// The zero value is not usable; build one with NewSeriesSet. All methods
// are safe to call concurrently from multiple goroutines, and every read
// observes an internally consistent snapshot, exactly like a Roller.
type SeriesSet struct {
	// mu guards every field below. window, maxSlots, and policy are
	// fixed at construction and read without the lock. The lock is
	// always taken before any roller's lock, so the two never nest the
	// other way.
	mu       sync.RWMutex
	window   time.Duration
	maxSlots int
	policy   OverflowPolicy

	// order lists slot-occupying series in first-seen order; series maps
	// their canonical key to the entry. overflow lists spilled series in
	// first-spill order and spilled maps them; they all share bucket.
	order    []string
	series   map[string]*seriesEntry
	overflow []string
	spilled  map[string]*seriesEntry
	bucket   *Roller

	rejected int64
}

// NewSeriesSet builds a labeled series set. It panics with
// "rollup: bad series config" if maxSeries is not positive or overflow is
// neither OverflowAggregate nor OverflowReject, and with "rollup: bad
// window" if window is not positive.
func NewSeriesSet(window time.Duration, cfg SeriesConfig) *SeriesSet {
	if window <= 0 {
		panic("rollup: bad window")
	}
	if cfg.MaxSeries <= 0 || (cfg.Overflow != OverflowAggregate && cfg.Overflow != OverflowReject) {
		panic("rollup: bad series config")
	}
	return &SeriesSet{
		window:   window,
		maxSlots: cfg.MaxSeries,
		policy:   cfg.Overflow,
		series:   make(map[string]*seriesEntry),
	}
}

// validateLabels reports whether every label has a non-empty key and a
// non-empty value.
func validateLabels(labels map[string]string) bool {
	for k, v := range labels {
		if k == "" || v == "" {
			return false
		}
	}
	return true
}

// keyFor returns the canonical map key for a label set: label pairs sorted
// by key, each length-prefixed so no delimiter can alias one arrangement
// to another. Equal label maps always produce the same key.
func keyFor(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		v := labels[k]
		b = strconv.AppendInt(b, int64(len(k)), 10)
		b = append(b, ':')
		b = append(b, k...)
		b = append(b, '=')
		b = strconv.AppendInt(b, int64(len(v)), 10)
		b = append(b, ':')
		b = append(b, v...)
		b = append(b, ';')
	}
	return string(b)
}

// cloneLabels returns an independently owned copy of labels.
func cloneLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// precedesMarker reports whether at is older than the roller's most recent
// accepted sample.
func (r *Roller) precedesMarker(at time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hasAny && at.Before(r.latest)
}

// advanceMarker sets a spilled entry's marker to the later of its current
// value and newest. The caller must hold the set lock.
func (e *seriesEntry) advanceMarker(newest time.Time, hadAny bool) {
	if !hadAny || newest.After(e.latest) {
		e.latest = newest
	}
	e.hasAny = true
}

// Add files one labeled sample. Validation runs without touching state:
// an empty label key or value returns ErrInvalidLabel, a non-finite value
// (NaN or an infinity) returns ErrNonFinite, and a sample earlier than the
// most recent sample already accepted into its own series returns
// ErrOutOfOrder. Under OverflowReject a sample of a series with no slot
// returns ErrSeriesLimit and counts one rejected sample. In every
// rejection case the set is left unchanged; a sample at the same instant
// as its series marker is accepted and counted again.
//
// Concurrent Adds are serialized: each takes effect in the order it
// acquires the set, and per-series ordering is measured against the
// samples accepted before it in that order.
func (s *SeriesSet) Add(sample LabeledSample) error {
	if !validateLabels(sample.Labels) {
		return ErrInvalidLabel
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return ErrNonFinite
	}
	key := keyFor(sample.Labels)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.series[key]; ok {
		return e.roller.Add(sample.At, sample.Value)
	}
	if e, ok := s.spilled[key]; ok {
		if e.hasAny && sample.At.Before(e.latest) {
			return ErrOutOfOrder
		}
		// File through a private roller and merge into the shared bucket,
		// so the bucket's own marker never imposes cross-series order.
		scratch := New(s.window)
		if err := scratch.Add(sample.At, sample.Value); err != nil {
			return err
		}
		if err := s.bucket.Merge(scratch); err != nil {
			return err
		}
		e.advanceMarker(sample.At, e.hasAny)
		return nil
	}
	if len(s.order) < s.maxSlots {
		roller := New(s.window)
		if err := roller.Add(sample.At, sample.Value); err != nil {
			return err
		}
		s.series[key] = &seriesEntry{roller: roller, labels: cloneLabels(sample.Labels)}
		s.order = append(s.order, key)
		return nil
	}
	if s.policy == OverflowReject {
		s.rejected++
		return ErrSeriesLimit
	}
	scratch := New(s.window)
	if err := scratch.Add(sample.At, sample.Value); err != nil {
		return err
	}
	if s.bucket == nil {
		s.bucket = New(s.window)
	}
	if err := s.bucket.Merge(scratch); err != nil {
		return err
	}
	if s.spilled == nil {
		s.spilled = make(map[string]*seriesEntry)
	}
	e := &seriesEntry{roller: s.bucket, latest: sample.At, hasAny: true}
	s.spilled[key] = e
	s.overflow = append(s.overflow, key)
	return nil
}

// AddBatch files a batch of labeled samples as one atomic unit: either
// every sample takes effect or none does. The samples may arrive in any
// order and are not checked against each other; several samples at the
// same instant are each counted. Each sample is validated and filed into
// its series exactly as Add would file it.
//
// A batch holding any empty label key or value is rejected with
// ErrInvalidLabel, any non-finite value with ErrNonFinite, and any sample
// of a series with no slot under OverflowReject with ErrSeriesLimit. A
// sample earlier than the most recent sample already accepted into its
// series rejects the whole batch with ErrOutOfOrder. Any rejection leaves
// the set unchanged: no slot is claimed, no series counted as spilled, no
// rejection counted, and no window altered. A nil or empty batch is
// accepted and changes nothing.
//
// The batch is atomic with respect to every other call on the set, in the
// same sense as Roller.AddBatch: concurrent calls observe the set wholly
// before or wholly after it.
func (s *SeriesSet) AddBatch(samples []LabeledSample) error {
	if len(samples) == 0 {
		return nil
	}
	// Value and label validation scans the whole batch first, exactly as
	// Roller.AddBatch does; nothing is touched until the batch passes.
	for _, sm := range samples {
		if !validateLabels(sm.Labels) {
			return ErrInvalidLabel
		}
		if math.IsNaN(sm.Value) || math.IsInf(sm.Value, 0) {
			return ErrNonFinite
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Plan every destination without mutating the set. New slots are
	// provisionally counted, so two first-seen series in one batch split
	// slot and overflow fates correctly. Rejection and order checks fire
	// before any roller is written, leaving nothing to roll back.
	type plan struct {
		key    string
		live   *seriesEntry // existing destination, nil for a new series
		fresh  bool         // roller created by this batch
		spill  bool         // destination is the overflow bucket
		items  []Sample
		labels map[string]string
	}
	plans := make(map[string]*plan)
	order := make([]*plan, 0, len(samples))
	slots := len(s.order)
	for _, sm := range samples {
		key := keyFor(sm.Labels)
		p := plans[key]
		if p == nil {
			switch {
			case s.series[key] != nil:
				p = &plan{live: s.series[key]}
			case s.spilled[key] != nil:
				p = &plan{live: s.spilled[key], spill: true}
			case slots < s.maxSlots:
				p = &plan{fresh: true, labels: cloneLabels(sm.Labels)}
				slots++
			case s.policy == OverflowReject:
				// No state has been mutated: provisional plans are local.
				return ErrSeriesLimit
			default:
				p = &plan{fresh: true, spill: true}
			}
			p.key = key
			plans[key] = p
			order = append(order, p)
		}
		p.items = append(p.items, Sample{At: sm.At, Value: sm.Value})
	}

	// Out-of-order check against each existing series' marker. Samples of
	// different spilled series share one bucket but keep independent
	// markers, and samples of one batch are never checked against each
	// other.
	for _, p := range order {
		if p.live == nil {
			continue
		}
		if p.spill {
			for _, it := range p.items {
				if p.live.hasAny && it.At.Before(p.live.latest) {
					return ErrOutOfOrder
				}
			}
			continue
		}
		for _, it := range p.items {
			if p.live.roller.precedesMarker(it.At) {
				return ErrOutOfOrder
			}
		}
	}

	// All checks passed; the remaining operations cannot fail for these
	// finite samples at the same window size, so the commit needs no
	// undo path.
	for _, p := range order {
		switch {
		case !p.fresh && !p.spill:
			// Existing slot roller: AddBatch keeps the marker semantics
			// (Merge deliberately does not advance it).
			if err := p.live.roller.AddBatch(p.items); err != nil {
				return err
			}
		case p.fresh && !p.spill:
			roller := New(s.window)
			if err := roller.AddBatch(p.items); err != nil {
				return err
			}
			s.series[p.key] = &seriesEntry{roller: roller, labels: p.labels}
			s.order = append(s.order, p.key)
		default:
			// Overflow destination: collect this series' samples in a
			// private roller and merge them into the shared bucket, whose
			// own marker stays unused.
			scratch := New(s.window)
			if err := scratch.AddBatch(p.items); err != nil {
				return err
			}
			if s.bucket == nil {
				s.bucket = New(s.window)
			}
			if err := s.bucket.Merge(scratch); err != nil {
				return err
			}
			newest := p.items[0].At
			for _, it := range p.items[1:] {
				if it.At.After(newest) {
					newest = it.At
				}
			}
			if p.live != nil {
				p.live.advanceMarker(newest, p.live.hasAny)
			} else {
				if s.spilled == nil {
					s.spilled = make(map[string]*seriesEntry)
				}
				e := &seriesEntry{roller: s.bucket, latest: newest, hasAny: true}
				s.spilled[p.key] = e
				s.overflow = append(s.overflow, p.key)
			}
		}
	}
	return nil
}

// Snapshot returns every window of every series as one consistent
// snapshot: samples filed after the call do not alter it. Slot series are
// reported in the order their labels first appeared; the overflow bucket
// comes last. Windows within each series are in time order. The returned
// label maps and windows are copies the caller may keep; the overflow
// bucket's windows are reported once, merged across all spilled series.
func (s *SeriesSet) Snapshot() []SeriesWindow {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]SeriesWindow, 0)
	emit := func(labels map[string]string, overflow bool, r *Roller) {
		for _, w := range r.Windows() {
			out = append(out, SeriesWindow{
				Labels:   cloneLabels(labels),
				Overflow: overflow,
				Window:   w,
			})
		}
	}
	for _, key := range s.order {
		e := s.series[key]
		emit(e.labels, false, e.roller)
	}
	if s.bucket != nil {
		emit(map[string]string{}, true, s.bucket)
	}
	return out
}

// Cardinality returns a snapshot of the quota accounting: the number of
// slot-occupying series, the number of distinct series whose samples have
// spilled into the overflow bucket, and the number of samples refused
// under OverflowReject.
func (s *SeriesSet) Cardinality() CardinalityReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CardinalityReport{
		AcceptedSeries:  len(s.order),
		OverflowSeries:  len(s.overflow),
		RejectedSamples: s.rejected,
	}
}
