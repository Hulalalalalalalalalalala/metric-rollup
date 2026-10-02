package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Hulalalalalalalalalalala/metric-rollup/rollup"
)

// runSeries streams one file's labeled samples into a bounded SeriesSet
// and prints one metric-rollup/series-report/v1 document. The whole file
// is ingested and the whole document marshaled before anything reaches
// stdout, so a bad line, a rejected sample, or an encoding failure leaves
// stdout empty.
func runSeries(path string, d time.Duration, maxSeries int, policy rollup.OverflowPolicy, stdout, stderr io.Writer) int {
	set, code := ingestSeriesFile(path, d, maxSeries, policy, stderr)
	if code != 0 {
		return code
	}
	doc, err := json.Marshal(buildSeriesReport(d, set))
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 1
	}
	doc = append(doc, '\n')
	if _, err := stdout.Write(doc); err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 1
	}
	return 0
}

// ingestSeriesFile opens one labeled input file by the same file rules as
// ingestFile and streams its three-field lines into a fresh SeriesSet.
func ingestSeriesFile(path string, d time.Duration, maxSeries int, policy rollup.OverflowPolicy, stderr io.Writer) (*rollup.SeriesSet, int) {
	f, code := openInput(path, stderr)
	if code != 0 {
		return nil, code
	}
	defer f.Close()
	set := rollup.NewSeriesSet(d, rollup.SeriesConfig{MaxSeries: maxSeries, Overflow: policy})
	if code := ingestSeriesInto(f, path, set, stderr); code != 0 {
		return nil, code
	}
	return set, 0
}

// ingestSeriesInto streams labeled records from in into set. On any
// content problem it returns code 4 after writing one diagnostic naming
// the file and line; the document is only printed by the caller on
// success, so stdout stays empty.
func ingestSeriesInto(in io.Reader, name string, set *rollup.SeriesSet, stderr io.Writer) int {
	contentError := func(line int, reason string) int {
		fmt.Fprintf(stderr, "rollupctl: %s:%d: %s\n", name, line, reason)
		return 4
	}
	missingFields := "expected a nanosecond timestamp, a value, and a JSON label object separated by whitespace"

	// The line buffer is capped at one byte over the limit, like
	// ingestInto, so a longer line fails immediately instead of growing
	// with the file.
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, maxLineBytes+1), maxLineBytes+1)

	line := 0
	samples := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if strings.TrimSpace(text) == "" {
			continue
		}
		// Split off timestamp and value from the front; the JSON label
		// object is the remainder and may contain whitespace of its own.
		rest := strings.TrimLeft(text, " \t")
		tsText, rest, found := cutField(rest)
		if !found {
			return contentError(line, missingFields)
		}
		valText, labelsText, found := cutField(rest)
		if !found || strings.TrimSpace(labelsText) == "" {
			return contentError(line, missingFields)
		}
		labelsText = strings.TrimSpace(labelsText)

		ns, err := strconv.ParseInt(tsText, 10, 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid timestamp %q", tsText))
		}
		value, err := strconv.ParseFloat(valText, 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid value %q", valText))
		}
		labels, ok := parseLabelsField(labelsText)
		if !ok {
			return contentError(line, fmt.Sprintf("invalid labels %q: expected a JSON object with string keys and values", labelsText))
		}
		if err := set.Add(rollup.LabeledSample{
			At:     time.Unix(0, ns).UTC(),
			Value:  value,
			Labels: labels,
		}); err != nil {
			return contentError(line, err.Error())
		}
		samples++
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			// The over-long line is the one after every line already
			// delivered, including an unterminated last line.
			return contentError(line+1, "line is longer than 1024 bytes")
		}
		fmt.Fprintf(stderr, "rollupctl: %s: %v\n", name, err)
		return 3
	}
	if samples == 0 {
		return contentError(0, "no samples found")
	}
	return 0
}

// cutField splits s at the first run of spaces or tabs, returning the
// leading field and the remainder with that run trimmed. It reports false
// when no field follows.
func cutField(s string) (field, rest string, found bool) {
	i := strings.IndexAny(s, " \t")
	if i < 0 {
		return s, "", false
	}
	return s[:i], strings.TrimLeft(s[i:], " \t"), true
}

// parseLabelsField decodes one JSON object of string labels. An empty
// object names the empty-label series; null, a non-object, or an object
// with non-string contents is rejected.
func parseLabelsField(s string) (map[string]string, bool) {
	var raw map[string]string
	if err := json.Unmarshal([]byte(s), &raw); err != nil || raw == nil {
		return nil, false
	}
	return raw, true
}

// Report types, field order fixed by the series-report/v1 contract.

type seriesReport struct {
	Schema      string              `json:"schema"`
	WindowNS    int64               `json:"window_ns"`
	Series      []seriesEntryReport `json:"series"`
	Cardinality cardinalityJSON     `json:"cardinality"`
}

type seriesEntryReport struct {
	Labels   map[string]string  `json:"labels"`
	Overflow bool               `json:"overflow"`
	Windows  []seriesWindowJSON `json:"windows"`
}

type seriesWindowJSON struct {
	StartNS json.RawMessage `json:"start_ns"`
	Count   int64           `json:"count"`
	Sum     float64         `json:"sum"`
	Min     float64         `json:"min"`
	Max     float64         `json:"max"`
}

type cardinalityJSON struct {
	AcceptedSeries  int   `json:"accepted_series"`
	OverflowSeries  int   `json:"overflow_series"`
	RejectedSamples int64 `json:"rejected_samples"`
}

// buildSeriesReport groups the set snapshot by series. Snapshot already
// lists slot series in first-seen order with all of one series' windows
// contiguous and the merged overflow bucket last, so the grouping is a
// single pass over it.
func buildSeriesReport(d time.Duration, set *rollup.SeriesSet) seriesReport {
	snap := set.Snapshot()
	entries := make([]seriesEntryReport, 0)
	for _, sw := range snap {
		if n := len(entries); n == 0 ||
			entries[n-1].Overflow != sw.Overflow ||
			!sameLabels(entries[n-1].Labels, sw.Labels) {
			entries = append(entries, seriesEntryReport{
				Labels:   sw.Labels,
				Overflow: sw.Overflow,
				Windows:  []seriesWindowJSON{},
			})
		}
		w := sw.Window
		entries[len(entries)-1].Windows = append(entries[len(entries)-1].Windows, seriesWindowJSON{
			// RawMessage keeps a window start outside the int64 range as
			// an exact JSON integer, signed, instead of clamping it.
			StartNS: json.RawMessage(unixNanoText(w.Start)),
			Count:   w.Count,
			Sum:     w.Sum,
			Min:     w.Min,
			Max:     w.Max,
		})
	}
	card := set.Cardinality()
	return seriesReport{
		Schema:   "metric-rollup/series-report/v1",
		WindowNS: d.Nanoseconds(),
		Series:   entries,
		Cardinality: cardinalityJSON{
			AcceptedSeries:  card.AcceptedSeries,
			OverflowSeries:  card.OverflowSeries,
			RejectedSamples: card.RejectedSamples,
		},
	}
}

// sameLabels reports whether two label maps hold the same keys and values.
func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
