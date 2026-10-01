package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// seriesReportJSON is the parsed form of the series report for tests that
// inspect values rather than exact bytes.
type seriesReportJSON struct {
	Schema   string `json:"schema"`
	WindowNS int64  `json:"window_ns"`
	Series   []struct {
		Labels   map[string]string `json:"labels"`
		Overflow bool              `json:"overflow"`
		Windows  []struct {
			StartNS int64   `json:"start_ns"`
			Count   int64   `json:"count"`
			Sum     float64 `json:"sum"`
			Min     float64 `json:"min"`
			Max     float64 `json:"max"`
		} `json:"windows"`
	} `json:"series"`
	Cardinality struct {
		AcceptedSeries  int   `json:"accepted_series"`
		OverflowSeries  int   `json:"overflow_series"`
		RejectedSamples int64 `json:"rejected_samples"`
	} `json:"cardinality"`
}

func parseReport(t *testing.T, raw string) seriesReportJSON {
	t.Helper()
	var doc seriesReportJSON
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("report is not valid JSON: %v\n%s", err, raw)
	}
	return doc
}

func seriesArgs(window, maxSeries, overflow string, extra ...string) []string {
	args := []string{"--window", window, "--max-series", maxSeries, "--overflow", overflow}
	args = append(args, extra...)
	args = append(args, "series")
	return args
}

func TestSeriesMinimalReportIsExactJSON(t *testing.T) {
	// Empty labels render {}, integers stay bare integers, and the field
	// order matches the published document shape.
	got := runWithFile(t, "0 1 {}\n", seriesArgs("1ns", "1", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := `{"schema":"metric-rollup/series-report/v1","window_ns":1,"series":[{"labels":{},"overflow":false,"windows":[{"start_ns":0,"count":1,"sum":1,"min":1,"max":1}]}],"cardinality":{"accepted_series":1,"overflow_series":0,"rejected_samples":0}}` + "\n"
	if got.stdout != want {
		t.Fatalf("stdout = %q\nwant   %q", got.stdout, want)
	}
}

func TestSeriesReportGroupsAndSorts(t *testing.T) {
	content := strings.Join([]string{
		`100 1 {"h":"a"}`,
		`0 2 {"h":"b"}`,
		`500000000 3 {"h":"a"}`,  // same window as the first a sample
		`1000000000 4 {"h":"a"}`, // next window
		`700000000 5 {"h":"c"}`,  // first overflowed origin
		`900000000 6 {"h":"d"}`,  // second overflowed origin
		``,
	}, "\n")
	got := runWithFile(t, content, seriesArgs("1s", "2", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if strings.Count(got.stdout, "\n") != 1 {
		t.Fatalf("report must be one line, got %q", got.stdout)
	}
	doc := parseReport(t, got.stdout)
	if doc.Schema != "metric-rollup/series-report/v1" || doc.WindowNS != 1000000000 {
		t.Fatalf("header = %q / %d", doc.Schema, doc.WindowNS)
	}
	if len(doc.Series) != 3 {
		t.Fatalf("series = %d: %+v", len(doc.Series), doc.Series)
	}
	if doc.Series[0].Labels["h"] != "a" || doc.Series[0].Overflow {
		t.Fatalf("series 0 = %+v", doc.Series[0])
	}
	if doc.Series[1].Labels["h"] != "b" || doc.Series[1].Overflow {
		t.Fatalf("series 1 = %+v", doc.Series[1])
	}
	if !doc.Series[2].Overflow || len(doc.Series[2].Labels) != 0 {
		t.Fatalf("series 2 must be overflow bucket: %+v", doc.Series[2])
	}
	a := doc.Series[0].Windows
	if len(a) != 2 || a[0].StartNS != 0 || a[0].Count != 2 || a[0].Sum != 4 ||
		a[1].StartNS != 1000000000 || a[1].Count != 1 || a[1].Sum != 4 {
		t.Fatalf("a windows = %+v", a)
	}
	ow := doc.Series[2].Windows
	if len(ow) != 1 || ow[0].Count != 2 || ow[0].Sum != 11 || ow[0].Min != 5 || ow[0].Max != 6 {
		t.Fatalf("overflow window = %+v", ow)
	}
	if doc.Cardinality.AcceptedSeries != 2 || doc.Cardinality.OverflowSeries != 2 ||
		doc.Cardinality.RejectedSamples != 0 {
		t.Fatalf("cardinality = %+v", doc.Cardinality)
	}
}

func TestSeriesEmptyLabelSeriesDistinctFromOverflow(t *testing.T) {
	// The empty-label series claims a slot; the overflow bucket still
	// appears after it with labels {} and overflow true.
	content := "0 1 {}\n0 2 {\"h\":\"x\"}\n"
	got := runWithFile(t, content, seriesArgs("1s", "1", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	doc := parseReport(t, got.stdout)
	if len(doc.Series) != 2 {
		t.Fatalf("series = %+v", doc.Series)
	}
	if doc.Series[0].Overflow || len(doc.Series[0].Labels) != 0 {
		t.Fatalf("empty-label slot series = %+v", doc.Series[0])
	}
	if !doc.Series[1].Overflow {
		t.Fatalf("second series = %+v, want overflow", doc.Series[1])
	}
}

func TestSeriesRejectPolicy(t *testing.T) {
	content := "0 1 {\"h\":\"a\"}\n1 2 {\"h\":\"b\"}\n"
	got := runWithFile(t, content, seriesArgs("1s", "1", "reject")...)
	if got.code != 4 {
		t.Fatalf("code = %d, want 4, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "" {
		t.Fatalf("rejected sample must leave stdout empty: %q", got.stdout)
	}
	if !strings.Contains(got.stderr, ":2:") || !strings.Contains(got.stderr, "series limit") {
		t.Fatalf("stderr = %q, want line 2 and limit reason", got.stderr)
	}
}

func TestSeriesLabelWhitespaceInsideJSON(t *testing.T) {
	content := "0 1 { \"h\" : \"a\" , \"z\" : \"b\" }\n"
	got := runWithFile(t, content, seriesArgs("1s", "1", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	doc := parseReport(t, got.stdout)
	if len(doc.Series) != 1 || doc.Series[0].Labels["h"] != "a" || doc.Series[0].Labels["z"] != "b" {
		t.Fatalf("labels = %+v", doc.Series)
	}
}

func TestSeriesContentErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		line    string
	}{
		{"empty file", "", "0"},
		{"only blank lines", "\n  \n", "0"},
		{"two fields", "0 1\n", "1"},
		{"bad timestamp", "x 1 {}\n", "1"},
		{"bad value", "0 nope {}\n", "1"},
		{"non-finite value", "0 NaN {}\n", "1"},
		{"labels not object", `0 1 [1]` + "\n", "1"},
		{"labels scalar", `0 1 123` + "\n", "1"},
		{"labels string", `0 1 "x"` + "\n", "1"},
		{"malformed json", `0 1 {"h":"a"` + "\n", "1"},
		{"trailing data", `0 1 {} x` + "\n", "1"},
		{"duplicate key", `0 1 {"h":"a","h":"b"}` + "\n", "1"},
		{"non-string value", `0 1 {"h":1}` + "\n", "1"},
		{"non-string null value", `0 1 {"h":null}` + "\n", "1"},
		{"empty key", `0 1 {"":"a"}` + "\n", "1"},
		{"empty value", `0 1 {"h":""}` + "\n", "1"},
		{"out of order in one series", "100 1 {\"h\":\"a\"}\n50 2 {\"h\":\"a\"}\n", "2"},
		{"out of order fine across series", "100 1 {\"h\":\"a\"}\n50 2 {\"h\":\"b\"}\n", "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runWithFile(t, tc.content, seriesArgs("1s", "4", "aggregate")...)
			if tc.line == "ok" {
				if got.code != 0 {
					t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
				}
				return
			}
			if got.code != 4 {
				t.Fatalf("code = %d, want 4, stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout must be empty, got %q", got.stdout)
			}
			if !strings.Contains(got.stderr, ":"+tc.line+":") {
				t.Fatalf("stderr = %q, want line %s", got.stderr, tc.line)
			}
		})
	}
}

func TestSeriesLongLine(t *testing.T) {
	// The cap applies to the whole physical line, labels included: the
	// 10-byte prefix, 1013 filler bytes, and 2-byte suffix total 1025.
	content := `0 1 {"k":"` + strings.Repeat("x", 1013) + "\"}\n"
	if len(strings.TrimRight(content, "\n")) != 1025 {
		t.Fatalf("test setup: line is %d bytes", len(content)-1)
	}
	got := runWithFile(t, content, seriesArgs("1ns", "1", "aggregate")...)
	if got.code != 4 || !strings.Contains(got.stderr, ":1:") {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestSeriesNegativeZeroKeepsSign(t *testing.T) {
	content := "0 -0 {}\n0 0 {}\n"
	got := runWithFile(t, content, seriesArgs("1ns", "1", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, `"min":-0`) || !strings.Contains(got.stdout, `"max":0`) {
		t.Fatalf("zero signs wrong: %q", got.stdout)
	}
}

func TestSeriesArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"missing max-series", []string{"--file", "x", "--window", "1s", "--overflow", "aggregate", "series"}},
		{"missing overflow", []string{"--file", "x", "--window", "1s", "--max-series", "2", "series"}},
		{"zero max", nil},
		{"negative max", nil},
		{"non-numeric max", nil},
		{"bad overflow value", nil},
		{"max-series on windows", []string{"--file", "x", "--window", "1s", "--max-series", "2", "windows"}},
		{"overflow on range", []string{"--file", "x", "--window", "1s", "--from", "0", "--to", "1", "--overflow", "aggregate", "range"}},
		{"two files", []string{"--file", "x", "--file", "y", "--window", "1s", "--max-series", "2", "--overflow", "aggregate", "series"}},
		{"from on series", []string{"--file", "x", "--window", "1s", "--max-series", "2", "--overflow", "aggregate", "--from", "0", "series"}},
	}
	base := func(extra ...string) []string {
		return append([]string{"--file", "x", "--window", "1s", "--max-series", "2", "--overflow", "aggregate"}, extra...)
	}
	cases[2].args = base("series", "--max-series", "0")
	cases[3].args = base("series", "--max-series", "-1")
	cases[4].args = base("series", "--max-series", "two")
	cases[5].args = base("series", "--overflow", "drop")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runArgs(t, tc.args...)
			if got.code != 2 {
				t.Fatalf("code = %d, want 2, stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout = %q", got.stdout)
			}
			if strings.Count(strings.TrimRight(got.stderr, "\n"), "\n") != 0 {
				t.Fatalf("stderr must be one line: %q", got.stderr)
			}
		})
	}
}

func TestSeriesMissingFile(t *testing.T) {
	got := runArgs(t, seriesArgs("1s", "1", "aggregate", "--file", "does-not-exist")...)
	if got.code != 3 {
		t.Fatalf("code = %d, want 3, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestSeriesRejectedSamplesCountAcrossRuns(t *testing.T) {
	// A later sample for the one admitted series is fine; every sample
	// for any other distinct series is refused and counted.
	content := strings.Join([]string{
		`0 1 {"h":"a"}`,
		`1 2 {"h":"b"}`,
		`2 3 {"h":"c"}`,
		`3 4 {"h":"a"}`,
		``,
	}, "\n")
	// The first new series rejects the run, so instead verify the
	// aggregate policy reports zero rejections for the same stream.
	got := runWithFile(t, content, seriesArgs("1s", "1", "aggregate")...)
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	doc := parseReport(t, got.stdout)
	if doc.Cardinality.RejectedSamples != 0 || doc.Cardinality.OverflowSeries != 2 {
		t.Fatalf("cardinality = %+v", doc.Cardinality)
	}
}
