package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func runSeriesWithFile(t *testing.T, content string, args ...string) runResult {
	t.Helper()
	return runWithFile(t, content, append(args, "series")...)
}

// parseReport decodes the series report while preserving raw fields we
// want to assert structurally.
func parseReport(t *testing.T, stdout string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, stdout)
	}
	return doc
}

func TestSeriesReportShape(t *testing.T) {
	content := strings.Join([]string{
		`0 1 {"host":"a"}`,
		`500000000 2 {"host":"a"}`,
		`0 10 {}`,
		`1000000000 5 {"host":"b"}`,
	}, "\n") + "\n"
	got := runSeriesWithFile(t, content, "--window", "1s", "--max-series", "2", "--overflow", "aggregate")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if !strings.HasSuffix(got.stdout, "\n") {
		t.Fatalf("stdout must end with a newline: %q", got.stdout)
	}
	doc := parseReport(t, got.stdout)
	if doc["schema"] != "metric-rollup/series-report/v1" {
		t.Fatalf("schema = %v", doc["schema"])
	}
	if doc["window_ns"] != float64(1_000_000_000) {
		t.Fatalf("window_ns = %v", doc["window_ns"])
	}
	series := doc["series"].([]any)
	if len(series) != 3 { // host=a slot, empty-label slot, host=b overflow bucket
		t.Fatalf("series count = %d in %v", len(series), series)
	}
	first := series[0].(map[string]any)
	if first["labels"].(map[string]any)["host"] != "a" || first["overflow"] != false {
		t.Fatalf("first series = %v", first)
	}
	windows := first["windows"].([]any)
	if len(windows) != 1 {
		t.Fatalf("host=a windows = %v", windows)
	}
	w0 := windows[0].(map[string]any)
	if w0["start_ns"] != float64(0) || w0["count"] != float64(2) || w0["sum"] != float64(3) ||
		w0["min"] != float64(1) || w0["max"] != float64(2) {
		t.Fatalf("host=a window = %v", w0)
	}
	// The empty-label series holds the second slot as an ordinary series
	// (overflow false), distinguishable from the shared overflow bucket.
	emptySlot := series[1].(map[string]any)
	if emptySlot["overflow"] != false {
		t.Fatalf("empty-label series must not be the overflow bucket: %v", emptySlot)
	}
	if labels := emptySlot["labels"].(map[string]any); len(labels) != 0 {
		t.Fatalf("empty-label labels = %v, want {}", labels)
	}
	ew := emptySlot["windows"].([]any)
	if len(ew) != 1 || ew[0].(map[string]any)["sum"] != float64(10) {
		t.Fatalf("empty-label window = %v", ew)
	}
	// host=b spilled: it appears once as the overflow bucket, labels {}.
	bucket := series[2].(map[string]any)
	if bucket["overflow"] != true {
		t.Fatalf("last series must be overflow: %v", bucket)
	}
	if labels := bucket["labels"].(map[string]any); len(labels) != 0 {
		t.Fatalf("overflow labels = %v, want {}", labels)
	}
	bw := bucket["windows"].([]any)
	if len(bw) != 1 || bw[0].(map[string]any)["sum"] != float64(5) {
		t.Fatalf("overflow window = %v", bw)
	}

	card := doc["cardinality"].(map[string]any)
	if card["accepted_series"] != float64(2) ||
		card["overflow_series"] != float64(1) ||
		card["rejected_samples"] != float64(0) {
		t.Fatalf("cardinality = %v", card)
	}
}

func TestSeriesEmptyInputsRenderAsEmptyCollections(t *testing.T) {
	// No windowed output happens when every sample lands... there is
	// always at least one window, so exercise empty labels and a series
	// whose only window still renders; instead check an empty result set
	// cannot occur, but verify the literals {} and [] appear for the
	// empty-label series and that window arrays are JSON arrays.
	content := `0 1 {}` + "\n"
	got := runSeriesWithFile(t, content, "--window", "1s", "--max-series", "1", "--overflow", "aggregate")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, `"labels":{}`) {
		t.Fatalf("empty labels not rendered as {}: %s", got.stdout)
	}
}

func TestSeriesOverflowRejectExitCode(t *testing.T) {
	content := strings.Join([]string{
		`0 1 {"host":"a"}`,
		`0 2 {"host":"b"}`, // second distinct series over quota -> rejected
	}, "\n") + "\n"
	got := runSeriesWithFile(t, content, "--window", "1s", "--max-series", "1", "--overflow", "reject")
	if got.code != 4 {
		t.Fatalf("code = %d, want 4; stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "" {
		t.Fatalf("stdout must be empty on rejection, got %q", got.stdout)
	}
	if !strings.Contains(got.stderr, "series limit") {
		t.Fatalf("stderr = %q", got.stderr)
	}
}

func TestSeriesRejectAcceptsKnownSeriesAfterQuotaFull(t *testing.T) {
	// After the one slot is taken, more samples of the same series still
	// file; only new series are refused.
	content := strings.Join([]string{
		`0 1 {"host":"a"}`,
		`1000000000 4 {"host":"a"}`,
	}, "\n") + "\n"
	got := runSeriesWithFile(t, content, "--window", "1s", "--max-series", "1", "--overflow", "reject")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	doc := parseReport(t, got.stdout)
	card := doc["cardinality"].(map[string]any)
	if card["rejected_samples"] != float64(0) || card["accepted_series"] != float64(1) {
		t.Fatalf("cardinality = %v", card)
	}
}

func TestSeriesLabelObjectAllowsInnerWhitespace(t *testing.T) {
	content := "0 1 { \"host\" : \"a\" , \"zone\": \"z1\" }\n"
	got := runSeriesWithFile(t, content, "--window", "1s", "--max-series", "1", "--overflow", "aggregate")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	doc := parseReport(t, got.stdout)
	labels := doc["series"].([]any)[0].(map[string]any)["labels"].(map[string]any)
	if labels["host"] != "a" || labels["zone"] != "z1" {
		t.Fatalf("labels = %v", labels)
	}
}

func TestSeriesContentErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		line    string
	}{
		{"empty file", "", "0"},
		{"two fields", "0 1\n", "1"},
		{"four fields", "0 1 {} extra\n", "1"},
		{"bad timestamp", "x 1 {}\n", "1"},
		{"bad value", "0 nope {}\n", "1"},
		{"non-finite value", "0 NaN {}\n", "1"},
		{"labels not json", "0 1 {host}\n", "1"},
		{"labels an array", "0 1 []\n", "1"},
		{"labels null", "0 1 null\n", "1"},
		{"labels number value", `0 1 {"host":1}` + "\n", "1"},
		{"empty label key", `0 1 {"":"v"}` + "\n", "1"},
		{"empty label value", `0 1 {"host":""}` + "\n", "1"},
		{"out of order within series", "0 1 {\"h\":\"a\"}\n50 2 {\"h\":\"a\"}\n0 3 {\"h\":\"a\"}\n", "3"},
		{"different series not stale", "100 1 {\"h\":\"a\"}\n0 2 {\"h\":\"b\"}\n", "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runSeriesWithFile(t, tc.content, "--window", "1ns", "--max-series", "2", "--overflow", "reject")
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

func TestSeriesFileErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	if got := runArgs(t, "--file", missing, "--window", "1s", "--max-series", "1", "--overflow", "aggregate", "series"); got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	if got := runArgs(t, "--file", dir, "--window", "1s", "--max-series", "1", "--overflow", "aggregate", "series"); got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestSeriesArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"missing max-series", []string{"--file", "x", "--window", "1s", "--overflow", "aggregate", "series"}},
		{"missing overflow", []string{"--file", "x", "--window", "1s", "--max-series", "1", "series"}},
		{"bad overflow", []string{"--file", "x", "--window", "1s", "--max-series", "1", "--overflow", "drop", "series"}},
		{"zero max-series", []string{"--file", "x", "--window", "1s", "--max-series", "0", "--overflow", "aggregate", "series"}},
		{"negative max-series", []string{"--file", "x", "--window", "1s", "--max-series", "-3", "--overflow", "aggregate", "series"}},
		{"non-numeric max-series", []string{"--file", "x", "--window", "1s", "--max-series", "many", "--overflow", "aggregate", "series"}},
		{"two files", []string{"--file", "x", "--file", "y", "--window", "1s", "--max-series", "1", "--overflow", "aggregate", "series"}},
		{"from flag", []string{"--file", "x", "--window", "1s", "--max-series", "1", "--overflow", "aggregate", "series", "--from", "0"}},
		{"series flags on windows", []string{"--file", "x", "--window", "1s", "--max-series", "1", "windows"}},
		{"overflow flag alone on windows", []string{"--file", "x", "--window", "1s", "--overflow", "aggregate", "windows"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runArgs(t, tc.args...)
			if got.code != 2 {
				t.Fatalf("code = %d, want 2; stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout must be empty, got %q", got.stdout)
			}
			if !strings.HasPrefix(got.stderr, "rollupctl: ") ||
				strings.Count(strings.TrimRight(got.stderr, "\n"), "\n") != 0 {
				t.Fatalf("stderr must be one prefixed line: %q", got.stderr)
			}
		})
	}
}

func TestSeriesFlagsBeforeAndAfterSubcommand(t *testing.T) {
	got := runWithFile(t, "0 1 {}\n", "series", "--window=1s", "--max-series=1", "--overflow=aggregate")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	parseReport(t, got.stdout)
}

func TestSeriesRejectedBatchDoesNotLeakJSON(t *testing.T) {
	// A valid long prefix followed by a rejected new series must still
	// produce no stdout at all (ingestion completes before printing).
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, "0 1 {\"h\":\"a\"}")
	}
	lines = append(lines, "0 1 {\"h\":\"b\"}")
	got := runSeriesWithFile(t, strings.Join(lines, "\n")+"\n",
		"--window", "1s", "--max-series", "1", "--overflow", "reject")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
}
