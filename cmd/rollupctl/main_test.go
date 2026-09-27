package main

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// runResult captures one invocation of the command against a file on disk.
type runResult struct {
	code   int
	stdout string
	stderr string
}

func runWithFile(t *testing.T, content string, args ...string) runResult {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "samples.txt")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	} else {
		// An explicitly empty file is different from a missing path.
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	full := append([]string{"--file", path}, args...)
	var stdout, stderr bytes.Buffer
	code := run(full, &stdout, &stderr)
	return runResult{code, stdout.String(), stderr.String()}
}

func runArgs(t *testing.T, args ...string) runResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return runResult{code, stdout.String(), stderr.String()}
}

func TestRunAggregatesWindows(t *testing.T) {
	content := strings.Join([]string{
		"0 1",
		"500000000 2",   // same 1s window
		"1000000000 -3", // next window
		"   ",           // whitespace-only, skipped
		"",              // blank, skipped
		"1500000000 -0", // negative zero
		"2000000000 1.5",
	}, "\n") + "\n"
	got := runWithFile(t, content, "--window", "1s", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"0 2 3 1 2",
		"1000000000 2 -3 -3 -0",
		"2000000000 1 1.5 1.5 1.5",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestRunShortestFloatFormatting(t *testing.T) {
	// 1e20 renders "1e+20" under 'g' -1; the sum keeps whatever the
	// floating-point addition yields, still in its shortest form.
	content := "0 1e20\n0 150000\n"
	got := runWithFile(t, content, "--window", "1m", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "0 2 1.0000000000000015e+20 150000 1e+20\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestRunNegativeWindowStart(t *testing.T) {
	// -1.5s aligns down to -2s in a 2s window; -1s stays in [-2s, 0).
	content := "-1500000000 4\n-1000000000 8\n"
	got := runWithFile(t, content, "--window", "2s", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "-2000000000 2 12 4 8\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRunExtremeTimestamp(t *testing.T) {
	// MinInt64 aligned to a 3ns window starts at -9223372036854775809,
	// which is outside the int64 range; the start must print exactly.
	content := "-9223372036854775808 1\n"
	got := runWithFile(t, content, "--window", "3ns", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if !strings.HasPrefix(got.stdout, "-9223372036854775809 ") {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRunMaxTimestamp(t *testing.T) {
	content := "9223372036854775807 1\n"
	got := runWithFile(t, content, "--window", "1ns", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "9223372036854775807 1 1 1 1\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRunFlagsBeforeAndAfterSubcommand(t *testing.T) {
	content := "0 1\n"
	got := runWithFile(t, content, "windows", "--window=1s")
	if got.code != 0 {
		t.Fatalf("flags after subcommand: code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 1 1 1 1\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRunArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no args", nil},
		{"missing subcommand", []string{"--file", "x", "--window", "1s"}},
		{"unknown subcommand", []string{"--file", "x", "--window", "1s", "rollup"}},
		{"second positional", []string{"--file", "x", "--window", "1s", "windows", "extra"}},
		{"unknown flag", []string{"--file", "x", "--window", "1s", "windows", "--factor", "2"}},
		{"missing file", []string{"--window", "1s", "windows"}},
		{"missing window", []string{"--file", "x", "windows"}},
		{"empty file value", []string{"--file=", "--window", "1s", "windows"}},
		{"dangling value", []string{"--file", "x", "--window", "windows"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// These cases are all rejected while arguments are parsed,
			// before any file on disk is consulted.
			got := runArgs(t, tc.args...)
			if got.code != 2 {
				t.Fatalf("code = %d, want 2; stderr = %q", got.code, got.stderr)
			}
			if !strings.HasPrefix(got.stderr, "rollupctl: ") {
				t.Fatalf("stderr = %q, want rollupctl: prefix", got.stderr)
			}
			if strings.Count(strings.TrimRight(got.stderr, "\n"), "\n") != 0 {
				t.Fatalf("stderr must be one line, got %q", got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout must be empty, got %q", got.stdout)
			}
		})
	}
}

func TestRunBadWindowDuration(t *testing.T) {
	for _, value := range []string{"0s", "-1ns", "5", "not-a-duration", "1.5"} {
		t.Run(value, func(t *testing.T) {
			got := runWithFile(t, "0 1\n", "--window", value, "windows")
			if got.code != 2 {
				t.Fatalf("window %q: code = %d, stderr = %q", value, got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout = %q", got.stdout)
			}
		})
	}
}

func TestRunMissingFileAndDirectory(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	if got := runArgs(t, "--file", missing, "--window", "1s", "windows"); got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	if got := runArgs(t, "--file", dir, "--window", "1s", "windows"); got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestRunContentErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		line    string
	}{
		{"empty file", "", "0"},
		{"only blank lines", "\n  \n\t\n", "0"},
		{"bad timestamp", "0 1\nx 2\n", "2"},
		{"timestamp out of range", "9223372036854775808 1\n", "1"},
		{"bad value", "0 not-a-number\n", "1"},
		{"non-finite NaN", "0 NaN\n", "1"},
		{"non-finite infinity", "0 +Inf\n", "1"},
		{"one field", "0\n", "1"},
		{"three fields", "0 1 2\n", "1"},
		{"out of order", "100 1\n50 2\n", "2"},
		{"equal timestamp accepted", "100 1\n100 2\n", "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runWithFile(t, tc.content, "--window", "1ns", "windows")
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
				t.Fatalf("no output on content error, got %q", got.stdout)
			}
			if !strings.HasPrefix(got.stderr, "rollupctl: ") {
				t.Fatalf("stderr = %q", got.stderr)
			}
			if !strings.Contains(got.stderr, ":"+tc.line+":") {
				t.Fatalf("stderr = %q, want line %s", got.stderr, tc.line)
			}
		})
	}
}

func TestRunLongLine(t *testing.T) {
	t.Run("exactly 1024 accepted", func(t *testing.T) {
		// "0 " plus 1016 digits plus "e-1000" is 1024 bytes and parses to
		// a finite value.
		content := "0 " + strings.Repeat("9", 1016) + "e-1000\n"
		if len(content) != 1025 { // 1024 bytes plus the newline
			t.Fatalf("test setup: line is %d bytes", len(content)-1)
		}
		got := runWithFile(t, content, "--window", "1ns", "windows")
		if got.code != 0 {
			t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
		}
	})
	t.Run("1025 rejected", func(t *testing.T) {
		content := "0 " + strings.Repeat("9", 1017) + "e-1000\n"
		got := runWithFile(t, content, "--window", "1ns", "windows")
		if got.code != 4 {
			t.Fatalf("code = %d, want 4, stderr = %q", got.code, got.stderr)
		}
		if !strings.Contains(got.stderr, ":1:") {
			t.Fatalf("stderr = %q, want line 1", got.stderr)
		}
	})
	t.Run("1025 on later line", func(t *testing.T) {
		content := "0 1\n" + strings.Repeat("x", 1025) + "\n"
		got := runWithFile(t, content, "--window", "1ns", "windows")
		if got.code != 4 || !strings.Contains(got.stderr, ":2:") {
			t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
		}
	})
	t.Run("1025 unterminated last line", func(t *testing.T) {
		content := "0 1\n" + strings.Repeat("x", 1025)
		got := runWithFile(t, content, "--window", "1ns", "windows")
		if got.code != 4 || !strings.Contains(got.stderr, ":2:") {
			t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
		}
	})
}

func TestRunWhitespaceOnlyLongLine(t *testing.T) {
	// A whitespace-only line is normally skipped, but length is a property
	// of the physical line and is rejected first.
	content := strings.Repeat(" ", 1025) + "\n"
	got := runWithFile(t, content, "--window", "1ns", "windows")
	if got.code != 4 || !strings.Contains(got.stderr, ":1:") {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestRunLeadingTrailingAndMultipleWhitespace(t *testing.T) {
	content := "\t 100\t 2 \n  200   3\t\n"
	got := runWithFile(t, content, "--window", "100ns", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "100 1 2 2 2\n200 1 3 3 3\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRunOutOfOrderLeavesNoOutput(t *testing.T) {
	// Several valid windows followed by a bad line: nothing is printed and
	// no state survives the process anyway, but stdout must be empty.
	content := "0 1\n100 2\n50 3\n"
	got := runWithFile(t, content, "--window", "100ns", "windows")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
}

func TestRunSingleDashRejected(t *testing.T) {
	got := runWithFile(t, "0 1\n", "--window", "1ns", "-")
	if got.code != 2 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestRunEndOfFlags(t *testing.T) {
	// After -- the subcommand is positional, but flags before it still
	// apply.
	content := "0 1\n"
	got := runWithFile(t, content, "--window", "1s", "--", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestFormatFloatRoundTrip(t *testing.T) {
	// Every distinct float64 in a dense sweep round-trips through the
	// chosen formatting.
	for bits := uint64(0); bits < 200000; bits++ {
		v := math.Float64frombits(bits)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		s := formatFloat(v)
		back, err := strconv.ParseFloat(s, 64)
		if err != nil || back != v {
			t.Fatalf("formatFloat(%v) = %q, round-trips to %v, %v", v, s, back, err)
		}
	}
}

func TestUnixNanoText(t *testing.T) {
	cases := []struct {
		t    time.Time
		want string
	}{
		{time.Unix(0, 0).UTC(), "0"},
		{time.Unix(0, 1).UTC(), "1"},
		{time.Unix(0, -1).UTC(), "-1"},
		{time.Unix(-1, 999999999).UTC(), "-1"},
		{time.Unix(1, 0).UTC(), "1000000000"},
	}
	for _, tc := range cases {
		if got := unixNanoText(tc.t); got != tc.want {
			t.Fatalf("unixNanoText(%v) = %q, want %q", tc.t, got, tc.want)
		}
	}
}
