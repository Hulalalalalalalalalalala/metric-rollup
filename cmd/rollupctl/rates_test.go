package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRatesDerivesIncrementsAndResets(t *testing.T) {
	content := strings.Join([]string{
		"0 5",           // baseline only
		"500000000 10",  // +5 in window 0
		"1000000000 12", // +2 in window 1
		"1500000000 2",  // reset: +2 in window 1
		"2000000000 2",  // +0 in window 2
		"2500000000 9",  // +7 in window 2
	}, "\n") + "\n"
	files := map[string]string{"counter.txt": content}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "0", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"0 1 5 5 5 5",
		"1000000000 2 4 2 2 4",
		"2000000000 2 7 0 7 7",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestRatesBaselineOnlyPrintsNothing(t *testing.T) {
	files := map[string]string{"counter.txt": "0 5\n"}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "0", "--to", "1000000000")
	if got.code != 0 || got.stdout != "" {
		t.Fatalf("code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
}

func TestRatesRangeIsHalfOpen(t *testing.T) {
	files := map[string]string{"counter.txt": "0 0\n1000000000 1\n2000000000 2\n3000000000 3\n"}

	// [1s, 3s) keeps the windows starting at 1s and 2s; the window at
	// --to is excluded.
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "1000000000", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"1000000000 1 1 1 1 1",
		"2000000000 1 1 1 1 1",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	// A start inside a window selects the whole window containing it; the
	// 2s window also overlaps because it starts before the 2.5s end.
	got = runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "1200000000", "--to", "2500000000")
	if got.stdout != "1000000000 1 1 1 1 1\n2000000000 1 1 1 1 1\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestRatesEmptyResultsAreNotErrors(t *testing.T) {
	// Every increment lands in window 0, so ranges past it are empty even
	// though the counter is populated.
	files := map[string]string{"counter.txt": "0 0\n500000000 2\n"}
	for name, endpoints := range map[string][2]string{
		"from equals to":   {"1000000000", "1000000000"},
		"from after to":    {"2000000000", "1000000000"},
		"no window inside": {"2000000000", "3000000000"},
		"before first":     {"-3000000000", "-2000000000"},
	} {
		t.Run(name, func(t *testing.T) {
			got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
				"rates", "--from", endpoints[0], "--to", endpoints[1])
			if got.code != 0 {
				t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout = %q, want empty", got.stdout)
			}
		})
	}
}

func TestRatesCoversManyWindowsInBatches(t *testing.T) {
	// More windows than one cursor batch, each holding one increment.
	var lines []string
	for i := 0; i <= 3000; i++ {
		lines = append(lines, fmt.Sprintf("%d %d", int64(i)*1000000, i))
	}
	files := map[string]string{"counter.txt": strings.Join(lines, "\n") + "\n"}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1ms",
		"rates", "--from", "0", "--to", "3001000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	out := strings.Split(strings.TrimRight(got.stdout, "\n"), "\n")
	if len(out) != 3000 {
		t.Fatalf("got %d lines, want 3000", len(out))
	}
	// Baseline is the reading at 0; the increment at 1ms equals 1 and is
	// the first reported window; each 1ms window's rate is 1/0.001.
	if out[0] != "1000000 1 1 1 1 1000" {
		t.Fatalf("first = %q", out[0])
	}
	if out[2999] != "3000000000 1 1 1 1 1000" {
		t.Fatalf("last = %q", out[2999])
	}
}

func TestRatesNonIntegerSecondWindows(t *testing.T) {
	files := map[string]string{"counter.txt": "0 0\n1000000 5\n"}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "2500ms",
		"rates", "--from", "0", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 1 5 5 5 2\n" {
		t.Fatalf("stdout = %q, rate 5/2.5 = 2", got.stdout)
	}
}

func TestRatesArgumentErrors(t *testing.T) {
	files := map[string]string{"counter.txt": "0 1\n1000000000 2\n"}
	cases := []struct {
		name string
		args []string
	}{
		{"missing from", []string{"--file", "counter.txt", "--window", "1s", "rates", "--to", "1"}},
		{"missing to", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1"}},
		{"bad from", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "x", "--to", "1"}},
		{"bad to", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "1.5"}},
		{"from out of range", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "9223372036854775808", "--to", "1"}},
		{"extra positional", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "2", "extra"}},
		{"unknown flag", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "2", "--step", "3"}},
		{"two files", []string{"--file", "counter.txt", "--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "2"}},
		{"unknown subcommand", []string{"--file", "counter.txt", "--window", "1s", "rate", "--from", "1", "--to", "2"}},
		{"missing window", []string{"--file", "counter.txt", "rates", "--from", "1", "--to", "2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runWithFiles(t, files, tc.args...)
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

func TestRatesFileAndContentErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	got := runArgs(t, "--file", missing, "--window", "1s", "rates", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	got = runArgs(t, "--file", dir, "--window", "1s", "rates", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}

	for name, content := range map[string]string{
		"out of order":  "0 1\n100 2\n50 3\n",
		"negative":      "0 1\n100 -2\n",
		"nan":           "0 1\n100 NaN\n",
		"infinity":      "0 1\n100 +Inf\n",
		"bad value":     "0 1\n100 xyz\n",
		"bad timestamp": "x 1\n",
		"empty":         "",
		"blank only":    "\n  \n",
	} {
		t.Run(name, func(t *testing.T) {
			files := map[string]string{"bad.txt": content}
			got := runWithFiles(t, files, "--file", "bad.txt", "--window", "1s",
				"rates", "--from", "0", "--to", "100")
			if got.code != 4 {
				t.Fatalf("code = %d, want 4; stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout must be empty on failure, got %q", got.stdout)
			}
			if !strings.Contains(got.stderr, "rollupctl: ") {
				t.Fatalf("stderr = %q", got.stderr)
			}
		})
	}
}

func TestRatesWindowsAndMergeUnchanged(t *testing.T) {
	// The shared ingestion refactor must leave the other subcommands
	// working: a negative value is content for rollers too, and windows
	// output keeps its five columns.
	files := map[string]string{"samples.txt": "0 1\n500000000 2\n1000000000 -3\n"}
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s", "windows")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"0 2 3 1 2",
		"1000000000 1 -3 -3 -3",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "x.txt")
	if err := os.WriteFile(path, []byte("0 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got = runArgs(t, "--file", path, "--file", path, "--window", "1s", "merge")
	if got.code != 0 || got.stdout != "0 2 2 1 1\n" {
		t.Fatalf("merge: code = %d, stdout = %q", got.code, got.stdout)
	}
}
