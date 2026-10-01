package main

import (
	"strconv"
	"strings"
	"testing"
)

// TestRatesResetsAndSixColumns drives the counter reset logic through the
// full command: normal growth, a reset below the previous value, and equal
// timestamps, checking the six columns start, count, sum, min, max, rate.
func TestRatesResetsAndSixColumns(t *testing.T) {
	content := strings.Join([]string{
		"0 100",         // baseline
		"500000000 140", // +40 in window [0,1s)
		"1000000000 10", // reset: +10
		"1500000000 30", // +20
		"1500000000 30", // same ts: +0
		"1500000000 5",  // same ts reset: +5
	}, "\n") + "\n"
	files := map[string]string{"counter.txt": content}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "0", "--to", "2000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"0 1 40 40 40 40",
		"1000000000 4 35 0 20 35",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

// TestRatesWindowSeconds uses a non-integer-second window so the rate
// column visibly divides by the window width.
func TestRatesWindowSeconds(t *testing.T) {
	// 2.5s window; one increment of 5 gives rate 2.
	files := map[string]string{"counter.txt": "0 0\n1000000 5\n"}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "2500ms",
		"rates", "--from", "0", "--to", "2500000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 1 5 5 5 2\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

// TestRatesOnlyPopulatedWindows confirms windows holding no increment are
// not reported, and the half-open [from,to) window rule matches range.
func TestRatesOnlyPopulatedWindows(t *testing.T) {
	files := map[string]string{"counter.txt": strings.Join([]string{
		"0 1",          // baseline
		"100 2",        // +1 in window 0s
		"1000000000 3", // +1 in window 1s
		"3000000000 5", // +2 in window 3s; window 2s has no increment
	}, "\n") + "\n"}

	// Window 2s overlaps nothing populated: empty success, no output.
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "2000000000", "--to", "3000000000")
	if got.code != 0 || got.stdout != "" {
		t.Fatalf("empty range: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	// The window starting exactly at --to is excluded.
	got = runWithFiles(t, files, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "0", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := "0 1 1 1 1 1\n1000000000 1 1 1 1 1\n"
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	// Baseline alone files no increment anywhere.
	one := map[string]string{"counter.txt": "0 7\n"}
	got = runWithFiles(t, one, "--file", "counter.txt", "--window", "1s",
		"rates", "--from", "0", "--to", "9223372036854775807")
	if got.code != 0 || got.stdout != "" {
		t.Fatalf("baseline only: code = %d, stdout = %q", got.code, got.stdout)
	}
}

// TestRatesStreamsManyWindows exercises the RateCursor batching through
// the command, printing more windows than one batch.
func TestRatesStreamsManyWindows(t *testing.T) {
	var lines []string
	// A pre-range baseline so all 3000 loop samples file an increment, one
	// per millisecond window 0s..2999ms.
	lines = append(lines, "-1000000 0")
	for i := int64(0); i < 3000; i++ {
		// The cumulative value rises by one each millisecond, so every
		// window gets one +1 increment: rate is 1 over a 1ms window.
		lines = append(lines, strconv.FormatInt(i*1000000, 10)+" "+strconv.FormatInt(i+1, 10))
	}
	files := map[string]string{"counter.txt": strings.Join(lines, "\n") + "\n"}
	got := runWithFiles(t, files, "--file", "counter.txt", "--window", "1ms",
		"rates", "--from", "0", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	out := strings.Split(strings.TrimRight(got.stdout, "\n"), "\n")
	if len(out) != 3000 {
		t.Fatalf("got %d lines, want 3000", len(out))
	}
	if out[0] != "0 1 1 1 1 1000" {
		t.Fatalf("first = %q", out[0])
	}
	if out[2999] != "2999000000 1 1 1 1 1000" {
		t.Fatalf("last = %q", out[2999])
	}
}

// TestRatesArgumentErrors mirrors the range argument rules: rates takes
// one file and both endpoints.
func TestRatesArgumentErrors(t *testing.T) {
	files := map[string]string{"counter.txt": "0 1\n"}
	cases := []struct {
		name string
		args []string
	}{
		{"missing from", []string{"--file", "counter.txt", "--window", "1s", "rates", "--to", "1"}},
		{"missing to", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1"}},
		{"bad from", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "x", "--to", "1"}},
		{"two files", []string{"--file", "counter.txt", "--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "2"}},
		{"extra positional", []string{"--file", "counter.txt", "--window", "1s", "rates", "--from", "1", "--to", "2", "extra"}},
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

// TestRatesFileAndContentErrors checks the exit codes and the no-partial-
// stdout guarantee for counter inputs.
func TestRatesFileAndContentErrors(t *testing.T) {
	dir := t.TempDir()
	missing := dir + "/does-not-exist"
	got := runArgs(t, "--file", missing, "--window", "1s", "rates", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	got = runArgs(t, "--file", dir, "--window", "1s", "rates", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}

	// Out of order: exit 4, no output.
	files := map[string]string{"bad.txt": "0 1\n100 2\n50 3\n"}
	got = runWithFiles(t, files, "--file", "bad.txt", "--window", "1s", "rates", "--from", "0", "--to", "1000")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("out of order: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	// Empty file: exit 4 with the :0: marker, no output (baseline absent).
	files = map[string]string{"empty.txt": "\n  \n"}
	got = runWithFiles(t, files, "--file", "empty.txt", "--window", "1s", "rates", "--from", "0", "--to", "1")
	if got.code != 4 || !strings.Contains(got.stderr, ":0:") || got.stdout != "" {
		t.Fatalf("blank file: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	// Negative cumulative value: exit 4, no output.
	files = map[string]string{"neg.txt": "0 5\n100 -1\n"}
	got = runWithFiles(t, files, "--file", "neg.txt", "--window", "1s", "rates", "--from", "0", "--to", "1000")
	if got.code != 4 || got.stdout != "" || !strings.Contains(got.stderr, ":2:") {
		t.Fatalf("negative: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	// Non-finite cumulative value: exit 4, no output.
	files = map[string]string{"nan.txt": "0 5\n100 NaN\n"}
	got = runWithFiles(t, files, "--file", "nan.txt", "--window", "1s", "rates", "--from", "0", "--to", "1000")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("NaN: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
}
