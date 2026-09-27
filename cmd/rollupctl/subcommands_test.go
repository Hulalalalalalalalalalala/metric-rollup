package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// runWithFiles writes each map entry to a file named by its key in one
// temporary directory, then runs the command replacing every argument that
// matches a map key with that file's path.
func runWithFiles(t *testing.T, contents map[string]string, args ...string) runResult {
	t.Helper()
	dir := t.TempDir()
	full := make([]string, 0, len(args))
	for _, a := range args {
		// Support an inline --flag=<name> as well as a bare <name>.
		if k := strings.IndexByte(a, '='); k >= 0 {
			prefix, name := a[:k+1], a[k+1:]
			if content, ok := contents[name]; ok {
				path := filepath.Join(dir, name)
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				full = append(full, prefix+path)
				continue
			}
		}
		if content, ok := contents[a]; ok {
			path := filepath.Join(dir, a)
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			a = path
		}
		full = append(full, a)
	}
	return runArgs(t, full...)
}

// --- range -------------------------------------------------------------

func TestRangeHalfOpenBoundaries(t *testing.T) {
	content := strings.Join([]string{
		"0 1",
		"500000000 2",
		"1000000000 -3",
		"1500000000 4",
		"2000000000 5",
	}, "\n") + "\n"
	files := map[string]string{"samples.txt": content}

	// [1s, 2s): the window starting exactly at --to (2s) is excluded.
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "1000000000", "--to", "2000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "1000000000 2 1 -3 4\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	// An endpoint inside a window attributes by the same window rule as
	// Add: the window containing 1.2s starts at 1s.
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "1200000000", "--to", "1500000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "1000000000 2 1 -3 4\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	// --from exactly on a window boundary starts with that window.
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "0", "--to", "1000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "0 2 3 1 2\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestRangeOutputShapeMatchesWindows(t *testing.T) {
	content := "0 1\n500000000 2\n1000000000 -0\n1500000000 1.5\n"
	files := map[string]string{"samples.txt": content}
	all := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s", "windows")
	rng := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "-1000000000", "--to", "3000000000")
	if all.code != 0 || rng.code != 0 {
		t.Fatalf("windows code %d, range code %d (%q %q)", all.code, rng.code, all.stderr, rng.stderr)
	}
	if rng.stdout != all.stdout {
		t.Fatalf("range stdout = %q, windows stdout = %q", rng.stdout, all.stdout)
	}
}

func TestRangeEmptyIntervalsPrintNothing(t *testing.T) {
	files := map[string]string{"samples.txt": "0 1\n1000000000 2\n"}
	cases := map[string][2]string{
		"equal endpoints":  {"1000000000", "1000000000"},
		"from after to":    {"2000000000", "1000000000"},
		"no window inside": {"5000000000", "6000000000"},
		"before the first": {"-3000000000", "-2000000000"},
	}
	for name, ep := range cases {
		t.Run(name, func(t *testing.T) {
			got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
				"range", "--from", ep[0], "--to", ep[1])
			if got.code != 0 {
				t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" || got.stderr != "" {
				t.Fatalf("stdout = %q, stderr = %q, want both empty", got.stdout, got.stderr)
			}
		})
	}
}

func TestRangeStreamsMoreThanOneBatch(t *testing.T) {
	// Well over rangeBatchSize windows, so the cursor must advance.
	const n = rangeBatchSize*3 + 7
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "%d %d\n", int64(i)*1000000, i)
	}
	files := map[string]string{"samples.txt": sb.String()}
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1ms",
		"range", "--from", "0", "--to", strconv.FormatInt(int64(n)*1000000, 10))
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	lines := strings.Split(strings.TrimRight(got.stdout, "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines, want %d", len(lines), n)
	}
	for i, line := range lines {
		want := fmt.Sprintf("%d 1 %d %d %d", int64(i)*1000000, i, i, i)
		if line != want {
			t.Fatalf("line %d = %q, want %q", i, line, want)
		}
	}
}

func TestRangeNegativeAndExtremeEndpoints(t *testing.T) {
	// -1.5s aligns to -2s in a 2s window; [-2s, 0) selects it.
	files := map[string]string{"samples.txt": "-1500000000 4\n-1000000000 8\n"}
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "2s",
		"range", "--from", "-2000000000", "--to", "0")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "-2000000000 2 12 4 8\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}

	// The most negative timestamp aligns to -9223372036854775809 in a 3ns
	// window; the range must reach that window and print its exact start.
	files = map[string]string{"samples.txt": "-9223372036854775808 1\n"}
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "3ns",
		"range", "--from", "-9223372036854775808", "--to", "-9223372036854775805")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "-9223372036854775809 1 1 1 1\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}

	// Backwards interval at the extremes: empty, not an error.
	files = map[string]string{"samples.txt": "9223372036854775807 1\n"}
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "1ns",
		"range", "--from", "9223372036854775807", "--to", "-9223372036854775808")
	if got.code != 0 || got.stdout != "" {
		t.Fatalf("code = %d, stdout = %q", got.code, got.stdout)
	}
}

func TestRangeArgumentErrors(t *testing.T) {
	files := map[string]string{"samples.txt": "0 1\n"}
	cases := []struct {
		name string
		args []string
	}{
		{"missing from", []string{"--file", "samples.txt", "--window", "1s", "range", "--to", "1"}},
		{"missing to", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1"}},
		{"bad from", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "x", "--to", "1"}},
		{"fractional to", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "1.5"}},
		{"from out of int64", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "9223372036854775808", "--to", "1"}},
		{"to out of int64", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "-9223372036854775809"}},
		{"extra positional", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2", "extra"}},
		{"unknown flag", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2", "--step", "3"}},
		{"two files", []string{"--file", "samples.txt", "--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2"}},
		{"no file", []string{"--window", "1s", "range", "--from", "1", "--to", "2"}},
		{"from on windows", []string{"--file", "samples.txt", "--window", "1s", "windows", "--from", "1"}},
		{"to on windows", []string{"--file", "samples.txt", "--window", "1s", "windows", "--to", "1"}},
		{"from on merge", []string{"--file", "samples.txt", "--file", "samples.txt", "--window", "1s", "merge", "--from", "1"}},
		{"dangling from", []string{"--file", "samples.txt", "--window", "1s", "range", "--to", "1", "--from"}},
		{"empty from value", []string{"--file", "samples.txt", "--window", "1s", "range", "--from=", "--to", "1"}},
		{"unknown subcommand", []string{"--file", "samples.txt", "--window", "1s", "query"}},
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

func TestRangeFileAndContentErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	if got := runArgs(t, "--file", missing, "--window", "1s", "range", "--from", "0", "--to", "1"); got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	if got := runArgs(t, "--file", dir, "--window", "1s", "range", "--from", "0", "--to", "1"); got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}

	files := map[string]string{"bad.txt": "0 1\n50 2\n30 3\n"}
	if got := runWithFiles(t, files, "--file", "bad.txt", "--window", "1s", "range", "--from", "0", "--to", "100"); got.code != 4 || got.stdout != "" {
		t.Fatalf("out of order: code = %d, stdout = %q", got.code, got.stdout)
	}
	files = map[string]string{"empty.txt": ""}
	if got := runWithFiles(t, files, "--file", "empty.txt", "--window", "1s", "range", "--from", "0", "--to", "1"); got.code != 4 || !strings.Contains(got.stderr, ":0:") || got.stdout != "" {
		t.Fatalf("empty file: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
	files = map[string]string{"nan.txt": "0 1\n0 NaN\n"}
	if got := runWithFiles(t, files, "--file", "nan.txt", "--window", "1s", "range", "--from", "0", "--to", "10"); got.code != 4 || got.stdout != "" {
		t.Fatalf("non-finite: code = %d, stdout = %q", got.code, got.stdout)
	}
}

// --- merge -------------------------------------------------------------

func TestMergeCombinesAndDedupesWindows(t *testing.T) {
	files := map[string]string{
		"a.txt": "0 1\n500000000 2\n2000000000 5\n",
		"b.txt": "500000000 10\n1000000000 -3\n",
	}
	got := runWithFiles(t, files, "--file", "a.txt", "--file", "b.txt", "--window", "1s", "merge")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := strings.Join([]string{
		"0 3 13 1 10",
		"1000000000 1 -3 -3 -3",
		"2000000000 1 5 5 5",
		"",
	}, "\n")
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestMergeDisjointRanges(t *testing.T) {
	files := map[string]string{
		"a.txt": "-2000000000 1\n-1500000000 2\n",
		"b.txt": "3000000000 7\n",
	}
	got := runWithFiles(t, files, "--file", "a.txt", "--file", "b.txt", "--window", "1s", "merge")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	want := "-2000000000 2 3 1 2\n3000000000 1 7 7 7\n"
	if got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestMergeSameFileTwiceDoublesIt(t *testing.T) {
	files := map[string]string{"a.txt": "0 2\n500000000 3\n"}
	got := runWithFiles(t, files, "--file", "a.txt", "--file", "a.txt", "--window", "1s", "merge")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 4 10 2 3\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}

func TestMergeOrderIndependent(t *testing.T) {
	files := map[string]string{
		"a.txt": "0 1\n1000000000 9\n",
		"b.txt": "0 4\n1000000000 -2\n",
	}
	ab := runWithFiles(t, files, "--file", "a.txt", "--file", "b.txt", "--window", "1s", "merge")
	ba := runWithFiles(t, files, "--file", "b.txt", "--file", "a.txt", "--window", "1s", "merge")
	if ab.code != 0 || ba.code != 0 {
		t.Fatalf("ab code %d (%q), ba code %d (%q)", ab.code, ab.stderr, ba.code, ba.stderr)
	}
	if ab.stdout != ba.stdout {
		t.Fatalf("ab stdout = %q, ba stdout = %q", ab.stdout, ba.stdout)
	}
	if want := "0 2 5 1 4\n1000000000 2 7 -2 9\n"; ab.stdout != want {
		t.Fatalf("stdout = %q, want %q", ab.stdout, want)
	}
}

func TestMergeArgumentErrors(t *testing.T) {
	files := map[string]string{"a.txt": "0 1\n", "b.txt": "0 2\n", "c.txt": "0 3\n"}
	cases := []struct {
		name string
		args []string
	}{
		{"one file", []string{"--file", "a.txt", "--window", "1s", "merge"}},
		{"three files", []string{"--file", "a.txt", "--file", "b.txt", "--file", "c.txt", "--window", "1s", "merge"}},
		{"no file", []string{"--window", "1s", "merge"}},
		{"missing window", []string{"--file", "a.txt", "--file", "b.txt", "merge"}},
		{"second subcommand", []string{"--file", "a.txt", "--file", "b.txt", "--window", "1s", "merge", "windows"}},
		{"unknown flag", []string{"--file", "a.txt", "--file", "b.txt", "--window", "1s", "merge", "--factor", "2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runWithFiles(t, files, tc.args...)
			if got.code != 2 {
				t.Fatalf("code = %d, want 2; stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout must be empty, got %q", got.stdout)
			}
			if !strings.HasPrefix(got.stderr, "rollupctl: ") ||
				strings.Count(strings.TrimRight(got.stderr, "\n"), "\n") != 0 {
				t.Fatalf("stderr must be one rollupctl: line, got %q", got.stderr)
			}
		})
	}
}

func TestMergeFileErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	good := filepath.Join(dir, "good.txt")
	if err := os.WriteFile(good, []byte("0 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := runArgs(t, "--file", good, "--file", missing, "--window", "1s", "merge"); got.code != 3 || got.stdout != "" {
		t.Fatalf("missing second: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
	if got := runArgs(t, "--file", missing, "--file", good, "--window", "1s", "merge"); got.code != 3 || got.stdout != "" {
		t.Fatalf("missing first: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
	if got := runArgs(t, "--file", good, "--file", dir, "--window", "1s", "merge"); got.code != 3 || got.stdout != "" {
		t.Fatalf("directory second: code = %d, stdout = %q", got.code, got.stdout)
	}
}

func TestMergeContentErrorsRejectBoth(t *testing.T) {
	// A problem in either file means no window line from either file may
	// appear, in either argument order.
	files := map[string]string{
		"good.txt":  "0 1\n1000000000 2\n",
		"nan.txt":   "0 1\n0 NaN\n",
		"order.txt": "100 1\n50 2\n",
		"empty.txt": "\n  \n\t\n",
		"long.txt":  "0 1\n" + strings.Repeat("x", 1025) + "\n",
	}
	for _, bad := range []string{"nan.txt", "order.txt", "empty.txt", "long.txt"} {
		for order, args := range [][]string{
			{"--file", "good.txt", "--file", bad, "--window", "1s", "merge"},
			{"--file", bad, "--file", "good.txt", "--window", "1s", "merge"},
		} {
			got := runWithFiles(t, files, args...)
			if got.code != 4 {
				t.Fatalf("%s order %d: code = %d, want 4, stderr = %q", bad, order, got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("%s order %d: stdout = %q, want empty", bad, order, got.stdout)
			}
			if !strings.HasPrefix(got.stderr, "rollupctl: ") ||
				strings.Count(strings.TrimRight(got.stderr, "\n"), "\n") != 0 {
				t.Fatalf("%s order %d: stderr = %q", bad, order, got.stderr)
			}
		}
	}
}

func TestMergeFlagsAroundSubcommand(t *testing.T) {
	files := map[string]string{
		"a.txt": "0 1\n",
		"b.txt": "0 3\n",
	}
	// Flags on both sides of the subcommand, including --name=value form.
	got := runWithFiles(t, files, "--file=a.txt", "merge", "--window=1s", "--file", "b.txt")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 2 4 1 3\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}
}
