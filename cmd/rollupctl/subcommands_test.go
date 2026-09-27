package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Hulalalalalalalalalalala/metric-rollup/rollup"
)

// runWithFiles writes each named content to its own file and runs the
// command with the given arguments, replacing every "<name>" placeholder
// with the file's path.
func runWithFiles(t *testing.T, contents map[string]string, args ...string) runResult {
	t.Helper()
	dir := t.TempDir()
	full := make([]string, 0, len(args))
	for _, a := range args {
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

func TestRangeSelectsHalfOpenInterval(t *testing.T) {
	content := strings.Join([]string{
		"0 1",
		"500000000 2",
		"1000000000 -3",
		"1500000000 4",
		"2000000000 5",
	}, "\n") + "\n"
	files := map[string]string{"samples.txt": content}

	// [1s, 2s) keeps the windows starting at 1s; the window starting
	// exactly at --to is excluded.
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "1000000000", "--to", "2000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "1000000000 2 1 -3 4\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}

	// A start inside a window selects the whole window containing it.
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
		"range", "--from", "1200000000", "--to", "1500000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if want := "1000000000 2 1 -3 4\n"; got.stdout != want {
		t.Fatalf("stdout = %q, want %q", got.stdout, want)
	}
}

func TestRangeEmptyResultsAreNotErrors(t *testing.T) {
	files := map[string]string{"samples.txt": "0 1\n1000000000 2\n"}
	for name, endpoints := range map[string][2]string{
		"from equals to":   {"1000000000", "1000000000"},
		"from after to":    {"2000000000", "1000000000"},
		"no window inside": {"2000000000", "3000000000"},
		"before first":     {"-3000000000", "-2000000000"},
	} {
		t.Run(name, func(t *testing.T) {
			got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1s",
				"range", "--from", endpoints[0], "--to", endpoints[1])
			if got.code != 0 {
				t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
			}
			if got.stdout != "" {
				t.Fatalf("stdout = %q, want empty", got.stdout)
			}
		})
	}
}

func TestRangeCoversManyWindowsInBatches(t *testing.T) {
	// More windows than one cursor batch, so iteration must advance.
	var lines []string
	for i := 0; i < 3000; i++ {
		lines = append(lines, fmt.Sprintf("%d %d", i*1000000, i))
	}
	files := map[string]string{"samples.txt": strings.Join(lines, "\n") + "\n"}
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "1ms",
		"range", "--from", "0", "--to", "3000000000")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	out := strings.Split(strings.TrimRight(got.stdout, "\n"), "\n")
	if len(out) != 3000 {
		t.Fatalf("got %d lines, want 3000", len(out))
	}
	if out[0] != "0 1 0 0 0" || out[2999] != "2999000000 1 2999 2999 2999" {
		t.Fatalf("first = %q, last = %q", out[0], out[2999])
	}
}

func TestRangeNegativeAndExtremeEndpoints(t *testing.T) {
	files := map[string]string{"samples.txt": "-1500000000 4\n-1000000000 8\n"}
	got := runWithFiles(t, files, "--file", "samples.txt", "--window", "2s",
		"range", "--from", "-2000000000", "--to", "0")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "-2000000000 2 12 4 8\n" {
		t.Fatalf("stdout = %q", got.stdout)
	}

	files = map[string]string{"samples.txt": "9223372036854775807 1\n"}
	got = runWithFiles(t, files, "--file", "samples.txt", "--window", "1ns",
		"range", "--from", "9223372036854775807", "--to", "-9223372036854775808")
	if got.code != 0 || got.stdout != "" {
		t.Fatalf("backwards extreme interval: code = %d, stdout = %q", got.code, got.stdout)
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
		{"bad to", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "1.5"}},
		{"from out of range", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "9223372036854775808", "--to", "1"}},
		{"extra positional", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2", "extra"}},
		{"unknown flag", []string{"--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2", "--step", "3"}},
		{"two files", []string{"--file", "samples.txt", "--file", "samples.txt", "--window", "1s", "range", "--from", "1", "--to", "2"}},
		{"from on windows", []string{"--file", "samples.txt", "--window", "1s", "windows", "--from", "1"}},
		{"from on merge", []string{"--file", "samples.txt", "--file", "samples.txt", "--window", "1s", "merge", "--from", "1", "--to", "2"}},
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
	got := runArgs(t, "--file", missing, "--window", "1s", "range", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("missing file: code = %d, stderr = %q", got.code, got.stderr)
	}
	got = runArgs(t, "--file", dir, "--window", "1s", "range", "--from", "0", "--to", "1")
	if got.code != 3 {
		t.Fatalf("directory: code = %d, stderr = %q", got.code, got.stderr)
	}

	files := map[string]string{"bad.txt": "0 1\n50 2\n30 3\n"}
	got = runWithFiles(t, files, "--file", "bad.txt", "--window", "1s", "range", "--from", "0", "--to", "100")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("out of order: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	files = map[string]string{"empty.txt": "\n  \n"}
	got = runWithFiles(t, files, "--file", "empty.txt", "--window", "1s", "range", "--from", "0", "--to", "1")
	if got.code != 4 || !strings.Contains(got.stderr, ":0:") {
		t.Fatalf("blank file: code = %d, stderr = %q", got.code, got.stderr)
	}
}

func TestMergeCombinesWindows(t *testing.T) {
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

func TestMergeSameFileTwice(t *testing.T) {
	files := map[string]string{"a.txt": "0 2\n"}
	got := runWithFiles(t, files, "--file", "a.txt", "--file", "a.txt", "--window", "1s", "merge")
	if got.code != 0 {
		t.Fatalf("code = %d, stderr = %q", got.code, got.stderr)
	}
	if got.stdout != "0 2 4 2 2\n" {
		t.Fatalf("stdout = %q", got.stdout)
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
		})
	}
}

func TestMergeFileAndContentErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist")
	good := filepath.Join(dir, "good.txt")
	if err := os.WriteFile(good, []byte("0 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := runArgs(t, "--file", good, "--file", missing, "--window", "1s", "merge")
	if got.code != 3 || got.stdout != "" {
		t.Fatalf("missing second file: code = %d, stdout = %q", got.code, got.stdout)
	}
	got = runArgs(t, "--file", missing, "--file", good, "--window", "1s", "merge")
	if got.code != 3 || got.stdout != "" {
		t.Fatalf("missing first file: code = %d, stdout = %q", got.code, got.stdout)
	}

	// A content problem in either file rejects the whole batch: no window
	// lines from the good file may leak out.
	files := map[string]string{
		"good.txt": "0 1\n1000000000 2\n",
		"bad.txt":  "0 1\n0 NaN\n",
	}
	got = runWithFiles(t, files, "--file", "good.txt", "--file", "bad.txt", "--window", "1s", "merge")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("bad second file: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
	got = runWithFiles(t, files, "--file", "bad.txt", "--file", "good.txt", "--window", "1s", "merge")
	if got.code != 4 || got.stdout != "" {
		t.Fatalf("bad first file: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}

	files = map[string]string{"good.txt": "0 1\n", "empty.txt": ""}
	got = runWithFiles(t, files, "--file", "good.txt", "--file", "empty.txt", "--window", "1s", "merge")
	if got.code != 4 || !strings.Contains(got.stderr, ":0:") || got.stdout != "" {
		t.Fatalf("empty second file: code = %d, stdout = %q, stderr = %q", got.code, got.stdout, got.stderr)
	}
}

// TestMergeMismatchDiagnostic pins the one piece of merge output the
// contract writes to the letter: when the widths differ, the two file
// names appear in command-line order, separated by a comma and a space,
// on the single stderr diagnostic line. The mismatched-width state is
// unreachable through one --window flag, so the formatting unit is
// exercised directly.
func TestMergeMismatchDiagnostic(t *testing.T) {
	var stderr bytes.Buffer
	writeMergeError(&stderr, "first.txt", "second.txt", rollup.ErrWindowMismatch)
	want := "rollupctl: first.txt, second.txt: rollup: window mismatch\n"
	if stderr.String() != want {
		t.Fatalf("stderr = %q, want %q", stderr.String(), want)
	}
	if strings.Count(strings.TrimRight(stderr.String(), "\n"), "\n") != 0 {
		t.Fatalf("stderr must be one line: %q", stderr.String())
	}
}
