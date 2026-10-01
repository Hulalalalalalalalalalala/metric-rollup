package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTempFile creates a temporary file with the given content and
// returns its path.
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runCLI(t *testing.T, args []string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestCLIWindowsOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"at":"2026-01-01T00:00:00Z","value":2.5}`,
		`{"at":"2026-01-01T00:00:30Z","value":1.5}`,
		`{"at":"2026-01-01T00:01:00Z","value":4}`,
	}, "\n")
	path := writeTempFile(t, "samples.jsonl", input)

	code, stdout, stderr := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	want := `{"windows":[{"start":"2026-01-01T00:00:00Z","count":2,"sum":4,"min":1.5,"max":2.5},{"start":"2026-01-01T00:01:00Z","count":1,"sum":4,"min":4,"max":4}]}` + "\n"
	if stdout != want {
		t.Errorf("stdout = %q\nwant %q", stdout, want)
	}
}

func TestCLIEmptyInput(t *testing.T) {
	path := writeTempFile(t, "empty.jsonl", "")
	code, stdout, stderr := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	if want := `{"windows":[]}` + "\n"; stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestCLIDefaultWindowIsOneMinute(t *testing.T) {
	input := `{"at":"2026-01-01T00:00:59Z","value":1}` + "\n" +
		`{"at":"2026-01-01T00:01:00Z","value":1}`
	path := writeTempFile(t, "samples.jsonl", input)

	code, stdout, _ := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if strings.Count(stdout, `"start"`) != 2 {
		t.Errorf("default window did not split at the minute: %q", stdout)
	}
}

func TestCLICustomWindow(t *testing.T) {
	input := `{"at":"2026-01-01T00:00:00Z","value":1}` + "\n" +
		`{"at":"2026-01-01T00:01:59Z","value":1}`
	path := writeTempFile(t, "samples.jsonl", input)

	code, stdout, stderr := runCLI(t, []string{"--file", path, "--window", "2m", "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	if strings.Count(stdout, `"start"`) != 1 {
		t.Errorf("2m window did not keep both samples together: %q", stdout)
	}
	if !strings.Contains(stdout, `"count":2`) {
		t.Errorf("count missing from output: %q", stdout)
	}
}

func TestCLIDuplicateSample(t *testing.T) {
	line := `{"at":"2026-01-01T00:00:00Z","value":2.5}`
	path := writeTempFile(t, "dup.jsonl", line+"\n"+line)
	code, stdout, stderr := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	want := `{"windows":[{"start":"2026-01-01T00:00:00Z","count":1,"sum":2.5,"min":2.5,"max":2.5}]}` + "\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestCLISingleSampleShape(t *testing.T) {
	path := writeTempFile(t, "one.jsonl", `{"at":"2026-01-01T00:00:00Z","value":2.5}`)
	code, stdout, stderr := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	want := `{"windows":[{"start":"2026-01-01T00:00:00Z","count":1,"sum":2.5,"min":2.5,"max":2.5}]}` + "\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestCLIBlankLinesIgnored(t *testing.T) {
	input := "\n" + `{"at":"2026-01-01T00:00:00Z","value":1}` + "\n\n"
	path := writeTempFile(t, "blanks.jsonl", input)
	code, stdout, stderr := runCLI(t, []string{"--file", path, "windows"})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	if strings.Count(stdout, `"start"`) != 1 {
		t.Errorf("blank lines were not ignored: %q", stdout)
	}
}

func TestCLIInputErrors(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantMsg string
	}{
		{"invalid JSON", `{"at":"2026-01-01T00:00:00Z","value":`, "rollup input error: invalid JSON"},
		{"invalid at", `{"at":"not-a-time","value":1}`, "rollup input error: invalid at"},
		{"invalid value", `{"at":"2026-01-01T00:00:00Z","value":"x"}`, "rollup input error: invalid value"},
		{"missing at", `{"value":1}`, "rollup input error: missing at"},
		{"missing value", `{"at":"2026-01-01T00:00:00Z"}`, "rollup input error: missing value"},
		{"out of order", `{"at":"2026-01-01T00:00:01Z","value":1}` + "\n" + `{"at":"2026-01-01T00:00:00Z","value":1}`, "rollup input error: out of order"},
		{"same time different value", `{"at":"2026-01-01T00:00:00Z","value":1}` + "\n" + `{"at":"2026-01-01T00:00:00Z","value":2}`, "rollup input error: out of order"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempFile(t, "bad.jsonl", tc.input)
			code, _, stderr := runCLI(t, []string{"--file", path, "windows"})
			if code != 2 {
				t.Errorf("exit = %d, want 2", code)
			}
			if strings.TrimSpace(stderr) != tc.wantMsg {
				t.Errorf("stderr = %q, want %q", stderr, tc.wantMsg)
			}
		})
	}
}

func TestCLIInvalidDuration(t *testing.T) {
	path := writeTempFile(t, "one.jsonl", `{"at":"2026-01-01T00:00:00Z","value":1}`)
	for _, d := range []string{"nope", "0s", "-1m"} {
		code, _, stderr := runCLI(t, []string{"--file", path, "--window", d, "windows"})
		if code != 2 {
			t.Errorf("--window %s: exit = %d, want 2", d, code)
		}
		if want := "rollup input error: invalid duration"; strings.TrimSpace(stderr) != want {
			t.Errorf("--window %s: stderr = %q, want %q", d, stderr, want)
		}
	}
}

func TestCLIMissingFileIsIOError(t *testing.T) {
	code, _, stderr := runCLI(t, []string{"--file", filepath.Join(t.TempDir(), "nope.jsonl"), "windows"})
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if want := "rollup io error"; strings.TrimSpace(stderr) != want {
		t.Errorf("stderr = %q, want %q", stderr, want)
	}
}

func TestCLINonWindowsSubcommand(t *testing.T) {
	path := writeTempFile(t, "one.jsonl", `{"at":"2026-01-01T00:00:00Z","value":1}`)
	code, _, _ := runCLI(t, []string{"--file", path, "rollup"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestCLIOutputFailureIsIOError(t *testing.T) {
	input := `{"at":"2026-01-01T00:00:00Z","value":1}`
	res, err := aggregateWindows([]byte(input), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeOutput(res, failingWriter{}); err == nil {
		t.Error("writeOutput with failing writer returned nil error")
	}
}

// failingWriter errors on every write so output failures can be tested.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, os.ErrClosed
}
