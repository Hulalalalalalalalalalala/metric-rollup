package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func TestRunSuccessMultipleWindows(t *testing.T) {
	input := strings.Join([]string{
		`{"at":"2026-01-02T15:04:05Z","value":2.5}`,
		`{"at":"2026-01-02T15:04:50Z","value":1.5}`,
		`{"at":"2026-01-02T15:05:05Z","value":4}`,
	}, "\n") + "\n"
	path := writeTemp(t, "samples.jsonl", input)

	var out bytes.Buffer
	code, msg := run([]string{"--file", path, "windows"}, &out)
	if code != exitOK || msg != "" {
		t.Fatalf("code = %d msg = %q, want 0/empty", code, msg)
	}
	want := `{"windows":[{"start":"2026-01-02T15:04:00Z","count":2,"sum":4,"min":1.5,"max":2.5},{"start":"2026-01-02T15:05:00Z","count":1,"sum":4,"min":4,"max":4}]}` + "\n"
	if out.String() != want {
		t.Errorf("output = %q, want %q", out.String(), want)
	}
}

func TestRunSingleSampleShape(t *testing.T) {
	path := writeTemp(t, "one.jsonl", `{"at":"2026-01-02T15:04:05Z","value":2.5}`+"\n")
	var out bytes.Buffer
	code, _ := run([]string{"windows", "--file", path}, &out)
	if code != exitOK {
		t.Fatalf("code = %d", code)
	}
	want := `{"windows":[{"start":"2026-01-02T15:04:00Z","count":1,"sum":2.5,"min":2.5,"max":2.5}]}` + "\n"
	if out.String() != want {
		t.Errorf("output = %q, want %q", out.String(), want)
	}
}

func TestRunEmptyInput(t *testing.T) {
	path := writeTemp(t, "empty.jsonl", "")
	var out bytes.Buffer
	code, msg := run([]string{"--file", path, "windows"}, &out)
	if code != exitOK || msg != "" {
		t.Fatalf("code = %d msg = %q", code, msg)
	}
	if out.String() != `{"windows":[]}`+"\n" {
		t.Errorf("output = %q, want empty windows envelope", out.String())
	}
}

func TestRunBlankLinesIgnored(t *testing.T) {
	path := writeTemp(t, "blanks.jsonl", "\n  \n"+`{"at":"2026-01-02T15:04:05Z","value":1}`+"\n\n")
	var out bytes.Buffer
	if code, msg := run([]string{"--file", path, "windows"}, &out); code != exitOK || msg != "" {
		t.Fatalf("code = %d msg = %q", code, msg)
	}
	if strings.Count(out.String(), `"start"`) != 1 {
		t.Errorf("output = %q, want one window", out.String())
	}
}

func TestRunDefaultWindowOneMinute(t *testing.T) {
	// 60s apart fall in adjacent windows with the default 1m.
	path := writeTemp(t, "def.jsonl",
		`{"at":"2026-01-02T15:04:00Z","value":1}`+"\n"+
			`{"at":"2026-01-02T15:05:00Z","value":2}`+"\n")
	var out bytes.Buffer
	if code, _ := run([]string{"--file", path, "windows"}, &out); code != exitOK {
		t.Fatal("default window run failed")
	}
	if strings.Count(out.String(), `"start"`) != 2 {
		t.Errorf("output = %q, want two 1m windows", out.String())
	}
}

func TestRunCustomWindow(t *testing.T) {
	path := writeTemp(t, "s.jsonl",
		`{"at":"2026-01-02T15:04:00Z","value":1}`+"\n"+
			`{"at":"2026-01-02T15:04:30Z","value":2}`+"\n")
	var out bytes.Buffer
	code, msg := run([]string{"--file", path, "--window", "30s", "windows"}, &out)
	if code != exitOK || msg != "" {
		t.Fatalf("code = %d msg = %q", code, msg)
	}
	if strings.Count(out.String(), `"start"`) != 2 {
		t.Errorf("output = %q, want two 30s windows", out.String())
	}
}

func TestRunDuplicateSampleCountedOnce(t *testing.T) {
	line := `{"at":"2026-01-02T15:04:05.123456789Z","value":2.5}`
	path := writeTemp(t, "dup.jsonl", line+"\n"+line+"\n")
	var out bytes.Buffer
	if code, msg := run([]string{"--file", path, "windows"}, &out); code != exitOK || msg != "" {
		t.Fatalf("code = %d msg = %q", code, msg)
	}
	if !strings.Contains(out.String(), `"count":1`) {
		t.Errorf("output = %q, want duplicate counted once", out.String())
	}
}

func TestRunPreservesNanosecondsInStart(t *testing.T) {
	path := writeTemp(t, "nano.jsonl", `{"at":"2026-01-02T15:04:05.123456789Z","value":1}`+"\n")
	var out bytes.Buffer
	code, _ := run([]string{"--file", path, "--window", "1ns", "windows"}, &out)
	if code != exitOK {
		t.Fatal("run failed")
	}
	if !strings.Contains(out.String(), `"start":"2026-01-02T15:04:05.123456789Z"`) {
		t.Errorf("output = %q, want nanosecond start", out.String())
	}
}

func TestRunInputErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantMsg string
	}{
		{"invalid JSON", `{"at":"2026-01-02T15:04:05Z",`, errInvalidJSON.Error()},
		{"invalid at", `{"at":"not-a-time","value":1}`, errInvalidAt.Error()},
		{"at not a string", `{"at":123,"value":1}`, errInvalidAt.Error()},
		{"invalid value string", `{"at":"2026-01-02T15:04:05Z","value":"x"}`, errInvalidValue.Error()},
		{"invalid value object", `{"at":"2026-01-02T15:04:05Z","value":{}}`, errInvalidValue.Error()},
		{"invalid value null", `{"at":"2026-01-02T15:04:05Z","value":null}`, errInvalidValue.Error()},
		{"missing at", `{"value":1}`, errMissingAt.Error()},
		{"missing value", `{"at":"2026-01-02T15:04:05Z"}`, errMissingValue.Error()},
		{"out of order earlier", "{\"at\":\"2026-01-02T15:04:05Z\",\"value\":1}\n{\"at\":\"2026-01-02T15:04:04Z\",\"value\":2}", errOutOfOrder.Error()},
		{"out of order same time diff value", "{\"at\":\"2026-01-02T15:04:05Z\",\"value\":1}\n{\"at\":\"2026-01-02T15:04:05Z\",\"value\":2}", errOutOfOrder.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, "bad.jsonl", tc.content)
			var out bytes.Buffer
			code, msg := run([]string{"--file", path, "windows"}, &out)
			if code != exitInput {
				t.Errorf("code = %d, want %d", code, exitInput)
			}
			if msg != tc.wantMsg {
				t.Errorf("msg = %q, want %q", msg, tc.wantMsg)
			}
			if out.Len() != 0 {
				t.Errorf("wrote %q on input error", out.String())
			}
		})
	}
}

func TestRunInvalidDuration(t *testing.T) {
	path := writeTemp(t, "x.jsonl", "")
	var out bytes.Buffer
	for _, d := range []string{"abc", "1", "0s", "-5s"} {
		code, msg := run([]string{"--file", path, "--window", d, "windows"}, &out)
		if code != exitInput || msg != errInvalidDuration.Error() {
			t.Errorf("window %q: code = %d msg = %q, want %d %q", d, code, msg, exitInput, errInvalidDuration)
		}
	}
}

func TestRunMissingFile(t *testing.T) {
	var out bytes.Buffer
	code, msg := run([]string{"--file", filepath.Join(t.TempDir(), "nope.jsonl"), "windows"}, &out)
	if code != exitIO {
		t.Errorf("code = %d, want %d", code, exitIO)
	}
	if msg != "rollup io error" {
		t.Errorf("msg = %q, want rollup io error", msg)
	}
}

func TestRunOutputFailure(t *testing.T) {
	path := writeTemp(t, "x.jsonl", `{"at":"2026-01-02T15:04:05Z","value":1}`+"\n")
	code, msg := run([]string{"--file", path, "windows"}, &failingWriter{})
	if code != exitIO || msg != "rollup io error" {
		t.Errorf("code = %d msg = %q, want %d / rollup io error", code, msg, exitIO)
	}
}

type failingWriter struct{}

func (*failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestParseArgs(t *testing.T) {
	t.Run("subcommand before flags", func(t *testing.T) {
		file, window, code, msg := parseArgs([]string{"windows", "--file", "a.jsonl", "--window=2m"})
		if code != exitOK || msg != "" {
			t.Fatalf("code = %d msg = %q", code, msg)
		}
		if file != "a.jsonl" || window != "2m" {
			t.Errorf("file = %q window = %q", file, window)
		}
	})
	t.Run("missing subcommand", func(t *testing.T) {
		if _, _, code, _ := parseArgs([]string{"--file", "a.jsonl"}); code != exitInput {
			t.Errorf("code = %d, want %d", code, exitInput)
		}
	})
	t.Run("wrong subcommand", func(t *testing.T) {
		if _, _, code, _ := parseArgs([]string{"--file", "a.jsonl", "rollup"}); code != exitInput {
			t.Errorf("code = %d, want %d", code, exitInput)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		if _, _, code, _ := parseArgs([]string{"windows"}); code != exitInput {
			t.Errorf("code = %d, want %d", code, exitInput)
		}
	})
	t.Run("unknown flag", func(t *testing.T) {
		if _, _, code, _ := parseArgs([]string{"windows", "--bogus", "x", "--file", "a"}); code != exitInput {
			t.Errorf("code = %d, want %d", code, exitInput)
		}
	})
	t.Run("flag without value", func(t *testing.T) {
		if _, _, code, _ := parseArgs([]string{"windows", "--file"}); code != exitInput {
			t.Errorf("code = %d, want %d", code, exitInput)
		}
	})
}
