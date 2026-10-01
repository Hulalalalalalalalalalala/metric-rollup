// Command rollupctl turns JSON Lines samples into fixed-window rollups.
//
// Usage:
//
//	rollupctl --file <path> [--window <duration>] windows
//
// Each input line is a JSON object with an RFC3339Nano "at" string and a
// numeric "value". The single windows command writes the rollup as JSON to
// stdout.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Hulalalalalalalalalalala/metric-rollup"
)

const (
	exitOK    = 0
	exitIO    = 1
	exitInput = 2
)

var (
	errInvalidDuration = errors.New("rollup input error: invalid duration")
	errInvalidJSON     = errors.New("rollup input error: invalid JSON")
	errInvalidAt       = errors.New("rollup input error: invalid at")
	errInvalidValue    = errors.New("rollup input error: invalid value")
	errMissingAt       = errors.New("rollup input error: missing at")
	errMissingValue    = errors.New("rollup input error: missing value")
	errOutOfOrder      = errors.New("rollup input error: sample out of order")
)

func main() {
	code, msg := run(os.Args[1:], os.Stdout)
	if msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(code)
}

// run parses args, reads the sample file and writes the rollup to out. It
// returns the process exit code and the error line to print on stderr.
func run(args []string, out io.Writer) (int, string) {
	filePath, windowText, code, msg := parseArgs(args)
	if msg != "" {
		return code, msg
	}

	if windowText == "" {
		windowText = "1m"
	}
	window, err := time.ParseDuration(windowText)
	if err != nil || window <= 0 {
		return exitInput, errInvalidDuration.Error()
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return exitIO, "rollup io error"
	}

	roller, code, msg := buildRoller(data, window)
	if msg != "" {
		return code, msg
	}
	if err := writeWindows(out, roller); err != nil {
		return exitIO, "rollup io error"
	}
	return exitOK, ""
}

// parseArgs accepts the windows subcommand with --file and --window flags on
// either side of the subcommand.
func parseArgs(args []string) (filePath, windowText string, code int, msg string) {
	var commands []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, hasValue := strings.Cut(arg, "=")
		switch name {
		case "--file", "-file", "--window", "-window":
			if !hasValue {
				i++
				if i >= len(args) {
					return "", "", exitInput, fmt.Sprintf("rollupctl: flag %s requires a value", name)
				}
				value = args[i]
			}
			if name == "--file" || name == "-file" {
				filePath = value
			} else {
				windowText = value
			}
		default:
			if strings.HasPrefix(arg, "-") && arg != "-" {
				return "", "", exitInput, fmt.Sprintf("rollupctl: unknown flag %q", name)
			}
			commands = append(commands, arg)
		}
	}
	if len(commands) != 1 || commands[0] != "windows" {
		return "", "", exitInput, "usage: rollupctl --file <path> [--window <duration>] windows"
	}
	if filePath == "" {
		return "", "", exitInput, "rollupctl: --file is required"
	}
	return filePath, windowText, exitOK, ""
}

// buildRoller feeds every non-empty JSON Lines record to a new roller.
func buildRoller(data []byte, window time.Duration) (*rollup.Roller, int, string) {
	roller := rollup.New(window)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		at, value, err := parseSample(line)
		if err != nil {
			return nil, exitInput, err.Error()
		}
		if err := roller.Add(at, value); err != nil {
			return nil, exitInput, errOutOfOrder.Error()
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, exitIO, "rollup io error"
	}
	return roller, exitOK, ""
}

// parseSample decodes one JSON Lines record into a time and value.
func parseSample(line string) (time.Time, float64, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		return time.Time{}, 0, errInvalidJSON
	}
	rawAt, ok := fields["at"]
	if !ok {
		return time.Time{}, 0, errMissingAt
	}
	rawValue, ok := fields["value"]
	if !ok {
		return time.Time{}, 0, errMissingValue
	}

	var atText string
	if err := json.Unmarshal(rawAt, &atText); err != nil {
		return time.Time{}, 0, errInvalidAt
	}
	at, err := time.Parse(time.RFC3339Nano, atText)
	if err != nil {
		return time.Time{}, 0, errInvalidAt
	}

	// json.Unmarshal accepts null into a float64 as a silent no-op.
	if string(bytes.TrimSpace(rawValue)) == "null" {
		return time.Time{}, 0, errInvalidValue
	}
	var value float64
	if err := json.Unmarshal(rawValue, &value); err != nil {
		return time.Time{}, 0, errInvalidValue
	}
	return at, value, nil
}

type outWindow struct {
	Start string  `json:"start"`
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// writeWindows emits the fixed compact JSON envelope, UTC start times in
// RFC3339Nano, windows ordered by start.
func writeWindows(out io.Writer, roller *rollup.Roller) error {
	windows := roller.Windows()
	rows := make([]outWindow, len(windows))
	for i, w := range windows {
		rows[i] = outWindow{
			Start: w.Start.UTC().Format(time.RFC3339Nano),
			Count: w.Count,
			Sum:   w.Sum,
			Min:   w.Min,
			Max:   w.Max,
		}
	}
	payload, err := json.Marshal(struct {
		Windows []outWindow `json:"windows"`
	}{rows})
	if err != nil {
		return err
	}
	_, err = out.Write(append(payload, '\n'))
	return err
}
