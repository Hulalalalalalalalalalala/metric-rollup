// Command rollupctl reads time series samples and emits fixed-window
// rollups.
//
// Usage:
//
//	rollupctl --file <path> [--window duration] windows
//
// The input file holds one JSON object per line:
//
//	{"at":"2026-01-01T00:00:00Z","value":2.5}
//
// The windows subcommand writes the aggregated buckets as JSON.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Hulalalalalalalalalalala/metric-rollup"
)

// Input errors share this prefix and exit with status 2; file and
// output failures use "rollup io error" and exit with status 1.
const (
	inputErrorPrefix = "rollup input error: "
	ioErrorText      = "rollup io error"
)

var (
	errInvalidJSON     = errors.New(inputErrorPrefix + "invalid JSON")
	errInvalidAt       = errors.New(inputErrorPrefix + "invalid at")
	errInvalidValue    = errors.New(inputErrorPrefix + "invalid value")
	errMissingAt       = errors.New(inputErrorPrefix + "missing at")
	errMissingValue    = errors.New(inputErrorPrefix + "missing value")
	errInvalidDuration = errors.New(inputErrorPrefix + "invalid duration")
)

// decodeSample parses one JSON line into its raw at and value fields.
func decodeSample(line []byte) (string, json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return "", nil, errInvalidJSON
	}

	atRaw, ok := raw["at"]
	if !ok {
		return "", nil, errMissingAt
	}
	var at string
	if err := json.Unmarshal(atRaw, &at); err != nil {
		return "", nil, errInvalidAt
	}

	valueRaw, ok := raw["value"]
	if !ok {
		return "", nil, errMissingValue
	}

	return at, valueRaw, nil
}

func parseAt(s string) (time.Time, error) {
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errInvalidAt
	}
	return at, nil
}

func parseValue(raw json.RawMessage) (float64, error) {
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, errInvalidValue
	}
	v, err := n.Float64()
	if err != nil {
		return 0, errInvalidValue
	}
	return v, nil
}

type outputWindow struct {
	Start string  `json:"start"`
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

type output struct {
	Windows []outputWindow `json:"windows"`
}

// aggregateWindows parses JSON Lines samples and returns the rollup
// result ready for encoding.
func aggregateWindows(input []byte, window time.Duration) (output, error) {
	r := rollup.New(window)

	for _, line := range bytes.Split(input, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		atStr, valueRaw, err := decodeSample(line)
		if err != nil {
			return output{}, err
		}
		at, err := parseAt(atStr)
		if err != nil {
			return output{}, err
		}
		value, err := parseValue(valueRaw)
		if err != nil {
			return output{}, err
		}
		if err := r.Add(at, value); err != nil {
			return output{}, fmt.Errorf("%s%w", inputErrorPrefix, err)
		}
	}

	windows := r.Windows()
	res := output{Windows: make([]outputWindow, 0, len(windows))}
	for _, w := range windows {
		res.Windows = append(res.Windows, outputWindow{
			Start: w.Start.UTC().Format(time.RFC3339Nano),
			Count: w.Count,
			Sum:   w.Sum,
			Min:   w.Min,
			Max:   w.Max,
		})
	}
	return res, nil
}

// writeOutput encodes the rollup result as one JSON line. It returns
// an error carrying the io-error text when encoding or writing fails.
func writeOutput(res output, w io.Writer) error {
	encoded, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("%s: %w", ioErrorText, err)
	}
	if _, err := w.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("%s: %w", ioErrorText, err)
	}
	return nil
}

// run executes the CLI. It returns the process exit status (0, 1 or 2)
// and writes any diagnostics to stderr.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("rollupctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: rollupctl --file <path> [--window duration] windows")
		fs.PrintDefaults()
	}
	file := fs.String("file", "", "path to a JSON Lines sample file")
	windowFlag := fs.String("window", "1m", "fixed window duration")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 || fs.Arg(0) != "windows" {
		fs.Usage()
		return 2
	}

	window, err := time.ParseDuration(*windowFlag)
	if err != nil || window <= 0 {
		fmt.Fprintln(stderr, errInvalidDuration)
		return 2
	}

	data, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintln(stderr, ioErrorText)
		return 1
	}

	res, err := aggregateWindows(data, window)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	if err := writeOutput(res, stdout); err != nil {
		fmt.Fprintln(stderr, ioErrorText)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
