// Command rollupctl reads timestamped samples from a file, files them into
// fixed windows aligned to the Unix epoch, and prints, one per line, each
// window's start, count, sum, minimum, and maximum.
//
// Usage:
//
//	rollupctl --file <path> --window <duration> windows
//	rollupctl --file <path> --window <duration> --from <ns> --to <ns> range
//	rollupctl --file <path> --file <path> --window <duration> merge
//
// The input file holds one sample per line: an integer number of
// nanoseconds since the epoch, whitespace, then a floating-point value.
// Blank and whitespace-only lines carry no sample. Samples must arrive in
// non-decreasing time order.
//
// windows prints every window of one file. range prints only the windows
// of one file that overlap the half-open interval [--from, --to), with both
// endpoints given as signed integer nanoseconds since the epoch; an empty,
// backwards, or unmatched interval prints nothing. merge aggregates two
// files that share the window size and prints their windows combined,
// folding windows that start at the same instant into one line.
//
// Exit codes: 0 success, 2 bad command-line arguments, 3 an input file
// does not exist or is a directory, 4 an input file's contents are invalid
// or the two merge inputs cannot be combined.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Hulalalalalalalalalalala/metric-rollup/rollup"
)

// maxLineBytes is the longest accepted physical input line, excluding the
// line terminator.
const maxLineBytes = 1024

// rangeBatchSize is how many windows one cursor Next call hands the range
// subcommand, so a hundred-thousand-window interval is emitted in batches
// instead of being materialized as one whole listing.
const rangeBatchSize = 1024

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// command holds one parsed command line: the subcommand and the flag
// values it runs with.
type command struct {
	sub            string
	files          []string
	window         string
	from, to       string
	hasFrom, hasTo bool
}

// run parses args, reads the input file or files, and writes the window
// listing. It returns the process exit code; every diagnostic is a single
// line on stderr prefixed with "rollupctl: ".
func run(args []string, stdout, stderr io.Writer) int {
	cmd, ok := parseArgs(args, stderr)
	if !ok {
		return 2
	}
	d, err := time.ParseDuration(cmd.window)
	if err != nil {
		argError(stderr, "invalid --window %q: %v", cmd.window, err)
		return 2
	}
	if d <= 0 {
		argError(stderr, "--window must be positive, got %s", d)
		return 2
	}

	switch cmd.sub {
	case "range":
		from, err := strconv.ParseInt(cmd.from, 10, 64)
		if err != nil {
			argError(stderr, "invalid --from %q: %v", cmd.from, err)
			return 2
		}
		to, err := strconv.ParseInt(cmd.to, 10, 64)
		if err != nil {
			argError(stderr, "invalid --to %q: %v", cmd.to, err)
			return 2
		}
		return runRange(cmd.files[0], d, from, to, stdout, stderr)
	case "merge":
		return runMerge(cmd.files[0], cmd.files[1], d, stdout, stderr)
	default:
		return runWindows(cmd.files[0], d, stdout, stderr)
	}
}

// runWindows prints every window of one file, in time order.
func runWindows(path string, d time.Duration, stdout, stderr io.Writer) int {
	roller, code := ingestFile(path, d, stderr)
	if code != 0 {
		return code
	}
	out := bufio.NewWriter(stdout)
	writeWindows(out, roller.Windows())
	return flushOutput(out, stderr)
}

// runRange prints the windows overlapping the half-open interval
// [from, to), in time order, advancing through an existing-API cursor one
// batch at a time. The interval endpoints are attributed to windows by the
// roller's own half-open rule. Equal endpoints, a backwards interval, and
// an interval no window overlaps are not errors and print nothing.
func runRange(path string, d time.Duration, from, to int64, stdout, stderr io.Writer) int {
	roller, code := ingestFile(path, d, stderr)
	if code != 0 {
		return code
	}
	out := bufio.NewWriter(stdout)
	cursor := roller.Cursor(time.Unix(0, from).UTC(), time.Unix(0, to).UTC())
	for {
		batch := cursor.Next(rangeBatchSize)
		if len(batch) == 0 {
			break
		}
		writeWindows(out, batch)
	}
	return flushOutput(out, stderr)
}

// runMerge aggregates two files with one window size and prints the
// combined listing. Both files are ingested before any window line is
// written, so a problem in either file rejects the whole batch: no line is
// printed on any failure.
func runMerge(pathA, pathB string, d time.Duration, stdout, stderr io.Writer) int {
	rollerA, code := ingestFile(pathA, d, stderr)
	if code != 0 {
		return code
	}
	rollerB, code := ingestFile(pathB, d, stderr)
	if code != 0 {
		return code
	}
	// With one --window both rollers necessarily share the width, but keep
	// the existing mismatch contract: if they ever did not, refuse the
	// whole batch in one diagnostic naming both files.
	if err := rollerA.Merge(rollerB); err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s and %s: %v\n", pathA, pathB, err)
		return 4
	}
	out := bufio.NewWriter(stdout)
	writeWindows(out, rollerA.Windows())
	return flushOutput(out, stderr)
}

// ingestFile opens one input file and streams its samples into a fresh
// roller. It returns exit code 3 with one diagnostic when the file is
// missing or a directory, and delegates invalid contents to ingest.
func ingestFile(path string, d time.Duration, stderr io.Writer) (*rollup.Roller, int) {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return nil, 3
	}
	if info.IsDir() {
		fmt.Fprintf(stderr, "rollupctl: %s: is a directory\n", path)
		return nil, 3
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return nil, 3
	}
	roller, code := ingest(f, path, d, stderr)
	f.Close()
	if code != 0 {
		return nil, code
	}
	return roller, 0
}

// writeWindows prints windows one line each in the existing window-output
// shape: start, count, sum, minimum, maximum.
func writeWindows(out *bufio.Writer, ws []rollup.Window) {
	for _, w := range ws {
		fmt.Fprintf(out, "%s %d %s %s %s\n",
			unixNanoText(w.Start), w.Count,
			formatFloat(w.Sum), formatFloat(w.Min), formatFloat(w.Max))
	}
}

// flushOutput flushes buffered output; a write failure is exit code 1.
func flushOutput(out *bufio.Writer, stderr io.Writer) int {
	if err := out.Flush(); err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 1
	}
	return 0
}

// argError writes one command-line usage diagnostic.
func argError(stderr io.Writer, format string, a ...any) {
	fmt.Fprintf(stderr, "rollupctl: "+format+"\n", a...)
}

// parseArgs extracts the subcommand and its flags. The subcommand is
// windows, range, or merge. windows and range take exactly one --file,
// merge exactly two; range additionally requires --from and --to, and no
// subcommand accepts the other's flags. Flags may stand on either side of
// the subcommand, Go-style: --name value, --name=value, with one or two
// leading dashes.
func parseArgs(args []string, stderr io.Writer) (command, bool) {
	fail := func(format string, a ...any) (command, bool) {
		argError(stderr, format, a...)
		return command{}, false
	}
	var cmd command
	sub := ""
	flagsEnded := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !flagsEnded && a == "--" {
			flagsEnded = true
			continue
		}
		if flagsEnded || !strings.HasPrefix(a, "-") || a == "-" {
			if sub != "" || (a != "windows" && a != "range" && a != "merge") {
				return fail("unexpected argument %q", a)
			}
			sub = a
			continue
		}
		name := a[1:]
		if strings.HasPrefix(name, "-") {
			name = name[1:]
		}
		value := ""
		inline := false
		if k := strings.IndexByte(name, '='); k >= 0 {
			value = name[k+1:]
			name = name[:k]
			inline = true
		}
		switch name {
		case "file", "window", "from", "to":
			if !inline {
				i++
				if i >= len(args) {
					return fail("flag --%s needs a value", name)
				}
				value = args[i]
			}
			if value == "" {
				return fail("flag --%s needs a value", name)
			}
			switch name {
			case "file":
				cmd.files = append(cmd.files, value)
			case "window":
				cmd.window = value
			case "from":
				cmd.from, cmd.hasFrom = value, true
			case "to":
				cmd.to, cmd.hasTo = value, true
			}
		default:
			return fail("unexpected argument %q", a)
		}
	}
	cmd.sub = sub
	if sub == "" {
		return fail("expected a subcommand: windows, range, or merge")
	}
	if cmd.window == "" {
		return fail("--window is required")
	}
	switch sub {
	case "merge":
		if cmd.hasFrom || cmd.hasTo {
			return fail("--from and --to are only valid with the range subcommand")
		}
		if len(cmd.files) != 2 {
			return fail("merge requires exactly two --file arguments, got %d", len(cmd.files))
		}
	case "range":
		if len(cmd.files) != 1 {
			return fail("range requires exactly one --file, got %d", len(cmd.files))
		}
		if !cmd.hasFrom {
			return fail("--from is required")
		}
		if !cmd.hasTo {
			return fail("--to is required")
		}
	default: // windows
		if cmd.hasFrom || cmd.hasTo {
			return fail("--from and --to are only valid with the range subcommand")
		}
		// windows keeps its historical last-value-wins behavior for a
		// repeated --file; only an absent --file is rejected, as before.
		if len(cmd.files) == 0 {
			return fail("--file is required")
		}
		cmd.files = cmd.files[len(cmd.files)-1:]
	}
	return cmd, true
}

// ingest streams the samples from in into a fresh roller. On any content
// problem it returns code 4 after writing one diagnostic naming the file
// and line; no sample lines are printed by the caller in that case.
func ingest(in io.Reader, name string, d time.Duration, stderr io.Writer) (*rollup.Roller, int) {
	contentError := func(line int, reason string) (*rollup.Roller, int) {
		fmt.Fprintf(stderr, "rollupctl: %s:%d: %s\n", name, line, reason)
		return nil, 4
	}

	// The line buffer is capped at one byte over the limit, so a longer
	// line fails immediately instead of growing with the file.
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, maxLineBytes+1), maxLineBytes+1)

	roller := rollup.New(d)
	line := 0
	samples := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if strings.TrimSpace(text) == "" {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return contentError(line, "expected a nanosecond timestamp and a value separated by whitespace")
		}
		ns, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid timestamp %q", fields[0]))
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid value %q", fields[1]))
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return contentError(line, "value must be finite")
		}
		if err := roller.Add(time.Unix(0, ns).UTC(), value); err != nil {
			return contentError(line, err.Error())
		}
		samples++
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			// The over-long line is the one after every line already
			// delivered, including an unterminated last line.
			return contentError(line+1, "line is longer than 1024 bytes")
		}
		fmt.Fprintf(stderr, "rollupctl: %s: %v\n", name, err)
		return nil, 3
	}
	if samples == 0 {
		return contentError(0, "no samples found")
	}
	return roller, 0
}

// formatFloat writes a float64 in its shortest round-trippable form; a
// negative zero keeps its sign and renders as -0.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

var bigBillion = big.NewInt(1e9)

// unixNanoText writes a window start as a signed integer count of
// nanoseconds since the epoch even when it falls outside the int64 range,
// so a start aligned past it is printed exactly instead of clamped.
func unixNanoText(t time.Time) string {
	n := new(big.Int).Mul(big.NewInt(t.Unix()), bigBillion)
	n.Add(n, big.NewInt(int64(t.Nanosecond())))
	return n.String()
}
