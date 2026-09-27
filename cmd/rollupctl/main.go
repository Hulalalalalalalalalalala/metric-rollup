// Command rollupctl reads timestamped samples from a file, files them into
// fixed windows aligned to the Unix epoch, and prints, one per line, each
// window's start, count, sum, minimum, and maximum.
//
// Usage:
//
//	rollupctl --file <path> --window <duration> windows
//
// The input file holds one sample per line: an integer number of
// nanoseconds since the epoch, whitespace, then a floating-point value.
// Blank and whitespace-only lines carry no sample. Samples must arrive in
// non-decreasing time order.
//
// Exit codes: 0 success, 2 bad command-line arguments, 3 the input file
// does not exist or is a directory, 4 the input file's contents are
// invalid.
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

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses args, reads the input file, and writes the window listing.
// It returns the process exit code; every diagnostic is a single line on
// stderr prefixed with "rollupctl: ".
func run(args []string, stdout, stderr io.Writer) int {
	file, window, ok := parseArgs(args, stderr)
	if !ok {
		return 2
	}
	d, err := time.ParseDuration(window)
	if err != nil {
		argError(stderr, "invalid --window %q: %v", window, err)
		return 2
	}
	if d <= 0 {
		argError(stderr, "--window must be positive, got %s", d)
		return 2
	}

	info, err := os.Stat(file)
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 3
	}
	if info.IsDir() {
		fmt.Fprintf(stderr, "rollupctl: %s: is a directory\n", file)
		return 3
	}
	f, err := os.Open(file)
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 3
	}
	roller, code := ingest(f, file, d, stderr)
	f.Close()
	if code != 0 {
		return code
	}

	out := bufio.NewWriter(stdout)
	for _, w := range roller.Windows() {
		fmt.Fprintf(out, "%s %d %s %s %s\n",
			unixNanoText(w.Start), w.Count,
			formatFloat(w.Sum), formatFloat(w.Min), formatFloat(w.Max))
	}
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

// parseArgs extracts the windows subcommand, the --file path, and the
// --window duration. Flags may stand on either side of the subcommand,
// Go-style: --name value, --name=value, with one or two leading dashes.
func parseArgs(args []string, stderr io.Writer) (file, window string, ok bool) {
	fail := func(format string, a ...any) (string, string, bool) {
		argError(stderr, format, a...)
		return "", "", false
	}
	sub := ""
	flagsEnded := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !flagsEnded && a == "--" {
			flagsEnded = true
			continue
		}
		if flagsEnded || !strings.HasPrefix(a, "-") || a == "-" {
			if a != "windows" || sub != "" {
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
		case "file", "window":
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
			if name == "file" {
				file = value
			} else {
				window = value
			}
		default:
			return fail("unexpected argument %q", a)
		}
	}
	if sub == "" {
		return fail("expected the windows subcommand")
	}
	if file == "" {
		return fail("--file is required")
	}
	if window == "" {
		return fail("--window is required")
	}
	return file, window, true
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
