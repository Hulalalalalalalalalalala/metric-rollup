// Command rollupctl prints the window rollups of a sample file.
//
// Usage:
//
//	rollupctl --file <path> --window <duration> windows
//
// The file holds one sample per line: an integer nanosecond timestamp
// since the Unix epoch, whitespace, then a floating-point value. Blank
// lines are skipped. Samples must arrive in non-decreasing time order.
//
// The exit code is 2 for argument problems, 3 when the file cannot be
// read, 4 when its content is invalid, and 0 on success.
package main

import (
	"bufio"
	"errors"
	"flag"
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

// maxLineBytes bounds one input line, not counting its newline; longer
// lines are rejected as invalid content.
const maxLineBytes = 1024

// errTooLong reports an input line past maxLineBytes.
var errTooLong = errors.New("line too long")

func main() {
	os.Exit(run(os.Args[1:]))
}

// run executes the command and returns the process exit code.
func run(args []string) int {
	fs := flag.NewFlagSet("rollupctl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	file := fs.String("file", "", "input sample file")
	window := fs.String("window", "", "window size as a Go duration")
	if err := fs.Parse(args); err != nil {
		return fail(2, "%v", err)
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fail(2, "missing subcommand: windows")
	}
	if len(rest) != 1 || rest[0] != "windows" {
		return fail(2, "unexpected arguments: %s", strings.Join(rest, " "))
	}
	if *file == "" {
		return fail(2, "missing --file")
	}
	if *window == "" {
		return fail(2, "missing --window")
	}
	d, err := time.ParseDuration(*window)
	if err != nil {
		return fail(2, "bad --window: %v", err)
	}
	if d <= 0 {
		return fail(2, "window must be positive")
	}

	f, err := os.Open(*file)
	if err != nil {
		return fail(3, "%v", err)
	}
	defer f.Close()
	if st, err := f.Stat(); err != nil {
		return fail(3, "%s: %v", *file, err)
	} else if st.IsDir() {
		return fail(3, "%s: is a directory", *file)
	}

	roll := rollup.New(d)
	line, err := load(f, roll)
	if err != nil {
		if line < 0 {
			// An I/O failure, not bad content.
			return fail(3, "%s: %v", *file, err)
		}
		return fail(4, "%s:%d: %v", *file, line, err)
	}

	out := bufio.NewWriter(os.Stdout)
	for _, w := range roll.Windows() {
		fmt.Fprintf(out, "%s %d %s %s %s\n",
			epochNanos(w.Start), w.Count,
			strconv.FormatFloat(w.Sum, 'g', -1, 64),
			strconv.FormatFloat(w.Min, 'g', -1, 64),
			strconv.FormatFloat(w.Max, 'g', -1, 64))
	}
	out.Flush()
	return 0
}

// fail prints a single diagnostic line to stderr and returns the exit
// code it was called with.
func fail(code int, format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "rollupctl: "+format+"\n", args...)
	return code
}

// load streams samples from r into roll, one line at a time. On a
// content problem it returns the offending 1-based line number and the
// reason; a file with no samples at all reports line 0. A line number
// of -1 marks an I/O failure rather than invalid content.
func load(r io.Reader, roll *rollup.Roller) (int, error) {
	br := bufio.NewReader(r)
	line, samples := 0, 0
	for {
		raw, rerr := readLine(br)
		if rerr == io.EOF && len(raw) == 0 {
			break
		}
		line++
		if rerr == errTooLong {
			return line, errTooLong
		}
		ok, perr := parseLine(raw, roll)
		if perr != nil {
			return line, perr
		}
		if ok {
			samples++
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return -1, rerr
		}
	}
	if samples == 0 {
		return 0, errors.New("no samples")
	}
	return 0, nil
}

// readLine returns the next line from br without its trailing newline.
// A line longer than maxLineBytes is rejected with errTooLong, so the
// memory held per line stays bounded no matter how the input is shaped.
func readLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		frag, err := br.ReadSlice('\n')
		line = append(line, frag...)
		if err == bufio.ErrBufferFull {
			if len(line) > maxLineBytes+1 {
				return nil, errTooLong
			}
			continue
		}
		if n := len(line); n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
		if len(line) > maxLineBytes {
			return nil, errTooLong
		}
		return line, err
	}
}

// parseLine files one input line into roll. It reports whether the line
// carried a sample; all-whitespace lines carry none and are skipped.
func parseLine(line []byte, roll *rollup.Roller) (bool, error) {
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return false, nil
	}
	if len(fields) != 2 {
		return false, fmt.Errorf("malformed line: want a timestamp and a value, got %d fields", len(fields))
	}
	ns, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return false, fmt.Errorf("bad timestamp %q", fields[0])
	}
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return false, fmt.Errorf("bad value %q", fields[1])
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return false, fmt.Errorf("non-finite value %q", fields[1])
	}
	if err := roll.Add(time.Unix(0, ns).UTC(), v); err != nil {
		return false, err
	}
	return true, nil
}

// epochNanos formats t as its integer nanosecond offset from the Unix
// epoch. The arithmetic is exact even when the offset does not fit in
// an int64, as can happen for a window start rounded down from a
// timestamp near math.MinInt64.
func epochNanos(t time.Time) string {
	sec := t.Unix()
	if sec >= -9223372036 && sec <= 9223372035 {
		return strconv.FormatInt(sec*1e9+int64(t.Nanosecond()), 10)
	}
	ns := big.NewInt(sec)
	ns.Mul(ns, big.NewInt(1e9))
	ns.Add(ns, big.NewInt(int64(t.Nanosecond())))
	return ns.String()
}
