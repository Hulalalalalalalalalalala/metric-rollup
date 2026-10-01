// Command rollupctl reads timestamped samples from a file, files them into
// fixed windows aligned to the Unix epoch, and prints, one per line, each
// window's start, count, sum, minimum, and maximum.
//
// Usage:
//
//	rollupctl --file <path> --window <duration> windows
//	rollupctl --file <path> --window <duration> --from <ns> --to <ns> range
//	rollupctl --file <path> --file <path> --window <duration> merge
//	rollupctl --file <path> --window <duration> --from <ns> --to <ns> rates
//	rollupctl --file <path> --window <duration> --max-series <n> --overflow aggregate|reject series
//
// The input file holds one sample per line: an integer number of
// nanoseconds since the epoch, whitespace, then a floating-point value.
// Blank and whitespace-only lines carry no sample. Samples must arrive in
// non-decreasing time order. The values given to rates are cumulative
// counter readings and must be finite and non-negative. The series
// subcommand adds a third field, a JSON object whose string pairs label
// the series: "ns value {\"k\":\"v\"}".
//
// The windows subcommand prints every window. The range subcommand prints
// only the windows overlapping the half-open interval [--from, --to), both
// endpoints given as nanoseconds since the epoch. The merge subcommand
// aggregates two files with the same window duration and prints their
// windows combined into one listing. The rates subcommand treats the
// values as a cumulative counter that may reset and prints, for every
// epoch-aligned window overlapping [--from, --to) that holds at least one
// increment, its start, increment count, increment sum, minimum, maximum,
// and rate (the increment sum divided by the window width in seconds).
// The series subcommand files labeled samples into per-series windows,
// bounds the number of distinct series, and prints one
// metric-rollup/series-report/v1 JSON document.
//
// Exit codes: 0 success, 2 bad command-line arguments, 3 an input file
// does not exist or is a directory, 4 an input file's contents are
// invalid, including a negative counter value, a sample rejected under a
// reject overflow policy, or the two files cannot be merged.
package main

import (
	"bufio"
	"encoding/json"
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

// rangeBatchSize is the number of windows the range subcommand asks the
// cursor for at a time, so printing a long interval streams in batches
// instead of materializing one listing per window.
const rangeBatchSize = 1024

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// command holds the parsed command line: which subcommand to run and the
// flag values it consumes.
type command struct {
	sub            string
	files          []string
	window         string
	from, to       string
	hasFrom, hasTo bool
	maxSeries      string
	hasMaxSeries   bool
	overflow       string
	hasOverflow    bool
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
		return runRange(cmd.files[len(cmd.files)-1], d, from, to, stdout, stderr)
	case "rates":
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
		return runRates(cmd.files[len(cmd.files)-1], d, from, to, stdout, stderr)
	case "merge":
		return runMerge(cmd.files[0], cmd.files[1], d, stdout, stderr)
	case "series":
		maxSeries, err := strconv.Atoi(cmd.maxSeries)
		if err != nil {
			argError(stderr, "invalid --max-series %q: %v", cmd.maxSeries, err)
			return 2
		}
		if maxSeries <= 0 {
			argError(stderr, "--max-series must be positive, got %d", maxSeries)
			return 2
		}
		switch cmd.overflow {
		case "aggregate":
			return runSeries(cmd.files[len(cmd.files)-1], d, maxSeries, rollup.OverflowAggregate, stdout, stderr)
		case "reject":
			return runSeries(cmd.files[len(cmd.files)-1], d, maxSeries, rollup.OverflowReject, stdout, stderr)
		default:
			argError(stderr, "invalid --overflow %q: want aggregate or reject", cmd.overflow)
			return 2
		}
	default:
		return runWindows(cmd.files[len(cmd.files)-1], d, stdout, stderr)
	}
}

// runWindows prints every window of one file's samples.
func runWindows(path string, d time.Duration, stdout, stderr io.Writer) int {
	roller, code := ingestFile(path, d, stderr)
	if code != 0 {
		return code
	}
	out := bufio.NewWriter(stdout)
	writeWindows(out, roller.Windows())
	return flush(out, stderr)
}

// runRange prints the windows of one file's samples that overlap the
// half-open interval [from, to), advancing through them in cursor batches.
// An empty or backwards interval is not an error and prints nothing.
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
	return flush(out, stderr)
}

// runRates treats one file's values as a cumulative counter and prints
// the rate windows that overlap the half-open interval [from, to),
// advancing through them in cursor batches. An empty or backwards
// interval is not an error and prints nothing.
func runRates(path string, d time.Duration, from, to int64, stdout, stderr io.Writer) int {
	counter, code := ingestCounterFile(path, d, stderr)
	if code != 0 {
		return code
	}
	out := bufio.NewWriter(stdout)
	cursor := counter.RateCursor(time.Unix(0, from).UTC(), time.Unix(0, to).UTC())
	for {
		batch := cursor.Next(rangeBatchSize)
		if len(batch) == 0 {
			break
		}
		writeRates(out, batch)
	}
	return flush(out, stderr)
}

// writeRates prints one line per rate window: start, count, sum, min, max,
// rate.
func writeRates(out *bufio.Writer, ws []rollup.RateWindow) {
	for _, w := range ws {
		fmt.Fprintf(out, "%s %d %s %s %s %s\n",
			unixNanoText(w.Start), w.Count,
			formatFloat(w.Sum), formatFloat(w.Min), formatFloat(w.Max),
			formatFloat(w.Rate))
	}
}

// runMerge aggregates two files with the same window duration and prints
// their combined windows. Both files are fully ingested before anything is
// printed, so a problem in either one leaves the listing empty.
func runMerge(pathA, pathB string, d time.Duration, stdout, stderr io.Writer) int {
	rollerA, code := ingestFile(pathA, d, stderr)
	if code != 0 {
		return code
	}
	rollerB, code := ingestFile(pathB, d, stderr)
	if code != 0 {
		return code
	}
	if err := rollerA.Merge(rollerB); err != nil {
		writeMergeError(stderr, pathA, pathB, err)
		return 4
	}
	out := bufio.NewWriter(stdout)
	writeWindows(out, rollerA.Windows())
	return flush(out, stderr)
}

// seriesReport is the on-disk metric-rollup/series-report/v1 document.
type seriesReport struct {
	Schema      string            `json:"schema"`
	WindowNS    int64             `json:"window_ns"`
	Series      []seriesEntry     `json:"series"`
	Cardinality cardinalityOutput `json:"cardinality"`
}

// seriesEntry is one reported series: its labels, whether it is the
// overflow bucket, and its windows.
type seriesEntry struct {
	Labels   map[string]string `json:"labels"`
	Overflow bool              `json:"overflow"`
	Windows  []seriesWindowOut `json:"windows"`
}

// seriesWindowOut is one window of one series: start in nanoseconds since
// the epoch, count, sum, minimum, and maximum.
type seriesWindowOut struct {
	StartNS json.Number `json:"start_ns"`
	Count   int64       `json:"count"`
	Sum     jsonFloat   `json:"sum"`
	Min     jsonFloat   `json:"min"`
	Max     jsonFloat   `json:"max"`
}

// cardinalityOutput is the report's cardinality object.
type cardinalityOutput struct {
	AcceptedSeries  int   `json:"accepted_series"`
	OverflowSeries  int   `json:"overflow_series"`
	RejectedSamples int64 `json:"rejected_samples"`
}

// jsonFloat is a float64 that marshals in its shortest round-trippable
// form; a non-finite sum a finite sample set rounded to emits null, the
// only JSON token that can stand for it.
type jsonFloat float64

// MarshalJSON implements json.Marshaler.
func (f jsonFloat) MarshalJSON() ([]byte, error) {
	v := float64(f)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatFloat(v, 'g', -1, 64)), nil
}

// runSeries ingests one labeled-sample file and prints one
// metric-rollup/series-report/v1 document. The whole file is ingested
// before anything is printed, so a bad field or a rejected sample leaves
// stdout empty.
func runSeries(path string, d time.Duration, maxSeries int, policy rollup.OverflowPolicy, stdout, stderr io.Writer) int {
	f, code := openInput(path, stderr)
	if code != 0 {
		return code
	}
	defer f.Close()
	set := rollup.NewSeriesSet(d, maxSeries, policy)
	if code := ingestSeriesInto(f, path, set, stderr); code != 0 {
		return code
	}
	series, card := set.Snapshot()
	doc := seriesReport{
		Schema:   "metric-rollup/series-report/v1",
		WindowNS: d.Nanoseconds(),
		Series:   make([]seriesEntry, 0, len(series)),
		Cardinality: cardinalityOutput{
			AcceptedSeries:  card.AcceptedSeries,
			OverflowSeries:  card.OverflowSeries,
			RejectedSamples: card.RejectedSamples,
		},
	}
	for _, sw := range series {
		entry := seriesEntry{
			Labels:   sw.Labels,
			Overflow: sw.Overflow,
			Windows:  make([]seriesWindowOut, 0, len(sw.Windows)),
		}
		for _, w := range sw.Windows {
			entry.Windows = append(entry.Windows, seriesWindowOut{
				StartNS: json.Number(unixNanoText(w.Start)),
				Count:   w.Count,
				Sum:     jsonFloat(w.Sum),
				Min:     jsonFloat(w.Min),
				Max:     jsonFloat(w.Max),
			})
		}
		doc.Series = append(doc.Series, entry)
	}
	// Marshal before writing so an encoding failure never leaves a
	// partial document on stdout.
	data, err := json.Marshal(doc)
	if err != nil {
		fmt.Fprintf(stderr, "rollupctl: %s\n", err)
		return 1
	}
	out := bufio.NewWriter(stdout)
	out.Write(data)
	out.WriteByte('\n')
	return flush(out, stderr)
}

// writeWindows prints one line per window: start, count, sum, min, max.
func writeWindows(out *bufio.Writer, ws []rollup.Window) {
	for _, w := range ws {
		fmt.Fprintf(out, "%s %d %s %s %s\n",
			unixNanoText(w.Start), w.Count,
			formatFloat(w.Sum), formatFloat(w.Min), formatFloat(w.Max))
	}
}

// flush flushes out and reports a write failure as exit code 1.
func flush(out *bufio.Writer, stderr io.Writer) int {
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

// writeMergeError writes the one merge diagnostic: the two file names in
// command-line order, separated by a comma and a space, followed by the
// reason the merge could not combine them.
func writeMergeError(stderr io.Writer, pathA, pathB string, err error) {
	fmt.Fprintf(stderr, "rollupctl: %s, %s: %v\n", pathA, pathB, err)
}

// parseArgs extracts the subcommand and the flag values. Flags may stand
// on either side of the subcommand, Go-style: --name value, --name=value,
// with one or two leading dashes.
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
			if sub != "" || (a != "windows" && a != "range" && a != "rates" && a != "merge" && a != "series") {
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
		case "file", "window", "from", "to", "max-series", "overflow":
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
			case "max-series":
				cmd.maxSeries, cmd.hasMaxSeries = value, true
			case "overflow":
				cmd.overflow, cmd.hasOverflow = value, true
			}
		default:
			return fail("unexpected argument %q", a)
		}
	}
	cmd.sub = sub
	if sub == "" {
		return fail("expected a subcommand: windows, range, rates, merge, or series")
	}
	if len(cmd.files) == 0 {
		return fail("--file is required")
	}
	switch sub {
	case "merge":
		if cmd.hasFrom || cmd.hasTo {
			return fail("--from and --to belong to the range subcommand")
		}
		if cmd.hasMaxSeries || cmd.hasOverflow {
			return fail("--max-series and --overflow belong to the series subcommand")
		}
		if len(cmd.files) != 2 {
			return fail("merge takes exactly two --file arguments, got %d", len(cmd.files))
		}
	case "range", "rates":
		if cmd.hasMaxSeries || cmd.hasOverflow {
			return fail("--max-series and --overflow belong to the series subcommand")
		}
		if len(cmd.files) != 1 {
			return fail("%s takes exactly one --file argument, got %d", sub, len(cmd.files))
		}
		if !cmd.hasFrom {
			return fail("--from is required")
		}
		if !cmd.hasTo {
			return fail("--to is required")
		}
	case "series":
		if cmd.hasFrom || cmd.hasTo {
			return fail("--from and --to belong to the range subcommand")
		}
		if len(cmd.files) != 1 {
			return fail("series takes exactly one --file argument, got %d", len(cmd.files))
		}
		if !cmd.hasMaxSeries {
			return fail("--max-series is required")
		}
		if !cmd.hasOverflow {
			return fail("--overflow is required")
		}
	default: // windows
		if cmd.hasFrom || cmd.hasTo {
			return fail("--from and --to belong to the range subcommand")
		}
		if cmd.hasMaxSeries || cmd.hasOverflow {
			return fail("--max-series and --overflow belong to the series subcommand")
		}
	}
	if cmd.window == "" {
		return fail("--window is required")
	}
	return cmd, true
}

// ingester is the common ingestion surface of a Roller and a Counter:
// both file one validated sample at a time.
type ingester interface {
	Add(at time.Time, value float64) error
}

// ingestFile opens one input file and streams its samples into a fresh
// roller. It returns a non-zero exit code after writing one diagnostic if
// the file cannot be opened or its contents are invalid.
func ingestFile(path string, d time.Duration, stderr io.Writer) (*rollup.Roller, int) {
	f, code := openInput(path, stderr)
	if code != 0 {
		return nil, code
	}
	defer f.Close()
	roller := rollup.New(d)
	if code := ingestInto(f, path, roller, stderr); code != 0 {
		return nil, code
	}
	return roller, 0
}

// ingestCounterFile opens one input file and streams its readings into a
// fresh counter, by the same file and content rules as ingestFile.
func ingestCounterFile(path string, d time.Duration, stderr io.Writer) (*rollup.Counter, int) {
	f, code := openInput(path, stderr)
	if code != 0 {
		return nil, code
	}
	defer f.Close()
	counter := rollup.NewCounter(d)
	if code := ingestInto(f, path, counter, stderr); code != 0 {
		return nil, code
	}
	return counter, 0
}

// openInput opens one input file after confirming it exists and is not a
// directory. It returns exit code 3 after writing one diagnostic
// otherwise.
func openInput(path string, stderr io.Writer) (*os.File, int) {
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
	return f, 0
}

// ingestInto streams the records from in into dst. On any content problem
// it returns code 4 after writing one diagnostic naming the file and line;
// no output lines are printed by the caller in that case.
func ingestInto(in io.Reader, name string, dst ingester, stderr io.Writer) int {
	contentError := func(line int, reason string) int {
		fmt.Fprintf(stderr, "rollupctl: %s:%d: %s\n", name, line, reason)
		return 4
	}

	// The line buffer is capped at one byte over the limit, so a longer
	// line fails immediately instead of growing with the file.
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, maxLineBytes+1), maxLineBytes+1)

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
		if err := dst.Add(time.Unix(0, ns).UTC(), value); err != nil {
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
		return 3
	}
	if samples == 0 {
		return contentError(0, "no samples found")
	}
	return 0
}

// seriesIngester is the ingestion surface of a SeriesSet.
type seriesIngester interface {
	Add(sample rollup.LabeledSample) error
}

// ingestSeriesInto streams labeled records from in into dst. Each line is
// a nanosecond timestamp, a finite float value, and a JSON object of
// string pairs, separated by whitespace; the JSON object may itself
// contain whitespace, so only the first two fields are split by
// whitespace and the remainder is parsed as JSON. On any content problem
// it returns code 4 after writing one diagnostic naming the file and
// line; no output is printed by the caller in that case.
func ingestSeriesInto(in io.Reader, name string, dst seriesIngester, stderr io.Writer) int {
	contentError := func(line int, reason string) int {
		fmt.Fprintf(stderr, "rollupctl: %s:%d: %s\n", name, line, reason)
		return 4
	}

	// The line buffer is capped at one byte over the limit, so a longer
	// line fails immediately instead of growing with the file.
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, maxLineBytes+1), maxLineBytes+1)

	line := 0
	samples := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if strings.TrimSpace(text) == "" {
			continue
		}
		rest := strings.TrimLeft(text, " \t")
		i := strings.IndexAny(rest, " \t")
		if i < 0 {
			return contentError(line, "expected a nanosecond timestamp, a value, and a JSON label object separated by whitespace")
		}
		nsText := rest[:i]
		rest = strings.TrimLeft(rest[i:], " \t")
		j := strings.IndexAny(rest, " \t")
		if j < 0 {
			return contentError(line, "expected a value and a JSON label object after the timestamp")
		}
		valueText := rest[:j]
		labelsText := strings.TrimSpace(rest[j:])

		ns, err := strconv.ParseInt(nsText, 10, 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid timestamp %q", nsText))
		}
		value, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid value %q", valueText))
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return contentError(line, "value must be finite")
		}
		labels, err := parseLabelObject(labelsText)
		if err != nil {
			return contentError(line, fmt.Sprintf("invalid labels: %v", err))
		}
		if err := dst.Add(rollup.LabeledSample{
			At:     time.Unix(0, ns).UTC(),
			Value:  value,
			Labels: labels,
		}); err != nil {
			return contentError(line, err.Error())
		}
		samples++
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return contentError(line+1, "line is longer than 1024 bytes")
		}
		fmt.Fprintf(stderr, "rollupctl: %s: %v\n", name, err)
		return 3
	}
	if samples == 0 {
		return contentError(0, "no samples found")
	}
	return 0
}

// parseLabelObject parses one JSON object whose keys and values are both
// strings. It rejects anything that is not exactly one such object:
// arrays, scalars, null, trailing data, duplicate keys, and non-string
// values. The empty object is legal and yields an empty non-nil map.
func parseLabelObject(text string) (map[string]string, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, errors.New("labels must be a JSON object")
	}
	labels := make(map[string]string)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, errors.New("label keys must be strings")
		}
		var raw any
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		value, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("label %q must have a string value", key)
		}
		if _, dup := labels[key]; dup {
			return nil, fmt.Errorf("duplicate label key %q", key)
		}
		labels[key] = value
	}
	if closeTok, err := dec.Token(); err != nil || closeTok != json.Delim('}') {
		return nil, errors.New("malformed label object")
	}
	if tok, err := dec.Token(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		_ = tok
		return nil, errors.New("trailing data after label object")
	}
	return labels, nil
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
