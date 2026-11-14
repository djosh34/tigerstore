package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// A run can fail because the runner's one disk, shared by all three
// nodes, stalled: every node's log sync takes seconds at once, the leader
// goes silent by #233's failover rule (a leader whose disk write is stuck
// sends nothing to its peers until it is durable), and a new leader is
// elected once the disk returns. The failure is the runner's, not the
// store's. bench.yml's quick grid measures such a job's pair once more
// (retried); diskStall tells it which failures those are, by a strict
// test, and anything else stays a plain failure.

const (
	// stallSync is how long a log sync must take on every node, and
	// stallSkew how close their ends must lie, to be one stall of the
	// shared disk.
	stallSync = time.Second
	stallSkew = 250 * time.Millisecond
	// silentRule is what a leader logs when #233's failover rule silences
	// it (internal/node).
	silentRule = "sending nothing to peers until it is durable"
)

// errNotDiskStall is the error of a failed run that is not a stall of the
// runner's disk.
var errNotDiskStall = errors.New("not a stall of the runner's disk")

// logLine is a node log's line: when, and its message.
type logLine struct {
	at  time.Time
	msg string
}

// readLog reads a node's log: zap's JSON lines, of which others are
// skipped.
func readLog(path string) ([]logLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // a read-only file
	var out []logLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var e struct {
			TS  string `json:"ts"`
			Msg string `json:"msg"`
		}
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, e.TS)
		if err != nil {
			continue
		}
		out = append(out, logLine{at, e.Msg})
	}
	return out, sc.Err()
}

// slowSyncEnds are the times a node's log syncs of at least stallSync
// ended.
func slowSyncEnds(lines []logLine) []time.Time {
	var ends []time.Time
	for _, l := range lines {
		d, ok := strings.CutPrefix(l.msg, "slow log sync: ")
		if !ok {
			continue
		}
		if dur, err := time.ParseDuration(d); err == nil && dur >= stallSync {
			ends = append(ends, l.at)
		}
	}
	return ends
}

// diskStall reports whether the failed run whose files start with prefix
// (<dir>/<target>-<mode>-<seed>.<rep>) failed by a stall of the runner's
// disk: its cause is a leader change; one node logged #233's silent rule;
// every node's log sync took stallSync or more, all ending within
// stallSkew of each other; and a node logged a new leader after that.
// It returns the stall's end, or errNotDiskStall with why not.
func diskStall(prefix string) (time.Time, error) {
	cause, err := os.ReadFile(prefix + ".failed")
	if err != nil {
		return time.Time{}, err
	}
	if !strings.Contains(string(cause), "the leader changed") {
		return time.Time{}, fmt.Errorf("%w: the cause is %q", errNotDiskStall, strings.TrimSpace(string(cause)))
	}
	logs, err := filepath.Glob(prefix + "-n*.log")
	if err != nil {
		return time.Time{}, err
	}
	if len(logs) < 3 {
		return time.Time{}, fmt.Errorf("%w: %d node logs", errNotDiskStall, len(logs))
	}
	var nodes [][]logLine
	silent := false
	for _, p := range logs {
		lines, err := readLog(p)
		if err != nil {
			return time.Time{}, err
		}
		nodes = append(nodes, lines)
		silent = silent || slices.ContainsFunc(lines, func(l logLine) bool { return strings.Contains(l.msg, silentRule) })
	}
	if !silent {
		return time.Time{}, fmt.Errorf("%w: no leader logged %q", errNotDiskStall, silentRule)
	}
	// A stall is a slow sync of the first node that every other node
	// matches: its own slow sync ending within stallSkew.
	var end time.Time
	for _, e := range slowSyncEnds(nodes[0]) {
		all := true
		for _, other := range nodes[1:] {
			if !slices.ContainsFunc(slowSyncEnds(other), func(o time.Time) bool { return o.Sub(e).Abs() <= stallSkew }) {
				all = false
				break
			}
		}
		if all {
			end = e
		}
	}
	if end.IsZero() {
		return time.Time{}, fmt.Errorf("%w: no log sync of %v or more ended on every node within %v", errNotDiskStall, stallSync, stallSkew)
	}
	for _, lines := range nodes {
		for _, l := range lines {
			if l.at.After(end.Add(-stallSkew)) && strings.HasPrefix(l.msg, "leader is ") && !strings.HasPrefix(l.msg, "leader is 0 ") {
				return end, nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("%w: no new leader after the stall that ended at %v", errNotDiskStall, end.Format(time.RFC3339Nano))
}

// diskStalls checks every failed run under dir (rtt-*/<bench>/*.failed)
// with diskStall, writes a line on each to w, and returns an error unless
// there is at least one and every one is a stall of the runner's disk.
func diskStalls(w io.Writer, dir string) error {
	failed, err := filepath.Glob(filepath.Join(dir, "rtt-*", "*", "*.failed"))
	if err != nil {
		return err
	}
	if len(failed) == 0 {
		return fmt.Errorf("%w: no failed run in %s", errNotDiskStall, dir)
	}
	var problems []error
	for _, f := range failed {
		prefix := strings.TrimSuffix(f, ".failed")
		end, err := diskStall(prefix)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", prefix, err))
			_, _ = fmt.Fprintf(w, "%s: %v\n", prefix, err)
			continue
		}
		_, _ = fmt.Fprintf(w, "%s: the runner's disk stalled on every node until %s, and the leader went silent by #233's failover rule\n", prefix, end.Format(time.RFC3339Nano))
	}
	return errors.Join(problems...)
}
