package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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
	// stallElected is how soon after the stall the new leader must come:
	// the election the stall held back ends as soon as the votes are
	// durable (3 ms in A/A run 43625723837). A leader change later than
	// that is the store's, not the disk's.
	stallElected = 2 * time.Second
	// stallWrite bounds what a node may write in one flush or log write
	// during a stall of the runner's disk: a disk that takes seconds for
	// a few KiB stalled; one that takes seconds for many MiB may just be
	// slow, or the store wrote too much (A/A run 43625723837 wrote 20 B
	// to 3 KiB a write while it stalled).
	stallWrite = 1 << 20
	// silentRule is what a leader logs when #233's failover rule silences
	// it (internal/node), and wlSilent what internal/wl's logs.
	silentRule = "sending nothing to peers until it is durable"
	wlSilent   = "log's oldest append waited for the disk since "
)

// errNotDiskStall is the error of a failed run that is not a stall of the
// runner's disk.
var errNotDiskStall = errors.New("not a stall of the runner's disk")

// logLine is a node log's line: when, and its message.
type logLine struct {
	at  time.Time
	msg string
}

// wlLine is a line of internal/wl's logger: its time, in UTC, to the
// microsecond, and its message.
var wlLine = regexp.MustCompile(`^tigerstore-wl (\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d{6}) (.*)$`)

// readLog reads a node's log: zap's JSON lines and internal/wl's lines,
// of which others are skipped.
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
		if m := wlLine.FindSubmatch(sc.Bytes()); m != nil {
			if at, err := time.Parse("2006/01/02 15:04:05.000000", string(m[1])); err == nil {
				out = append(out, logLine{at, string(m[2])})
			}
			continue
		}
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

// slowSync is a log sync of a node that took stallSync or more: when it
// ended, and how long it took.
type slowSync struct {
	end time.Time
	dur time.Duration
}

// slowSyncs are a node's log syncs of stallSync or more.
func slowSyncs(lines []logLine) []slowSync {
	var out []slowSync
	for _, l := range lines {
		d, ok := strings.CutPrefix(l.msg, "slow log sync: ")
		if !ok {
			continue
		}
		if dur, err := time.ParseDuration(d); err == nil && dur >= stallSync {
			out = append(out, slowSync{l.at, dur})
		}
	}
	return out
}

// stallGroup is one stall of the shared disk: a slow sync of every node,
// their ends within stallSkew of each other; from the latest start of
// them to the latest end.
type stallGroup struct{ start, end time.Time }

// stallGroups are the stalls of the shared disk that the nodes' slow
// syncs show: for each slow sync of the first node, the slow sync of each
// other node that ended closest to it, if all their ends lie within
// stallSkew from first to last.
func stallGroups(nodes [][]logLine) []stallGroup {
	syncs := make([][]slowSync, len(nodes))
	for i, lines := range nodes {
		syncs[i] = slowSyncs(lines)
	}
	var groups []stallGroup
	for _, first := range syncs[0] {
		group := []slowSync{first}
		for _, other := range syncs[1:] {
			if len(other) == 0 {
				return nil
			}
			closest := slices.MinFunc(other, func(a, b slowSync) int {
				return cmp.Compare(a.end.Sub(first.end).Abs(), b.end.Sub(first.end).Abs())
			})
			group = append(group, closest)
		}
		lo := slices.MinFunc(group, func(a, b slowSync) int { return a.end.Compare(b.end) }).end
		hi := slices.MaxFunc(group, func(a, b slowSync) int { return a.end.Compare(b.end) }).end
		if hi.Sub(lo) > stallSkew {
			continue
		}
		var start time.Time
		for _, g := range group {
			if s := g.end.Add(-g.dur); s.After(start) {
				start = s
			}
		}
		groups = append(groups, stallGroup{start, hi})
	}
	return groups
}

// writeBytes is the size a node logs for a slow flush or log write.
var writeBytes = regexp.MustCompile(`^slow (?:log flush: |write to the log: \d+ entries, )(\d+) bytes`)

// smallWrites reports whether every node logged, from from to to, at
// least one slow flush or log write, and none of more than stallWrite. A
// write's size is logged just after the sync it waited on, so the window
// reaches stallSkew past the stall.
func smallWrites(nodes [][]logLine, from, to time.Time) bool {
	for _, lines := range nodes {
		seen := false
		for _, l := range lines {
			if l.at.Before(from) || l.at.After(to) {
				continue
			}
			m := writeBytes.FindStringSubmatch(l.msg)
			if m == nil {
				continue
			}
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil || n > stallWrite {
				return false
			}
			seen = true
		}
		if !seen {
			return false
		}
	}
	return true
}

// leaderTerm is the term of the leader a failed leader check found last
// (bench/cluster.sh check: "... now <endpoint> term <n>"), or "".
var leaderTerm = regexp.MustCompile(`now \S+ term (\d+)`)

// diskStall reports whether the failed run whose files start with prefix
// (<dir>/<target>-<mode>-<seed>.<rep>) failed by a stall of the runner's
// disk, as one episode: its cause is a leader change to a new term; the
// shared disk stalled (stallGroups); the leader went silent by #233's
// failover rule while it stalled; every node's flushes and log writes in
// the stall were small (smallWrites); and a node logged the new leader, at
// the term the check found, within stallElected after that stall ended. It returns the stall's
// end, or errNotDiskStall with why not.
func diskStall(prefix string) (time.Time, error) {
	cause, err := os.ReadFile(prefix + ".failed")
	if err != nil {
		return time.Time{}, err
	}
	m := leaderTerm.FindSubmatch(cause)
	if !strings.Contains(string(cause), "the leader changed") || m == nil {
		return time.Time{}, fmt.Errorf("%w: the cause is %q", errNotDiskStall, strings.TrimSpace(string(cause)))
	}
	term := string(m[1])
	logs, err := filepath.Glob(prefix + "-n*.log")
	if err != nil {
		return time.Time{}, err
	}
	if len(logs) < 3 {
		return time.Time{}, fmt.Errorf("%w: %d node logs", errNotDiskStall, len(logs))
	}
	var nodes [][]logLine
	var silences, leaders []time.Time
	for _, p := range logs {
		lines, err := readLog(p)
		if err != nil {
			return time.Time{}, err
		}
		nodes = append(nodes, lines)
		for _, l := range lines {
			// internal/wl's nodes word both lines their own way: "the
			// leader is", and wl's silent rule (wl/silent.go).
			msg := strings.TrimPrefix(l.msg, "the ")
			switch {
			case strings.Contains(msg, silentRule) || strings.HasPrefix(msg, wlSilent):
				silences = append(silences, l.at)
			case strings.HasPrefix(msg, "leader is ") && strings.HasSuffix(msg, " at term "+term) && !strings.HasPrefix(msg, "leader is 0 "):
				leaders = append(leaders, l.at)
			}
		}
	}
	groups := stallGroups(nodes)
	if len(groups) == 0 {
		return time.Time{}, fmt.Errorf("%w: no log sync of %v or more on every node, their ends within %v", errNotDiskStall, stallSync, stallSkew)
	}
	// A stall of the disk runs through groups one after another, a group
	// starting within stallSkew of the end of the one before: the new
	// leader must come after the whole stall, not after a group of it.
	slices.SortFunc(groups, func(a, b stallGroup) int { return a.end.Compare(b.end) })
	for i, g := range groups {
		// A node's stall may show in a slow flush rather than a sync, so
		// the group's start is known within stallSkew.
		if !slices.ContainsFunc(silences, func(s time.Time) bool { return !s.Before(g.start.Add(-stallSkew)) && !s.After(g.end) }) {
			continue
		}
		end := g.end
		for _, next := range groups[i+1:] {
			if next.start.After(end.Add(stallSkew)) {
				break
			}
			end = next.end
		}
		if slices.ContainsFunc(leaders, func(l time.Time) bool { return !l.Before(end) && l.Sub(end) <= stallElected }) &&
			smallWrites(nodes, g.start.Add(-stallSkew), end.Add(stallSkew)) {
			return end, nil
		}
	}
	return time.Time{}, fmt.Errorf("%w: no stall of every node in which the leader went silent by #233's rule (%q), followed by a leader at term %s", errNotDiskStall, silentRule, term)
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
