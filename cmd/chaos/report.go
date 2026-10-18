package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// reportLines is how many of its last log lines the report shows per node.
const reportLines = 15

// report describes each node now, for a failure: whether its process
// runs, is stopped or exited and how, Raft's view of it, its members, and
// its last log lines. A node that does not answer in a second says why.
func (c *cluster) report(ctx context.Context) string {
	var b strings.Builder
	for i, n := range c.nodes {
		n.mu.Lock()
		cmd, exited, starts, unexpected := n.cmd, n.exitedNow, n.starts, slices.Clone(n.unexpected)
		n.mu.Unlock()
		fmt.Fprintf(&b, "node%d: ", i+1)
		switch {
		case cmd == nil:
			b.WriteString("never started")
		case exited:
			fmt.Fprintf(&b, "incarnation %d (pid %d) exited: %v", starts, cmd.Process.Pid, cmd.ProcessState)
		default:
			st, err := procState(cmd.Process.Pid)
			if err != nil {
				st = err.Error()
			} else if st == "T" {
				st = "T, stopped by a signal"
			}
			fmt.Fprintf(&b, "incarnation %d (pid %d) running, state %s", starts, cmd.Process.Pid, st)
		}
		for _, u := range unexpected {
			fmt.Fprintf(&b, "; %s", u)
		}
		b.WriteString("\n")
		if st, err := c.status(ctx, i); err != nil {
			fmt.Fprintf(&b, "  status: no answer: %v\n", err)
		} else {
			role := "follower"
			switch {
			case st.IsLearner:
				role = "learner"
			case st.Leader == 0:
				role = "without a leader"
			case st.Leader == st.Header.MemberId:
				role = "leader"
			}
			fmt.Fprintf(&b, "  member %x, %s, term %d, leader %x, commit %d, applied %d, revision %d", st.Header.MemberId, role, st.RaftTerm, st.Leader, st.RaftIndex, st.RaftAppliedIndex, st.Header.Revision)
			if len(st.Errors) > 0 {
				fmt.Fprintf(&b, ", errors %v", st.Errors)
			}
			b.WriteString("\n")
		}
		mctx, cancel := context.WithTimeout(ctx, time.Second)
		ms, err := n.control.MemberList(mctx, clientv3.WithSerializable())
		cancel()
		if err != nil {
			fmt.Fprintf(&b, "  members: no answer: %v\n", err)
		} else {
			var parts []string
			for _, m := range ms.Members {
				p := fmt.Sprintf("%x %q %v", m.ID, m.Name, m.PeerURLs)
				if m.IsLearner {
					p += " learner"
				}
				parts = append(parts, p)
			}
			fmt.Fprintf(&b, "  members: %s\n", strings.Join(parts, ", "))
		}
		for _, line := range lastLines(n.log.Name(), reportLines) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	return b.String()
}

// lastLines returns the last n lines of a file, each cut to 400 bytes.
func lastLines(path string, n int) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return []string{err.Error()}
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	lines = lines[max(0, len(lines)-n):]
	for i, l := range lines {
		if len(l) > 400 {
			lines[i] = l[:400] + "..."
		}
	}
	return lines
}

// tally reads the faults.json of every run under dirs and counts, per
// fault, the runs it was installed in and those in which it took effect
// and overlapped a write. A fault took effect in too few of them when in
// fewer than three in four, rounded down; then tally reports false and
// names up to five runs it missed, whose config.json and faults.json say
// how to run them again and what the fault did.
func tally(dirs []string, logf func(string, ...any)) (bool, error) {
	type count struct {
		installed, took int
		missed          []string
	}
	counts := map[string]*count{}
	runs := 0
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || d.Name() != "faults.json" {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// Only the fields the tally needs: a planned step's direction
			// is written as text and not read back.
			var records []struct {
				Planned struct {
					Fault string `json:"fault"`
				} `json:"planned"`
				Installed, Effect, Overlapped bool
			}
			if err := json.Unmarshal(b, &records); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			runs++
			for _, r := range records {
				c := counts[r.Planned.Fault]
				if c == nil {
					c = &count{}
					counts[r.Planned.Fault] = c
				}
				if r.Installed {
					c.installed++
					if r.Effect && r.Overlapped {
						c.took++
					} else {
						c.missed = append(c.missed, filepath.Dir(path))
					}
				}
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	if runs == 0 {
		return false, fmt.Errorf("no faults.json under %v", dirs)
	}
	ok := true
	for _, f := range slices.Sorted(maps.Keys(counts)) {
		c := counts[f]
		least := c.installed * 3 / 4
		verdict := "ok"
		if c.took < least {
			verdict, ok = fmt.Sprintf("too few, want %d at least", least), false
		}
		logf("%-20s installed in %3d runs, took effect and overlapped a write in %3d: %s", f, c.installed, c.took, verdict)
		if c.took < least {
			logf("  it missed in %s", strings.Join(c.missed[:min(5, len(c.missed))], ", "))
		}
	}
	logf("%d runs", runs)
	return ok, nil
}
