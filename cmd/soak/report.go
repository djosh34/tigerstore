package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// reportLines is how many of its last log lines the report shows per node.
const reportLines = 15

// report describes each node now, for a failure: whether its process
// runs and how much memory its cgroup took, Raft's view of it, its
// members, and its last log lines. A node that does not answer in two
// seconds says why.
func (c *cluster) report(ctx context.Context) string {
	var b strings.Builder
	for _, n := range c.nodes {
		n.mu.Lock()
		cmd, starts, exits := n.cmd, n.starts, append([]string(nil), n.exits...)
		n.mu.Unlock()
		fmt.Fprintf(&b, "node%d: ", n.i+1)
		switch {
		case cmd == nil:
			b.WriteString("never started")
		case errors.Is(syscall.Kill(cmd.Process.Pid, 0), syscall.ESRCH):
			fmt.Fprintf(&b, "incarnation %d (pid %d) exited", starts, cmd.Process.Pid)
		default:
			state := "running"
			if st, err := procState(cmd.Process.Pid); err == nil && st == "T" {
				state = "stopped by a signal"
			}
			fmt.Fprintf(&b, "incarnation %d (pid %d) %s, RSS %d MiB", starts, cmd.Process.Pid, state, rss(cmd.Process.Pid)>>20)
		}
		if st, err := n.cg.Stats(); err == nil {
			fmt.Fprintf(&b, "; cgroup peak %d MiB, %d OOM kills", st.PeakBytes>>20, st.OOMKills)
		}
		fmt.Fprintf(&b, "; data %d MiB", dirSize(n.data)>>20)
		for _, e := range exits {
			fmt.Fprintf(&b, "; %s", e)
		}
		b.WriteString("\n")
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := n.one.Status(cctx, n.one.Endpoints()[0])
		cancel()
		if err != nil {
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
		cctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		ms, err := n.one.MemberList(cctx, clientv3.WithSerializable())
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

// procState reads the scheduler state letter of pid from /proc.
func procState(pid int) (string, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return "", fmt.Errorf("unreadable /proc/%d/stat", pid)
	}
	return s[i+2 : i+3], nil
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

// replayCommand is the command that runs c again, with every flag that
// shapes the run.
func replayCommand(c config) string {
	cmd := fmt.Sprintf("go run ./cmd/soak -tigerstore <binary> -out <dir> -duration %v -seed %d -objects %d -memory %d -disk-budget %d -workers %d -rate %v -kill-every %v -hold-every %v -compact-every %v -burst %d -liveness %v -request-timeout %v",
		c.duration, c.seed, c.objects, c.memory, c.diskBudget, c.workers, c.rate, c.killEvery, c.holdEvery, c.compactEvery, c.burst, c.liveness, c.requestTimeout)
	if len(c.nodeArgs) > 0 {
		cmd += " -node-arg " + strings.Join(c.nodeArgs, " -node-arg ")
	}
	return cmd
}
