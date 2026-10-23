package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
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
		fmt.Fprintf(&b, "  heartbeats: %s\n", c.heartbeats(ctx, i))
		for _, line := range lastLines(n.log.Name(), reportLines) {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	return b.String()
}

// heartbeats says how long ago node i last wrote a Raft heartbeat to each
// peer and read one from each, from its /metrics (#241).
func (c *cluster) heartbeats(ctx context.Context, i int) string {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+c.nodes[i].control.Endpoints()[0]+"/metrics", nil)
	if err != nil {
		return err.Error()
	}
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: c.tls}}
	defer hc.CloseIdleConnections()
	resp, err := hc.Do(req)
	if err != nil {
		return "no answer: " + err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	return heartbeatAges(resp.Body, time.Now())
}

// heartbeatAges reads the heartbeat times of a /metrics page and says how
// long before now each was.
func heartbeatAges(r io.Reader, now time.Time) string {
	var parts []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		for prefix, dir := range map[string]string{"tigerstore_network_heartbeat_sent_time_seconds{": "to", "tigerstore_network_heartbeat_received_time_seconds{": "from"} {
			rest, ok := strings.CutPrefix(sc.Text(), prefix)
			if !ok {
				continue
			}
			labels, value, _ := strings.Cut(rest, "} ")
			at, err := strconv.ParseFloat(value, 64)
			if err != nil {
				parts = append(parts, fmt.Sprintf("%s %s: %v", dir, labels, err))
				continue
			}
			ago := now.Sub(time.Unix(0, int64(at*1e9)))
			parts = append(parts, fmt.Sprintf("%s %s %.1fs ago", dir, labels, ago.Seconds()))
		}
	}
	if err := sc.Err(); err != nil {
		parts = append(parts, err.Error())
	}
	if len(parts) == 0 {
		return "none"
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
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
