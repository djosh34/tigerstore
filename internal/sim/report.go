package sim

import (
	"fmt"
	"slices"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// coreState is what a node's core showed at its last turn, for the report
// of a failure. Reading it never touches the core, which may be running.
type coreState struct {
	core                        *core.Node
	at                          int64
	role                        string
	term, lead, commit, applied uint64
	members                     []*pb.Member
}

// remember keeps what core c of n shows now. The lock is held, on c's
// committer.
func (n *node) remember(c *core.Node, now int64) {
	st := c.RaftStatus()
	members := n.last.members
	if n.last.core != c || n.last.applied != c.Applied() {
		members = c.Members()
	}
	n.last = coreState{
		core: c, at: now, role: st.RaftState.String(),
		term: st.GetTerm(), lead: st.Lead, commit: st.GetCommit(), applied: c.Applied(), members: members,
	}
}

// reportLines is how many of its last log lines the report shows per node.
const reportLines = 12

// report describes every node at the end of the run, for a failure:
// whether its process runs, or why it went down, Raft's view of it at
// its last turn, its members, and its last lines in the trace.
func (s *Sim) report() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for _, n := range s.nodes {
		fmt.Fprintf(&b, "node %d: ", n.id)
		switch {
		case n.proc != nil:
			fmt.Fprintf(&b, "running since %s", seconds(n.since))
		case n.starting:
			b.WriteString("starting from its disk")
		case n.inactive && n.since == 0:
			b.WriteString("not a member yet")
		default:
			fmt.Fprintf(&b, "down since %s: %s", seconds(n.since), n.why)
		}
		if n.fenced {
			b.WriteString("; fenced for good")
		}
		if n.inactive {
			b.WriteString("; not a member")
		}
		b.WriteString("\n")
		if l := n.last; l.core != nil {
			fmt.Fprintf(&b, "  at its last turn, %s: %s, term %d, leader %d, commit %d, applied %d; members %s\n",
				seconds(l.at), l.role, l.term, l.lead, l.commit, l.applied, describeMembers(l.members))
		}
		var lines []string
		mark := fmt.Sprintf(" node %d: ", n.id)
		for i := len(s.trace) - 1; i >= 0 && len(lines) < reportLines; i-- {
			if strings.Contains(s.trace[i], mark) {
				lines = append(lines, s.trace[i])
			}
		}
		for _, l := range slices.Backward(lines) {
			b.WriteString("  " + l + "\n")
		}
	}
	return b.String()
}

func seconds(t int64) string { return fmt.Sprintf("%.3f s", float64(t)/float64(Second)) }

func describeMembers(ms []*pb.Member) string {
	var parts []string
	for _, m := range ms {
		p := fmt.Sprintf("%d %q %v", m.ID, m.Name, m.PeerURLs)
		if m.IsLearner {
			p += " learner"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ", ")
}
