package sim

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/wl"
)

// TestReplacementLosesQuorum is the end of TestCampaign seed 780 on main
// (#850), built directly, as no seed keeps that timing once runs change
// (#846): node 2 was replaced, node 1 had applied the removal of node 2
// at 55 but not the add at 56, which node 3 and node 1 had committed,
// when node 3 was fenced for good. No node can win an election by the
// voters it applied, so the run lost its quorum at that fence and takes
// the checks of a cluster that lost it, not convergence. Had node 1
// applied the add, or did it know entries committed that it had not
// applied, the run would be left to convergence.
func TestReplacementLosesQuorum(t *testing.T) {
	const fence = 40 * Second
	const replacement = 281751481
	voters := func(ids ...uint64) []wl.TurnMember {
		var ms []wl.TurnMember
		for _, id := range ids {
			ms = append(ms, wl.TurnMember{ID: id})
		}
		return ms
	}
	for _, c := range []struct {
		name string
		one  wl.TurnState
		lost bool
		want []string
	}{
		{"the add unapplied", wl.TurnState{Term: 2, Commit: 55, Applied: 55, Rev: 40, Members: voters(1, 3), Removed: []uint64{2}}, true, []string{
			"node 1: applied 55, commit 55, voters [1 3]",
			"node 281751481: applied 0, commit 0, voters []",
			"no node can win an election by the voters it applied: the cluster lost its quorum for good at the fence at 40000 ms",
		}},
		{"the add applied", wl.TurnState{Term: 2, Commit: 56, Applied: 56, Rev: 40, Members: voters(1, 3, replacement), Removed: []uint64{2}}, false, []string{
			"node 1: applied 56, commit 56, voters [1 3 281751481]",
		}},
		{"the add committed, unapplied", wl.TurnState{Term: 2, Commit: 56, Applied: 55, Rev: 40, Members: voters(1, 3), Removed: []uint64{2}}, false, []string{
			"node 1: applied 55, commit 56, voters [1 3]",
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := DefaultConfig(1)
			cfg.Nodes = 3
			s, err := newSim(cfg)
			if err != nil {
				t.Fatal(err)
			}
			running := func(n *node, turn wl.TurnState) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				n.proc = &proc{n: n, ctx: ctx, cancel: cancel, wl: &wlProc{turn: turn}}
			}
			// Node 2's slot holds its replacement, which applied nothing;
			// node 3 is fenced for good.
			one, three := s.nodes[0], s.nodes[2]
			repl := &node{idx: 1, id: replacement, joins: true, wake: make(chan struct{})}
			s.nodes[1] = repl
			running(one, c.one)
			running(repl, wl.TurnState{})
			three.fenced, three.since = true, fence
			s.lastFenceAt = fence
			last := map[uint64]lastEntry{1: {term: 2, index: 56}, replacement: {}}
			if err := s.lostQuorumBy(func(n *node) (lastEntry, error) { return last[n.id], nil }); err != nil {
				t.Fatal(err)
			}

			if lost := s.stats.QuorumLostAt != 0; lost != c.lost {
				t.Fatalf("quorum lost at %s, want lost %v", seconds(s.stats.QuorumLostAt), c.lost)
			}
			if c.lost && (s.stats.QuorumLostAt != fence || s.quorumLostAt != fence) {
				t.Fatalf("quorum lost at %s (run %s), want the fence at %s", seconds(s.stats.QuorumLostAt), seconds(s.quorumLostAt), seconds(fence))
			}
			for _, want := range c.want {
				if !slices.ContainsFunc(s.trace, func(l string) bool { return strings.HasSuffix(l, want) }) {
					t.Errorf("the trace has no %q:\n%s", want, strings.Join(s.trace, "\n"))
				}
			}
		})
	}
}
