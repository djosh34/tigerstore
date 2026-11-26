package sim

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wl"
)

// TestLoseQuorumWithoutDamage runs the scenarios whose damage found no
// place on main (#873): each must lose its quorum and pass every check.
func TestLoseQuorumWithoutDamage(t *testing.T) {
	simrun.Each(t, []uint64{932, 1196, 1436, 1945, 2322}, func(t *testing.T, seed uint64) Stats {
		cfg := Scenario(seed)
		if !cfg.LoseQuorum {
			t.Fatalf("seed %d no longer requests a quorum loss", seed)
		}
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		if st.QuorumLostAt == 0 || st.Fenced != cfg.voters()/2+1 {
			t.Fatalf("seed %d: %d fenced, quorum lost at %s", seed, st.Fenced, seconds(st.QuorumLostAt))
		}
		t.Logf("seed %d: %d voters, %d fenced, damage %v, quorum lost at %s", seed, cfg.voters(), st.Fenced, st.Damage, seconds(st.QuorumLostAt))
		for _, l := range res.Trace {
			if strings.Contains(l, "fenced:") || strings.Contains(l, "damage after a sync:") || strings.Contains(l, "unavailable:") {
				t.Log(l)
			}
		}
		return st
	}, nil)
}

// TestLoseQuorumWithDamage writes checkpoints before the quorum loss, so
// damage finds a place: nodes must refuse their disks, not take the
// direct fence that loseQuorum uses when no damage fits.
func TestLoseQuorumWithDamage(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.LoseQuorum, cfg.Duration = true, 40*Second
		cfg.Faults = Faults{Corrupt: true, MaxDown: 100 * Millisecond, DiskBudget: 32 << 10}
		cfg.damageKinds = []damageKind{flippedRoot}
		res := run(t, cfg)
		checkers(t, res)
		st := res.Stats
		if st.QuorumLostAt == 0 || st.Damage[flippedRoot] != cfg.voters()/2+1 || st.Fenced != st.Damage[flippedRoot] {
			t.Fatalf("%d fenced, damage %v, quorum lost at %s", st.Fenced, st.Damage, seconds(st.QuorumLostAt))
		}
		for _, l := range res.Trace {
			if strings.Contains(l, "damage found no place") {
				t.Fatalf("damage took the fallback: %s", l)
			}
			if strings.Contains(l, "damage after a sync:") || strings.Contains(l, "fenced:") {
				t.Log(l)
			}
		}
		return st
	}, nil)
}

// TestRequestedQuorumLossMustHappen disables damage and its fallback:
// reaching healing with a quorum must fail, not take the ordinary checks.
func TestRequestedQuorumLossMustHappen(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.LoseQuorum, cfg.Duration = true, 4*Second
		cfg.Faults = Faults{MaxDown: 100 * Millisecond}
		res, err := runSeed(t, cfg)
		if err == nil || !strings.Contains(err.Error(), "requested a quorum loss and none happened by healing") {
			t.Fatalf("run returned %v, want the requested-loss failure", err)
		}
		if res.Stats.QuorumLostAt != 0 || res.Stats.Fenced != 0 {
			t.Fatalf("the run lost its quorum: %+v", res.Stats)
		}
		t.Log(err)
		return res.Stats
	}, nil)
}

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
