package sim

import (
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wl"
)

// TestHasQuorum counts the voters each survivor applied, not the initial
// cluster or a configuration only the fenced leader applied.
func TestHasQuorum(t *testing.T) {
	members := func(ids ...uint64) []wl.TurnMember {
		ms := make([]wl.TurnMember, len(ids))
		for i, id := range ids {
			ms[i].ID = id
		}
		return ms
	}
	learner := func(ms []wl.TurnMember, id uint64) []wl.TurnMember {
		return append(ms, wl.TurnMember{ID: id, Learner: true})
	}
	for _, tc := range []struct {
		name  string
		nodes []*node
		want  bool
	}{
		{"three voters", []*node{
			{id: 1, lastMembers: members(1, 2, 3)}, {id: 2}, {id: 3},
		}, true},
		{"one fenced, two down", []*node{
			{id: 1, lastMembers: members(1, 2, 3)}, {id: 2, fenced: true}, {id: 3},
		}, true},
		{"two fenced", []*node{
			{id: 1, lastMembers: members(1, 2, 3)}, {id: 2, fenced: true}, {id: 3, fenced: true},
		}, false},
		{"removal pending", []*node{
			{id: 1, lastMembers: members(1, 3)}, {id: 4}, {id: 3, fenced: true, lastMembers: members(1)},
		}, false},
		{"removal applied", []*node{
			{id: 1, lastMembers: members(1)}, {id: 4}, {id: 3, fenced: true},
		}, true},
		{"add not applied", []*node{
			{id: 1, lastMembers: members(1, 3)}, {id: 4}, {id: 3, fenced: true, lastMembers: members(1, 3, 4)},
		}, false},
		{"add applied by survivor", []*node{
			{id: 1, lastMembers: members(1, 3, 4)}, {id: 4}, {id: 3, fenced: true},
		}, true},
		{"add applied by replacement alone", []*node{
			{id: 1, lastMembers: members(1, 3)}, {id: 4, lastMembers: members(1, 3, 4)}, {id: 3, fenced: true},
		}, true},
		{"old member cannot vote as its replacement", []*node{
			{id: 1, lastMembers: members(1, 2, 3)}, {id: 4}, {id: 3, fenced: true},
		}, false},
		{"learner cannot vote", []*node{
			{id: 1, lastMembers: learner(members(1, 3), 4)}, {id: 4}, {id: 3, fenced: true},
		}, false},
		{"learner does not enlarge quorum", []*node{
			{id: 1, lastMembers: learner(members(1, 2, 3), 4)}, {id: 2, fenced: true}, {id: 3}, {id: 4},
		}, true},
		{"learner cannot campaign", []*node{
			{id: 1}, {id: 4, lastMembers: learner(members(1), 4)},
		}, false},
		{"removed node cannot campaign", []*node{
			{id: 1, lastMembers: members(3)}, {id: 3},
		}, false},
		{"inactive node cannot vote", []*node{
			{id: 1, lastMembers: members(1, 3)}, {id: 3, inactive: true},
		}, false},
		{"empty disks cannot campaign", []*node{{id: 1}, {id: 2}, {id: 3}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasQuorum(tc.nodes); got != tc.want {
				t.Fatalf("hasQuorum = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReplacementLosesQuorum is TestCampaign seed 780 (#850): node 2
// was replaced, but node 1 had not applied the add when node 3 was fenced
// too. Its run must check safety and unavailability, not convergence.
func TestReplacementLosesQuorum(t *testing.T) {
	if simrun.Race {
		t.Skip("the replacement's unapplied add needs the seed's deterministic timing")
	}
	simrun.Each(t, []uint64{780}, func(t *testing.T, seed uint64) Stats {
		res := run(t, Scenario(seed))
		if st := res.Stats; st.Replaced != 1 || st.QuorumLostAt == 0 {
			t.Fatalf("seed %d: %d replaced, quorum lost at %s", seed, st.Replaced, seconds(st.QuorumLostAt))
		}
		one, replacement, three := res.nodes[0], res.nodes[1], res.nodes[2]
		if !three.fenced || res.Stats.QuorumLostAt != three.since {
			t.Fatalf("quorum lost at %s, node 3 fenced %v at %s", seconds(res.Stats.QuorumLostAt), three.fenced, seconds(three.since))
		}
		if len(one.lastMembers) != 2 || !slices.ContainsFunc(one.lastMembers, func(m wl.TurnMember) bool { return m.ID == 3 }) || len(replacement.lastMembers) != 0 {
			t.Fatalf("survivors' applied members: node 1 %v, replacement %v", one.lastMembers, replacement.lastMembers)
		}
		checkers(t, res)
		return res.Stats
	}, nil)
}
