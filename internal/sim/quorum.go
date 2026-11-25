package sim

import (
	"fmt"
	"strings"
)

// lostQuorum decides, at the run's end, whether a run that had a node
// fenced for good lost its quorum for good without Config.LoseQuorum
// asking for it, as a replacement can (#850): its nodes still disagree
// once settled, none leads, every running survivor applied all it knows
// is committed, and none can win an election (canElect), each by the
// voters it applied and their logs. The loss then dates from the last
// fence for good, after which nothing could commit, and the run takes the
// checks of a cluster that lost its quorum, not convergence.
//
// It decides only here, once the faults, the clients and the operator
// have stopped and the nodes are frozen: a loss that looked certain at a
// fence can be undone by a survivor that applies a committed change at
// its next start (#850's review). A survivor that knows entries committed
// and has not applied them may still apply a membership change among
// them and elect, so the run is left to convergence then, which a failure
// to apply fails (#850's re-check). A run that converged, or a LoseQuorum
// run, which decides at its fences, is left as it is.
func (s *Sim) lostQuorum() error {
	s.mu.Lock()
	skip := s.quorumLostAt > 0 || s.cfg.LoseQuorum || s.lastFenceAt == 0 || hasLeader(s.nodes)
	s.mu.Unlock()
	if skip {
		return nil
	}
	if a, _ := disagree(s.live()); a == nil {
		return nil
	}
	s.mu.Lock()
	var behind bool
	for _, n := range s.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		t := n.proc.wl.turn
		var voters []string
		for _, m := range t.Members {
			if !m.Learner {
				voters = append(voters, fmt.Sprint(m.ID))
			}
		}
		s.tracef("node %d: applied %d, commit %d, voters [%s]", n.id, t.Applied, t.Commit, strings.Join(voters, " "))
		behind = behind || t.Commit > t.Applied
	}
	s.mu.Unlock()
	if behind {
		return nil
	}
	ok, err := s.canElect()
	if err != nil || ok {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quorumLostAt, s.stats.QuorumLostAt = s.lastFenceAt, s.lastFenceAt
	s.tracef("no node can win an election by the voters it applied: the cluster lost its quorum for good at the fence at %d ms", s.lastFenceAt/Millisecond)
	return nil
}
