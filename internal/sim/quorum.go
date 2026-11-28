package sim

import (
	"strconv"
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
func (s *Sim) lostQuorum() error { return s.lostQuorumBy(s.lastOnDisk) }

// lostQuorumBy is lostQuorum with each running node's last entry from
// lastOf (canElectBy).
func (s *Sim) lostQuorumBy(lastOf func(*node) (lastEntry, error)) error {
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
				voters = append(voters, strconv.FormatUint(m.ID, 10))
			}
		}
		s.tracef("node %d: applied %d, commit %d, voters [%s]", n.id, t.Applied, t.Commit, strings.Join(voters, " "))
		behind = behind || t.Commit > t.Applied
	}
	s.mu.Unlock()
	if behind {
		return nil
	}
	ok, err := s.canElectBy(lastOf)
	if err != nil || ok {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quorumLostAt, s.stats.QuorumLostAt = s.lastFenceAt, s.lastFenceAt
	s.tracef("no node can win an election by the voters it applied: the cluster lost its quorum for good at the fence at %d ms", s.lastFenceAt/Millisecond)
	return nil
}

// stuckRemoval decides, at the run's end, whether the membership work's
// removal of the member it added left a cluster of two voters without a
// quorum for good, as etcd v3.7.2's (#950): the removed member stopped on
// applying its removal, and the survivor's last start, during the
// removal's stage, recovered a commit below the one the removed member
// held durable as it stopped, so that it lost its commit of the removal
// to a crash. The survivor, having applied all it knows committed, still
// counts the removed member among its two voters, and leads not: it waits
// for that member's vote for good. Etcd applies a change before its
// commit is synced (raft.MustSync), and stops a removed member a second
// after its apply. The run then takes the checks of a cluster that lost
// its quorum, from the removed member's stop. Every other run that ends
// without a leader is left to fail.
//
// Temporary: #950's fix fences the removed member until the survivor
// knows the commit, and removes this excuse and the cold restarts' skip
// (checkColdRestarts) with it.
func (s *Sim) stuckRemoval() {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := &s.membership
	if !s.cfg.Membership || w.voters != 1 || w.stage != 3 || w.stoppedAt == 0 || s.quorumLostAt > 0 || hasLeader(s.nodes) {
		return
	}
	survivor := s.nodes[0]
	if survivor.out() || survivor.proc == nil || survivor.startedAt < w.removing || survivor.startCommit >= w.stoppedCommit {
		return
	}
	t := survivor.proc.wl.turn
	voters := 0
	removedVotes := false
	for _, m := range t.Members {
		if !m.Learner {
			voters++
			removedVotes = removedVotes || m.ID == w.id
		}
	}
	if t.Commit != t.Applied || voters != 2 || !removedVotes {
		return
	}
	w.stuck = true
	s.quorumLostAt, s.stats.QuorumLostAt, s.stats.StuckRemovalAt = w.stoppedAt, w.stoppedAt, w.stoppedAt
	s.tracef("node %d started at commit %d, below the %d node %d held durable as it stopped on its removal: it waits for that member's vote for good, as etcd's does; the cluster lost its quorum at %d ms",
		survivor.id, survivor.startCommit, w.stoppedCommit, w.id, w.stoppedAt/Millisecond)
}
