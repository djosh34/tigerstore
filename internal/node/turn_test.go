package node

import "testing"

// TestStoppedNodeHasTurnRoom checks that a node that stopped right after
// a turn that took a full turn's worth of calls still has room for calls:
// its loop drains on without its committer, which no longer starts turns,
// and its connections must read what clients send, so that they hear
// that it stopped (#647).
func TestStoppedNodeHasTurnRoom(t *testing.T) {
	n := &Node{done: make(chan struct{}), turnCalls: MaxTurnCalls}
	if r := n.TurnRoom(); r != 0 {
		t.Fatalf("a running node after a full turn has room for %d calls", r)
	}
	close(n.done)
	if r := n.TurnRoom(); r != MaxTurnCalls {
		t.Fatalf("a stopped node after a full turn has room for %d calls, want %d", r, MaxTurnCalls)
	}
}
