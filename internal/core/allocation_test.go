package core

import (
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// A settled workspace must NOT reappear in an old timeout/proposal list after
// the driver clears and reuses it. Exercise both pre-proposal expiry and an
// ordinary settlement, without relying on the runtime GC to hide stale refs.
func TestAllocationWorkspaceRetiresAllPendingReferences(t *testing.T) {
	put, err := ParsePut([]byte{10, 1, 'k', 18, 1, 'v'})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{cfg: Config{RequestTicks: 2, MaxBatchBytes: 4 << 20}}
	n.EnableAllocationWorkspace(16)
	var w Workspace
	for id := uint64(1); id <= 20; id++ {
		n.Submit(Request{ID: id, Put: put, Workspace: &w})
		if len(n.proposals) != 1 || len(n.timeouts) != 1 {
			t.Fatal("request was not pinned in both lists")
		}
		if id%2 == 0 {
			n.finish(&w.p, nil, rpctypes.ErrGRPCStopped)
		} else {
			n.now += 2
			n.expire()
		}
		n.retireManaged()
		if len(n.proposals) != 0 || len(n.timeouts) != 0 || len(n.waiting) != 0 {
			t.Fatal("settled request retained in core workspace collections")
		}
		if n.out.ManagedReplies != &w || w.Reply.ID != id {
			t.Fatal("reply lost or attributed to wrong generation")
		}
		n.out = Effects{}
		w.Clear()
	}
}
