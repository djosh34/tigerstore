package wl

import "testing"

// The node's bounded buffers, each at its worst, fit the budget with
// room for the store: the rings and the stage mapped outside Go's heap,
// and on the heap every follower's send buffer full, the entries Raft's
// storage holds, the leader's uncommitted proposals and the kept stream
// buffers, with the quarter more Go needs to collect in (store.goNeed).
// The store's slab gets what is left (store.relimit), which must be at
// least a quarter of the budget. A client connection's own buffers are
// per connection and left out, as etcd leaves them out.
func TestTheBuffersFitTheBudget(t *testing.T) {
	for _, budget := range []int64{256 << 20, 512 << 20, 1 << 30, 3 << 30, 16 << 30} {
		z := sizeFor(budget)
		for _, followers := range []int{1, 2, 4, 6} {
			// A send buffer grows to at most four times what waits in it,
			// its backlog and one message on top (sock.openRec).
			replication := int64(followers) * (4*(z.peerBacklog(followers)+maxPeerMsg) + maxPlain + recOverhead + 8)
			// The clients' send buffers and held replies: each within its
			// share of clientOut (connOut), with the reply under way.
			heap := replication + z.held + int64(z.uncommitted) + keptBufs*keptBufMax + z.clientOut + z.clientIn
			store := budget - z.mapped() - (heap + heap/4)
			if store < budget/4 {
				t.Errorf("budget %d MiB, %d followers: the buffers leave the store %d MiB (mapped %d MiB, send buffers %d MiB, held %d MiB, uncommitted %d MiB, clients %d MiB out and %d MiB in); want at least a quarter",
					budget>>20, followers, store>>20, z.mapped()>>20, replication>>20, z.held>>20, z.uncommitted>>20, z.clientOut>>20, z.clientIn>>20)
			}
			if z.inflight(followers+1) > uint64(z.peerBacklog(followers)) {
				t.Errorf("budget %d MiB, %d followers: Raft keeps %d bytes in flight to a follower, past its backlog of %d", budget>>20, followers, z.inflight(followers+1), z.peerBacklog(followers))
			}
		}
		// The clients' shares of what waits to be sent sum to clientOut
		// up to the most clients the node takes.
		for _, clients := range []int{1, z.maxClients()} {
			if sum := int64(clients) * z.connOut(clients); sum > z.clientOut {
				t.Errorf("budget %d MiB, %d clients: their shares sum to %d MiB, past the client buffers' %d MiB", budget>>20, clients, sum>>20, z.clientOut>>20)
			}
		}
		// The most connections the node takes, each with its fixed
		// buffers and its share of requests sent in part, at least one
		// whole request, fit clientIn.
		for _, clients := range []int{1, z.maxClients()} {
			if in := int64(clients) * (connFixed + z.connIn(clients)); in > z.clientIn || z.connIn(clients) < maxMsg {
				t.Errorf("budget %d MiB, %d clients: their input takes %d MiB, a share of %d KiB, of %d MiB", budget>>20, clients, in>>20, z.connIn(clients)>>10, z.clientIn>>20)
			}
		}
		t.Logf("budget %d MiB: at most %d clients", budget>>20, z.maxClients())
		if z.stage&(z.stage-1) != 0 || z.stage < 4<<20 || z.stage > z.ring {
			t.Errorf("budget %d MiB: a stage of %d bytes, want a power of two from 4 MiB up to the ring's %d", budget>>20, z.stage, z.ring)
		}
	}
	// The bench's 3 GiB keeps the sizes it measured.
	if z := sizeFor(3 << 30); z.ring != 64<<20 || z.side != 16<<20 || z.stage != 32<<20 || z.held != 64<<20 {
		t.Errorf("at 3 GiB: rings %d, side %d, stage %d, held %d; want 64, 16, 32 and 64 MiB", z.ring>>20, z.side>>20, z.stage>>20, z.held>>20)
	}
}
