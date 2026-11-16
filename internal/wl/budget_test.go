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
			// The client connections: idle ones within a quarter of the
			// client pool, and what they hold while they read or write
			// within the rest (sock.pool).
			heap := replication + z.held + int64(z.uncommitted) + keptBufs*keptBufMax + z.clientPool
			store := budget - z.mapped() - (heap + heap/4)
			if store < budget/4 {
				t.Errorf("budget %d MiB, %d followers: the buffers leave the store %d MiB (mapped %d MiB, send buffers %d MiB, held %d MiB, uncommitted %d MiB, clients %d MiB); want at least a quarter",
					budget>>20, followers, store>>20, z.mapped()>>20, replication>>20, z.held>>20, z.uncommitted>>20, z.clientPool>>20)
			}
			if z.inflight(followers+1) > uint64(z.peerBacklog(followers)) {
				t.Errorf("budget %d MiB, %d followers: Raft keeps %d bytes in flight to a follower, past its backlog of %d", budget>>20, followers, z.inflight(followers+1), z.peerBacklog(followers))
			}
		}
		// The idle connections the node takes fit what the client
		// buffers leave of the client pool; the clients that hold buffers, at most as many as
		// the buffers hold their read and opened buffers, have shares of
		// what waits to be sent within the buffers, and each a share of
		// requests sent in part of at least one whole request.
		if idle := int64(z.maxClients()) * idleConn; idle+z.clientBufs() > z.clientPool {
			t.Errorf("budget %d MiB: %d idle clients take %d MiB, past what the client buffers leave of the client pool's %d MiB", budget>>20, z.maxClients(), idle>>20, z.clientPool>>20)
		}
		most := int(z.clientInBufs() / (clientInBuf + clientPtBuf))
		for _, active := range []int{1, most} {
			if out := int64(active) * z.connOut(active); out > z.clientBufs()-z.clientInBufs() || z.connIn(active) < maxMsg {
				t.Errorf("budget %d MiB, %d active clients: their send shares sum to %d MiB, past the send half of the client buffers' %d MiB, or a share of requests sent in part of %d KiB",
					budget>>20, active, out>>20, z.clientBufs()>>20, z.connIn(active)>>10)
			}
		}
		t.Logf("budget %d MiB: at most %d clients", budget>>20, z.maxClients())
		if z.stage&(z.stage-1) != 0 || z.stage < 4<<20 || z.stage > z.ring {
			t.Errorf("budget %d MiB: a stage of %d bytes, want a power of two from 4 MiB up to the ring's %d", budget>>20, z.stage, z.ring)
		}
	}
	// Thousands of clients at the bench's 3 GiB, as a cluster of several
	// API servers, controllers and tools may hold.
	if most := sizeFor(3 << 30).maxClients(); most < 3000 {
		t.Errorf("at 3 GiB at most %d client connections, want at least 3,000", most)
	}
	// The bench's 3 GiB keeps the sizes it measured.
	if z := sizeFor(3 << 30); z.ring != 64<<20 || z.side != 16<<20 || z.stage != 32<<20 || z.held != 64<<20 {
		t.Errorf("at 3 GiB: rings %d, side %d, stage %d, held %d; want 64, 16, 32 and 64 MiB", z.ring>>20, z.side>>20, z.stage>>20, z.held>>20)
	}
}
