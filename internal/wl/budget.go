package wl

// The node's buffers outside the store are sized from its memory budget
// (Config.MemoryBudget), so that their worst case and the store's share
// fit within it together (TestTheBuffersFitTheBudget). The store's slab
// takes what is left (store.relimit).

// unbudgeted is the budget the buffers are sized for when the node has
// none: the bench's 3 GiB, for which the sizes below were measured.
const unbudgeted = 3 << 30

// maxPeerMsg is the most one message to a peer takes in its send buffer:
// an append of at most MaxSizePerMsg, or of one entry up to the request
// limit, and its framing.
const maxPeerMsg = 2 << 20

// sizes are the node's buffers outside the store.
type sizes struct {
	// ring is the size of each of the proposal and receive rings, side
	// the side ring's, and stage the log's stage, a power of two.
	ring, side, stage int
	// held is the most entry data Raft's storage holds (logStorage.trim).
	held int64
	// replication is the most every follower's send buffer takes
	// together, at its worst (peerBacklog).
	replication int64
	// uncommitted is Raft's MaxUncommittedEntriesSize: the leader's
	// proposals not yet committed, on Go's heap.
	uncommitted uint64
	// clientPool is what the client connections hold together: half
	// for at most maxClients connections, idle, of idleConn each,
	// and the rest (clientBufs) for what they hold while they read or
	// write: their buffers and stream slots, requests sent in part, and
	// replies held for want of window (sock.pool).
	clientPool int64
}

// sizeFor sizes the buffers of a node with budget bytes, 0 for none.
func sizeFor(budget int64) sizes {
	if budget <= 0 {
		budget = unbudgeted
	}
	ring := int(min(max(budget/48, 8<<20), ringMax)) &^ (block - 1)
	stage := 4 << 20
	for stage*2 <= ring/2 {
		stage *= 2
	}
	return sizes{
		ring:        ring,
		side:        ring / 4,
		stage:       stage,
		held:        max(budget/48, 4<<20),
		replication: budget / 16,
		uncommitted: uint64(budget / 16),
		clientPool:  budget / 16,
	}
}

// maxMsg is the largest gRPC message a client may send: etcd's request
// limit and its gRPC server's overhead over it.
const maxMsg = maxRequestBytes + grpcOverhead

// idleConn is the most an idle client connection holds: its client, its
// HPACK decoder's table and its header cache at their fullest, and its
// flow state, without buffers (TestAnIdleClientHoldsNoBuffers).
const idleConn = 32 << 10

// maxClients is the most client connections the node takes, a backstop:
// half the client pool of idle ones (3,072 at 3 GiB). Past it the node
// closes a new connection, as etcd refuses work past its limits.
func (z sizes) maxClients() int { return int(max(z.clientPool/2/idleConn, 1)) }

// clientBufs is what the client connections hold while they read or
// write, together: half of it what they read (node.cpool), where a client
// that would go past it to read waits and a request that would go past it
// is refused (client.data); and half what waits to be sent to them
// (node.cout), within their shares (connOut).
func (z sizes) clientBufs() int64 { return z.clientPool / 2 }

// clientInBufs is the half of clientBufs for what the clients read.
func (z sizes) clientInBufs() int64 { return z.clientBufs() / 2 }

// connIn is the most of requests sent in part one of active clients
// holds before its next is refused: its share of clientInBufs, at least
// one whole request while clientInBufs holds it.
func (z sizes) connIn(active int) int64 {
	return max(z.clientInBufs()/int64(max(active, 1)), maxMsg)
}

// minConnOut is the least a client's share of what waits to be sent may
// be.
const minConnOut = 64 << 10

// connOut is the most that waits unsent to one of active clients before
// the node stops reading its requests and sends its watches' events by
// catching up from the change index (client.backedUp): its share of the
// half of clientBufs for what waits to be sent, less the reserve. A
// message that would take a client past its share takes room from the
// reserve first (node.msgRoom); a range reply past oneShot, and a watch's
// catch-up past syncBytes a turn, go in pieces within it (bigrange.go,
// #721).
func (z sizes) connOut(active int) int64 {
	return max((z.clientBufs()-z.clientInBufs()-z.reserve())/int64(max(active, 1)), minConnOut)
}

// reserve is the room of the send half of clientBufs that the messages
// past their clients' shares take, together (#727): at least one whole
// message, and a thirty-second of the client pool.
func (z sizes) reserve() int64 { return max(maxMsg, z.clientPool/32) }

// mapped is what the rings and the stage map outside Go's heap.
func (z sizes) mapped() int64 { return int64(2*z.ring + z.side + z.stage) }

// peerBacklog is the most of a follower's send buffer that waits to be
// written before appends to it wait for Raft's next probe, with followers
// sharing the replication buffers. A buffer grows to at most four times
// what waits in it (sock.openRec), and an append may come on top.
func (z sizes) peerBacklog(followers int) int64 {
	return max(z.replication/int64(4*max(followers, 1))-maxPeerMsg, 1<<20)
}

// inflight is Raft's MaxInflightBytes for a cluster of members: a
// follower's appends in flight stay within its backlog.
func (z sizes) inflight(members int) uint64 { return uint64(z.peerBacklog(members - 1)) }

// heldFor is the entry data Raft's storage holds for budget bytes.
func heldFor(budget int64) int64 { return sizeFor(budget).held }
