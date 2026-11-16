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
	// clientOut is what waits unsent to every client together, at most
	// the connections' shares (connOut), and the one reply each may write
	// past its share.
	clientOut int64
}

// sizeFor sizes the buffers of a node with budget bytes, 0 for none.
func sizeFor(budget int64) sizes {
	if budget <= 0 {
		budget = unbudgeted
	}
	ring := int(min(max(budget/48, 8<<20), 64<<20)) &^ (block - 1)
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
		clientOut:   budget / 16,
	}
}

// minConnOut is the least a client connection's share of clientOut may
// be: with more clients than clientOut holds such shares (768 at 3 GiB),
// they may hold more together.
const minConnOut = 256 << 10

// connOut is the most that waits unsent to one of clients connections
// before the node stops reading its requests and sends its watches'
// events by catching up from the change index (client.backedUp).
func (z sizes) connOut(clients int) int64 {
	return max(z.clientOut/int64(max(clients, 1)), minConnOut)
}

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
