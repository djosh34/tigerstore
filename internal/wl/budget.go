package wl

import "fmt"

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
	// replies held for want of window (sock.pool). Beyond both halves,
	// the reserve takes the messages past their clients' shares (#727).
	clientPool int64
	// kept is the most buffers that ended streams give back the node
	// keeps for the next, each of at most keptBufMax (node.putBuf): 64
	// at the bench's 3 GiB, and fewer at smaller budgets, from 4 up.
	kept int
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
		kept:        int(min(max(budget/(48<<20), 4), 64)),
	}
}

// worst is the most the node's bounded buffers take together, each at its
// worst, with followers: the rings and the stage mapped outside Go's
// heap, and on the heap every follower's send buffer full, the entries
// Raft's storage holds, the leader's uncommitted proposals, the kept
// stream buffers, the client pool and its reserve (#727), and the values
// fetched off the loop that it keeps (fetchedMax) and reads or hands off
// (fetchWorkMax), with the quarter more Go needs to collect in
// (store.goNeed). A client connection's own
// buffers are per connection and left out, as etcd leaves them out.
func (z sizes) worst(budget int64, followers int) int64 {
	// A send buffer grows to at most four times what waits in it, its
	// backlog and one message on top (sock.openRec).
	replication := int64(followers) * (4*(z.peerBacklog(followers)+maxPeerMsg) + maxPlain + recOverhead + 8)
	heap := replication + z.held + int64(z.uncommitted) + int64(z.kept)*keptBufMax + z.clientPool + z.reserve() + fetchedMax(budget) + fetchWorkMax(budget)
	return z.mapped() + heap + heap/4
}

// fetchWorkMax counts the second fetch buffer, separate from the kept
// cache: in-flight reads and results not yet taken by the loop. They fit
// fetchedMax, except for one batch alone. A consumer stops at oneShot
// (at most the lone client's share), plus the last value; a watch may
// also read that event's value before it. Values fit maxRequestBytes;
// gRPC's extra overhead in maxMsg holds no extra value bytes. Count both
// values in the overshoot, not just oneShot or the ordinary share.
func fetchWorkMax(budget int64) int64 {
	return max(fetchedMax(budget), max(maxMsg, sizeFor(budget).connOut(1))+2*maxRequestBytes)
}

// keeps reports whether a node with budget bytes keeps it with followers:
// its buffers at their worst leave the store at least a quarter of the
// budget, for its tables and its slab, which takes what the tables leave
// (store.relimit).
func keeps(budget int64, followers int) bool {
	return budget-sizeFor(budget).worst(budget, followers) >= budget/4
}

// leastBudget is the least memory budget, in whole MiB, that a node keeps
// with followers: 121 MiB for a cluster of three. For up to 8 followers
// it keeps every budget above it, and every smaller cluster
// (TestTheBuffersFitTheBudget); Config.check and budgetKeeps hold the
// budget to it.
// The send buffers' floors, a backlog and a message of each follower's,
// do not shrink with the budget, so each follower more needs about 28 MiB
// more (#760).
func leastBudget(followers int) int64 {
	b := int64(1 << 20)
	for !keeps(b, followers) {
		b += 1 << 20
	}
	return b
}

// startFollowers is the most followers a node that starts with members
// is checked for (Config.check): the other members, and, on a node that
// bootstrapped the cluster, as many learners as the cluster may add
// (maxLearners). A node that joined was added to a cluster whose budget
// was checked for it as the add was proposed (node.budgetKeeps), and
// starts with that cluster's members: counting a learner more would refuse
// it, and its restarts, at the very budget that admitted it.
func startFollowers(members int, join bool) int {
	f := max(members-1, 0)
	if !join {
		f += maxLearners
	}
	return f
}

// budgetKeeps refuses a membership change that leaves a cluster of
// members the node's memory budget does not keep (#760). It is checked as
// the change is proposed, on the node a client asks for an add and on the
// leader for a promotion, against that node's own budget and member list,
// and never as the change is applied, so that no member's applied state
// depends on its own budget. A promotion does not change the members'
// count: it is refused only where the cluster already passed the
// budget. Two adds asked of two nodes at once, or an add asked of a node
// whose budget is larger than another member's, are not caught here:
// every member is meant to run with the same budget, and one that does
// not keep the cluster says so as it applies the add
// (node.applyConfChange).
func (n *node) budgetKeeps(members int) error {
	if n.st.budget <= 0 || n.st.budget >= leastBudget(members-1) {
		return nil
	}
	return requestErr(codeFailedPrecondition, fmt.Sprintf("etcdserver: re-configuration failed: a memory budget of %d bytes does not keep a cluster of %d members, which needs at least %d bytes", n.st.budget, members, leastBudget(members-1)))
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
// holds before its next is refused: its share of clientInBufs less its
// own buffers, at least one whole request. Above that floor, the shares
// and buffers of the clients that read fit clientInBufs together, so a
// part that waits for room finds it; at the floor, as at small budgets,
// the turn finds it (partwait.go).
func (z sizes) connIn(active int) int64 {
	return max(z.clientInBufs()/int64(max(active, 1))-int64(clientInBuf+clientPtBuf+slotBytes), maxMsg)
}

// minConnOut is the least a client's share of what waits to be sent may
// be.
const minConnOut = 64 << 10

// connOut is the most that waits unsent to one of active clients before
// the node stops reading its requests and sends its watches' events by
// catching up from the change index (client.backedUp): its share of the
// half of clientBufs for what waits to be sent. A message that would
// take a client past its share takes room from the reserve first
// (node.msgRoom); a range reply past oneShot, and a watch's catch-up past
// syncBytes a turn, go in pieces within it (bigrange.go, #721).
func (z sizes) connOut(active int) int64 {
	return max((z.clientBufs()-z.clientInBufs())/int64(max(active, 1)), minConnOut)
}

// reserve is the client pool's room, beyond its two halves, that the
// messages past their clients' shares take together (#727): at least one
// whole message, and a thirty-second of the client pool. Taken out of the
// send half, it cut a lone client's share by an eighth, and crd-heavy,
// one client of the bench, by a fifth at 2 and 10 ms (#689).
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
