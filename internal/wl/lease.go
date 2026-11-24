package wl

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/planted"
)

// Leases. A grant, a revoke and a keepalive go through the batch and the
// apply like any write, so every node holds the same leases and the same
// keys on each. Expiry is the leader's alone, as in etcd: it keeps each
// lease's deadline, which a keepalive's apply moves on, and proposes a
// revoke once a deadline passes, while a quorum confirms that it still
// leads (confirmLead). A new leader starts every lease's TTL over.

type lease struct {
	ttl  int64
	keys map[uint32]struct{} // the keys attached, by id
}

const (
	minLeaseTTL = 2 // seconds: no shorter than an election may take
	maxLeaseTTL = 9000000000
	// revokeRetry is how long the leader waits on a revoke it proposed
	// before it proposes it again.
	revokeRetry = 5 * time.Second
	// confirmFor is how long after asking a ReadIndex that a quorum
	// answered the leader may decide expiries (confirmLead): half an
	// election timeout, so no other leader can have been elected since.
	confirmFor = electionTicks * tickEvery / 2

	msgLeaseNotFound = "etcdserver: requested lease not found"
	msgLeaseExists   = "etcdserver: lease already exists"
	msgLeaseTTLLarge = "etcdserver: too large lease TTL"
	codeNotFound     = 5
	codeFailedPrecon = 9
)

// leaseClock is the leader's deadlines of the leases.
type leaseClock struct {
	deadline map[int64]time.Time
	revoking map[int64]time.Time // revokes proposed and not applied yet
	// The ReadIndex that confirms the node still leads (confirmLead):
	// the one asked and not answered yet, 0 for none, and when; and when
	// the last one a quorum answered was asked.
	askSeq    uint64
	askedAt   time.Time
	confirmed time.Time
}

// setLease moves key id from lease from to lease to.
func (st *store) setLease(id uint32, from, to int64) {
	if from == to {
		return
	}
	if l := st.leases[from]; l != nil {
		delete(l.keys, id)
	}
	if l := st.leases[to]; l != nil {
		l.keys[id] = struct{}{}
	}
}

// withLeaseID returns a LeaseGrant that asks for no ID with one the node
// chose, as etcd's server chooses it before the grant goes through Raft.
// The IDs are etcd's shape, so that they compare with IDs clients ask for
// as etcd's do: the member's low 16 bits, then a count that starts at the
// node's start time in milliseconds (etcd's idutil), the top bit clear.
func (n *node) withLeaseID(msg []byte) []byte {
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return msg // the check refuses it
		}
		if num == 2 && v != 0 {
			return msg
		}
		m = rest
	}
	return appendVarintField(slices.Clone(msg), 2, n.newLeaseID())
}

// newLeaseID is the next lease ID of the node's own (withLeaseID).
func (n *node) newLeaseID() uint64 {
	const suffixBits = 48
	if n.leaseIDs == 0 {
		n.leaseIDs = uint64(time.Now().UnixMilli()) & (1<<40 - 1) << 8
	}
	for {
		n.leaseIDs++
		id := (uint64(uint16(n.id))<<suffixBits | n.leaseIDs&(1<<suffixBits-1)) & (1<<63 - 1)
		if id != 0 {
			return id
		}
	}
}

// leaseID reads the ID, field 1, of a lease request.
func leaseID(msg []byte) (int64, error) {
	id := int64(0)
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return 0, err
		}
		if num == 1 {
			id = int64(v)
		}
		m = rest
	}
	return id, nil
}

// parseLeaseTTL reads a LeaseTimeToLiveRequest: its ID and whether it
// asks for the keys.
func parseLeaseTTL(msg []byte) (id int64, keys bool, err error) {
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return 0, false, err
		}
		switch num {
		case 1:
			id = int64(v)
		case 2:
			keys = v != 0
		}
		m = rest
	}
	return id, keys, nil
}

func (n *node) leading() bool { return n.lead == n.id && n.id != 0 }

// leaderChanged starts every lease's TTL over on a new leader, and drops
// the deadlines on a node that no longer leads.
func (n *node) leaderChanged() {
	// What the node appended as an earlier leader may have been replaced
	// since: a new term's leader holds back for its own first entry,
	// which Raft's pendingConfIndex covers (#689's area-2 review).
	n.confHold.leadFirst, n.confHold.leadTerm, n.confHold.lastConf = 0, 0, 0
	n.leaseSeen = make(map[int64]time.Time, len(n.st.leases))
	for id := range n.st.leases {
		n.leaseSeen[id] = n.now
	}
	lc := &n.leases
	*lc = leaseClock{}
	if !n.leading() && !planted.On("B10") {
		return
	}
	lc.deadline = make(map[int64]time.Time, len(n.st.leases))
	lc.revoking = map[int64]time.Time{}
	for id, l := range n.st.leases {
		lc.deadline[id] = n.now.Add(time.Duration(l.ttl) * time.Second)
	}
}

// renewed moves lease id's deadline on, on the leader.
func (n *node) renewed(id int64) {
	if l := n.st.leases[id]; l != nil && n.leases.deadline != nil {
		n.gs.LeaseRenewed++
		n.leases.deadline[id] = n.now.Add(time.Duration(l.ttl) * time.Second)
		delete(n.leases.revoking, id)
	}
}

// renewProposed moves on, on the leader, the deadline of each lease that
// a keepalive in batch data renews, as its Raft log takes the batch: its
// own (proposeBatch) or a follower's (step). So the deadlines
// always count every keepalive in the log, and a revoke the leader puts
// in the log at once when it decides (expireLeases) comes after each of
// them: a keepalive the log takes later applies after the revoke, and
// every node answers it "lease not found", as etcd's leader does.
// Renewing at the apply instead let a leader whose disk stalled, which
// applies late, revoke a lease its followers had renewed and told the
// client so (seed 2 of TestWLStalledLeaderIsReplaced); renewing in the
// Ready after the decision let a keepalive the log took before the revoke
// apply alive on a follower, and the revoke then take the lease (#709).
// The apply renews again.
func (n *node) renewProposed(data []byte) {
	// Planted (B34, #709's kind): the leader moves a deadline only as it
	// applies a keepalive, and may decide a revoke with one ahead of it
	// in the log.
	if !isBatch(data) || planted.On("B34") {
		return
	}
	p := batchHdr
	for range binary.LittleEndian.Uint32(data[12:]) {
		if len(data) < p+opHdr {
			break
		}
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		if len(data) < p+opHdr+l {
			break
		}
		if data[p] == kLeaseKeepAlive {
			if id, err := leaseID(data[p+opHdr : p+opHdr+l]); err == nil {
				if _, revoking := n.leases.revoking[id]; !revoking {
					n.renewed(id)
				}
			}
		}
		p += opHdr + l
	}
}

// step steps a peer's message into Raft. A follower's proposal renews the
// leases its keepalives name first, as the leader's log takes it
// (renewProposed).
func (n *node) step(m *pb.Message) {
	if m.GetType() == pb.MsgReadIndex && n.readIndexUnsafe() {
		// A learner's ReadIndex, which Raft would answer short: dropped,
		// and the learner asks again (retryReads).
		return
	}
	if m.GetType() == pb.MsgProp && len(n.leases.deadline) > 0 {
		for _, e := range m.GetEntries() {
			if e.GetType() == pb.EntryNormal {
				n.renewProposed(e.GetData())
			}
		}
	}
	_ = n.rn.Step(m)
}

// expireLeases revokes every lease past its deadline, on the leader, once
// a quorum confirmed that it still leads (confirmLead). The turn's batch
// goes to the log first, renewing what its keepalives renew, and the
// revokes then go in a batch of their own at once: so each comes after
// every keepalive the log took before the decision, and before every one
// it takes after (renewProposed).
func (n *node) expireLeases() {
	lc := &n.leases
	due := func(id int64, d time.Time) bool {
		if n.now.Before(d) {
			return false
		}
		at, ok := lc.revoking[id]
		return !ok || n.now.Sub(at) >= revokeRetry
	}
	expired := false
	for id, d := range lc.deadline {
		if due(id, d) {
			expired = true
			break
		}
	}
	// A leader decides on the leases it applied of its own term at least
	// (termApplied): before, the entries a lone voter committed before a
	// restart may still revoke one, or grant it again under the same ID.
	if !expired || (n.leading() && (!n.termApplied() || !n.confirmLead())) {
		return
	}
	n.proposeBatch(n.now)
	for id, d := range lc.deadline {
		if !due(id, d) {
			continue
		}
		lc.revoking[id] = n.now
		n.simExpired(id)
		if !n.leading() && planted.On("B10") {
			// Planted: a follower expires the lease on its own clock,
			// without the log.
			_ = n.applyLeaseRevoke(appendVarintField(nil, 1, uint64(id)), nil, nil)
			continue
		}
		n.addOp(0, kLeaseRevoke, appendVarintField(nil, 1, uint64(id)))
	}
	n.proposeBatch(n.now)
}

// confirmLead reports whether a quorum answered a ReadIndex the node
// asked within confirmFor, and asks one if none is in flight. A node that
// was deposed without knowing it yet, as after a pause or on a stalled
// disk, holds deadlines that miss the keepalives the new leader took, and
// would decide expiries early (#710, seed 376 of TestSeeds): its revoke
// never commits, but etcd's rule is that no lease expires before its TTL
// ran out since its last renewal. A quorum that answers a heartbeat sent
// at the ask had elected no other leader by then, and none of them votes
// for another within an election timeout of answering (CheckQuorum), so
// within confirmFor of the ask the node is still the only leader.
func (n *node) confirmLead() bool {
	lc := &n.leases
	if !lc.confirmed.IsZero() && n.now.Sub(lc.confirmed) < confirmFor {
		return true
	}
	if lc.askSeq == 0 || n.now.Sub(lc.askedAt) >= confirmFor {
		n.readSeq++
		lc.askSeq, lc.askedAt = n.readSeq, n.now
		var ctx [8]byte
		binary.BigEndian.PutUint64(ctx[:], lc.askSeq)
		n.rn.ReadIndex(ctx[:])
	}
	return false
}

// leadConfirmed takes the answer to the ReadIndex of confirmLead.
func (n *node) leadConfirmed(seq uint64) bool {
	lc := &n.leases
	if seq == 0 || seq != lc.askSeq {
		return false
	}
	lc.confirmed, lc.askSeq = lc.askedAt, 0
	return true
}

// remaining is lease id's TTL left, in seconds: the leader's deadline, or
// on another node the TTL from when it last saw the lease renewed (its
// grant, a keepalive, a new leader), which the leader's is within the
// time a renewal takes to reach the node; the granted TTL if it saw none
// since it started.
func (n *node) remaining(id int64, l *lease) int64 {
	if d, ok := n.leases.deadline[id]; ok {
		return max(0, int64(math.Ceil(d.Sub(n.now).Seconds())))
	}
	if at, ok := n.leaseSeen[id]; ok {
		return max(0, int64(math.Ceil((time.Duration(l.ttl)*time.Second - n.now.Sub(at)).Seconds())))
	}
	return l.ttl
}

// sawRenewed notes that this node saw lease id renewed now.
func (n *node) sawRenewed(id int64) {
	if n.leaseSeen == nil {
		n.leaseSeen = map[int64]time.Time{}
	}
	n.leaseSeen[id] = n.now
}

// ---- requests

// leaseKeepAlive proposes a renewal; the stream stays open for the next.
func (n *node) leaseKeepAlive(c *client, s *stream, msg []byte) {
	n.openStream(c, s)
	n.addOp(mkHandle(c, s.id), kLeaseKeepAlive, msg)
}

// leaseTimeToLive answers from this node's leases, once its ReadIndex is
// applied (serveReads), as etcd answers from its applied state.
func (n *node) leaseTimeToLive(c *client, s *stream, msg []byte) error {
	id, wantKeys, err := parseLeaseTTL(msg)
	if err != nil {
		return err
	}
	ttl, granted := int64(-1), int64(0)
	var keys [][]byte
	if l := n.st.leases[id]; l != nil {
		ttl, granted = n.remaining(id, l), l.ttl
		if wantKeys {
			for k := range l.keys {
				keys = append(keys, n.st.key(k))
			}
			slices.SortFunc(keys, bytes.Compare)
		}
	}
	size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(id)) + sizeVarintField(3, uint64(ttl)) + sizeVarintField(4, uint64(granted))
	for _, k := range keys {
		size += sizeBytesField(5, len(k))
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	writeVarintNZ(&w, 2, uint64(id))
	writeVarintNZ(&w, 3, uint64(ttl))
	writeVarintNZ(&w, 4, uint64(granted))
	for _, k := range keys {
		w.bytesField(5, k)
	}
	n.endReply(c, s, &w)
	return nil
}

// leaseLeases lists this node's leases.
func (n *node) leaseLeases(c *client, s *stream) {
	ids := make([]int64, 0, len(n.st.leases))
	for id := range n.st.leases {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	size := sizeBytesField(1, n.hdrSize())
	for _, id := range ids {
		size += sizeBytesField(2, sizeVarintField(1, uint64(id)))
	}
	var w msgW
	n.beginReply(c, s, &w, size)
	n.writeHdr(&w)
	for _, id := range ids {
		w.lenField(2, sizeVarintField(1, uint64(id)))
		writeVarintNZ(&w, 1, uint64(id))
	}
	n.endReply(c, s, &w)
}

// ---- the apply

func (n *node) applyLeaseGrant(req []byte, c *client, s *stream) error {
	ttl, id := int64(0), int64(0)
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return badRequest(err)
		}
		switch num {
		case 1:
			ttl = int64(v)
		case 2:
			id = int64(v)
		}
		m = rest
	}
	if ttl > maxLeaseTTL {
		return requestErr(codeOutOfRange, msgLeaseTTLLarge)
	}
	ttl = max(ttl, minLeaseTTL)
	st := n.st
	if id == 0 {
		id = int64(n.applied+1)<<20 | int64(len(st.leases)&0xfffff)
		for st.leases[id] != nil {
			id++
		}
	} else if st.leases[id] != nil {
		return requestErr(codeFailedPrecon, msgLeaseExists)
	}
	st.leases[id] = &lease{ttl: ttl, keys: map[uint32]struct{}{}}
	n.sawRenewed(id)
	n.gs.LeaseGranted++
	n.gs.LeaseTTLs[min(bits.Len64(uint64(max(ttl-1, 0))), core.LeaseTTLBuckets)]++
	n.gs.LeaseTTLSum += ttl
	if n.leases.deadline != nil {
		n.leases.deadline[id] = n.now.Add(time.Duration(ttl) * time.Second)
	}
	if c != nil {
		var w msgW
		size := sizeBytesField(1, n.hdrSize()) + sizeVarintField(2, uint64(id)) + sizeVarintField(3, uint64(ttl))
		n.beginReply(c, s, &w, size)
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(id))
		writeVarintNZ(&w, 3, uint64(ttl))
		n.endReply(c, s, &w)
	}
	return nil
}

// applyLeaseRevoke deletes the lease and its keys, all at one revision.
func (n *node) applyLeaseRevoke(req []byte, c *client, s *stream) error {
	id, err := leaseID(req)
	if err != nil {
		return badRequest(err)
	}
	st := n.st
	l := st.leases[id]
	if l == nil {
		return requestErr(codeNotFound, msgLeaseNotFound)
	}
	keys := make([]uint32, 0, len(l.keys))
	for k := range l.keys {
		keys = append(keys, k)
	}
	// Every node deletes them in one order, by name as etcd does: ids
	// differ between nodes once freed ids are reused.
	slices.SortFunc(keys, func(a, b uint32) int { return bytes.Compare(st.key(a), st.key(b)) })
	rev := st.rev + 1
	deleted := false
	for _, k := range keys {
		prev, live, err := st.del(k, rev, n.w.wantsPrev(st, k, rev, true))
		if err != nil {
			return err
		}
		if live {
			deleted = true
			n.w.record(st, k, true, rev, nil, prev)
		}
	}
	if deleted {
		st.rev = rev
	}
	n.gs.LeaseRevoked++
	n.gs.deletes += int64(len(keys))
	if len(keys) > 1 {
		n.gs.txns++
	}
	if planted.On("B36") {
		// Planted (B36): the node remembers the TTL of a lease it revoked.
		if n.plantedRevoked == nil {
			n.plantedRevoked = map[int64]int64{}
		}
		n.plantedRevoked[id] = l.ttl
	}
	delete(st.leases, id)
	delete(n.leaseSeen, id)
	delete(n.leases.deadline, id)
	delete(n.leases.revoking, id)
	if c != nil {
		n.replyHeader(c, s)
	}
	return nil
}

// applyLeaseKeepAlive renews the lease and answers on the open stream, a
// TTL of 0 for a lease that is gone.
func (n *node) applyLeaseKeepAlive(req []byte, c *client, s *stream) error {
	id, err := leaseID(req)
	if err != nil {
		return badRequest(err)
	}
	ttl := int64(0)
	if l := n.st.leases[id]; l != nil {
		ttl = l.ttl
		n.renewed(id)
		n.sawRenewed(id)
		// Planted (B35): the answer gives the lease a second more than its
		// TTL. Every node decides the expiry on time by the TTL granted
		// (lease rule 1); the revoke then cuts what the client was told
		// (rule 3).
		if planted.On("B35") {
			ttl++
		}
	} else if planted.On("B36") {
		// Planted (B36): a keepalive applied after its lease's revoke is
		// answered alive, with the TTL the lease had. The expiry was on
		// time (lease rule 1); the client is told a revoked lease lives
		// (rule 2).
		ttl = n.plantedRevoked[id]
	}
	if c != nil {
		n.replyKeepAlive(c, s, id, ttl)
	}
	return nil
}

// replyKeepAlive answers a keepalive of lease id on its open stream s.
func (n *node) replyKeepAlive(c *client, s *stream, id, ttl int64) {
	var w msgW
	w.begin(c, s, sizeBytesField(1, n.hdrSize())+sizeVarintField(2, uint64(id))+sizeVarintField(3, uint64(ttl)))
	n.writeHdr(&w)
	writeVarintNZ(&w, 2, uint64(id))
	writeVarintNZ(&w, 3, uint64(ttl))
	w.end()
	n.markDirty(&c.sock)
}

// checkLease fails a put whose lease does not exist.
func (n *node) checkLease(id int64) error {
	if id != 0 && n.st.leases[id] == nil {
		return requestErr(codeNotFound, msgLeaseNotFound)
	}
	return nil
}
