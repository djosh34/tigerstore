package wl

import (
	"bytes"
	"math"
	"slices"
	"time"
)

// Leases. A grant, a revoke and a keepalive go through the batch and the
// apply like any write, so every node holds the same leases and the same
// keys on each. Expiry is the leader's alone, as in etcd: it keeps each
// lease's deadline, which a keepalive's apply moves on, and proposes a
// revoke once a deadline passes. A new leader starts every lease's TTL
// over.

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

func (n *node) leading() bool { return n.lead == n.id && n.id != 0 }

// leaderChanged starts every lease's TTL over on a new leader, and drops
// the deadlines on a node that no longer leads.
func (n *node) leaderChanged() {
	lc := &n.leases
	lc.deadline, lc.revoking = nil, nil
	if !n.leading() {
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
		n.leases.deadline[id] = n.now.Add(time.Duration(l.ttl) * time.Second)
		delete(n.leases.revoking, id)
	}
}

// expireLeases proposes a revoke of every lease past its deadline, on the
// leader.
func (n *node) expireLeases() {
	lc := &n.leases
	for id, d := range lc.deadline {
		if n.now.Before(d) {
			continue
		}
		if at, ok := lc.revoking[id]; ok && n.now.Sub(at) < revokeRetry {
			continue
		}
		lc.revoking[id] = n.now
		n.addOp(0, kLeaseRevoke, appendVarintField(nil, 1, uint64(id)))
	}
}

// remaining is lease id's TTL left, in seconds: the leader's deadline, or
// the granted TTL on another node.
func (n *node) remaining(id int64, l *lease) int64 {
	if d, ok := n.leases.deadline[id]; ok {
		return max(0, int64(math.Ceil(d.Sub(n.now).Seconds())))
	}
	return l.ttl
}

// ---- requests

// leaseKeepAlive proposes a renewal; the stream stays open for the next.
func (n *node) leaseKeepAlive(c *client, s *stream, msg []byte) {
	n.openStream(c, s)
	n.addOp(mkHandle(c, s.id), kLeaseKeepAlive, msg)
}

// leaseTimeToLive answers from this node's leases.
func (n *node) leaseTimeToLive(c *client, s *stream, msg []byte) error {
	id, wantKeys := int64(0), false
	for m := msg; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return err
		}
		switch num {
		case 1:
			id = int64(v)
		case 2:
			wantKeys = v != 0
		}
		m = rest
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
	slices.Sort(keys) // every node deletes them in one order
	rev := st.rev + 1
	deleted := false
	for _, k := range keys {
		prev, err := st.del(k, rev)
		if err != nil {
			return err
		}
		if prev != nil {
			deleted = true
			n.w.record(st, k, true, rev, nil, prev)
		}
	}
	if deleted {
		st.rev = rev
	}
	delete(st.leases, id)
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
	}
	if c != nil {
		var w msgW
		w.begin(c, s, sizeBytesField(1, n.hdrSize())+sizeVarintField(2, uint64(id))+sizeVarintField(3, uint64(ttl)))
		n.writeHdr(&w)
		writeVarintNZ(&w, 2, uint64(id))
		writeVarintNZ(&w, 3, uint64(ttl))
		w.end()
		n.markDirty(&c.sock)
	}
	return nil
}

// checkLease fails a put whose lease does not exist.
func (n *node) checkLease(id int64) error {
	if id != 0 && n.st.leases[id] == nil {
		return requestErr(codeNotFound, msgLeaseNotFound)
	}
	return nil
}
