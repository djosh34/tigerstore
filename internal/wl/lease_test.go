package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// leaseNode is a node with a store, for the apply of lease requests.
func leaseNode(t *testing.T) *node {
	t.Helper()
	n, l := testNode(t)
	st, err := newStore()
	if err != nil {
		t.Fatal(err)
	}
	st.log = l
	n.st = st
	n.now = time.Now()
	return n
}

func grant(t *testing.T, n *node, id, ttl int64) {
	t.Helper()
	req := appendVarintField(appendVarintField(nil, 1, uint64(ttl)), 2, uint64(id))
	if err := n.applyLeaseGrant(req, nil, nil); err != nil {
		t.Fatal(err)
	}
	n.applied++
}

func putLeased(n *node, key string, lease int64) error {
	req := appendBytesField(appendBytesField(nil, 1, []byte(key)), 2, []byte("v"))
	if lease != 0 {
		req = appendVarintField(req, 3, uint64(lease))
	}
	err := n.applyPut(req, 0, nil, nil)
	n.st.endApply()
	return err
}

func live(n *node, key string) bool {
	id, ok := n.st.find([]byte(key))
	return ok && n.st.exists(id)
}

func TestRevokeDeletesTheLeaseKeysAtOneRevision(t *testing.T) {
	n := leaseNode(t)
	grant(t, n, 7, 60)
	for _, k := range []string{"/a", "/b"} {
		if err := putLeased(n, k, 7); err != nil {
			t.Fatal(err)
		}
	}
	if err := putLeased(n, "/c", 0); err != nil {
		t.Fatal(err)
	}
	// A put without the lease takes the key off it.
	if err := putLeased(n, "/b", 0); err != nil {
		t.Fatal(err)
	}
	rev := n.st.rev
	if err := n.applyLeaseRevoke(appendVarintField(nil, 1, 7), nil, nil); err != nil {
		t.Fatal(err)
	}
	if live(n, "/a") || !live(n, "/b") || !live(n, "/c") {
		t.Fatalf("after the revoke /a %v /b %v /c %v, want only /a gone", live(n, "/a"), live(n, "/b"), live(n, "/c"))
	}
	if n.st.rev != rev+1 {
		t.Fatalf("the revoke moved the revision from %d to %d, want one step", rev, n.st.rev)
	}
	if n.st.leases[7] != nil {
		t.Fatal("the lease outlived its revoke")
	}
	err := n.applyLeaseRevoke(appendVarintField(nil, 1, 7), nil, nil)
	if re, ok := errors.AsType[*requestError](err); !ok || re.msg != msgLeaseNotFound {
		t.Fatalf("a second revoke: %v", err)
	}
}

func TestPutToAMissingLeaseFails(t *testing.T) {
	n := leaseNode(t)
	err := putLeased(n, "/a", 9)
	if re, ok := errors.AsType[*requestError](err); !ok || re.code != codeNotFound {
		t.Fatalf("a put to a missing lease: %v", err)
	}
	if live(n, "/a") {
		t.Fatal("the put to a missing lease wrote its key")
	}
}

// leaderNode is a leaseNode that leads a Raft group of itself alone.
func leaderNode(t *testing.T) (*node, *raft.MemoryStorage) {
	t.Helper()
	n := leaseNode(t)
	ms := raft.NewMemoryStorage()
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{n.id}}}}
	if err := ms.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	rn, err := raft.NewRawNode(raftConfig(n.id, ms, sizeFor(0), 1))
	if err != nil {
		t.Fatal(err)
	}
	n.rn = rn
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		logOps(t, n, ms)
	}
	if rn.Status().RaftState != raft.StateLeader {
		t.Fatal("the node did not become the leader")
	}
	n.lead = n.id
	return n, ms
}

// logOp is a lease op in the log: its kind and its lease.
type logOp struct {
	kind uint8
	id   int64
}

// logOps hands n's Raft its storage's work and n its ReadIndex answers,
// and returns the lease ops its log took since the last call, in the
// log's order.
func logOps(t *testing.T, n *node, ms *raft.MemoryStorage) []logOp {
	t.Helper()
	var ops []logOp
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		for _, rs := range rd.ReadStates {
			n.readState(rs.RequestCtx, rs.Index)
		}
		for _, m := range rd.Messages {
			if m.GetTo() == raft.LocalAppendThread {
				if err := ms.Append(m.GetEntries()); err != nil {
					t.Fatal(err)
				}
				for _, e := range m.GetEntries() {
					ops = append(ops, batchLeaseOps(e.GetData())...)
				}
			}
			for _, r := range m.GetResponses() {
				_ = n.rn.Step(r)
			}
		}
	}
	return ops
}

func batchLeaseOps(data []byte) []logOp {
	var ops []logOp
	if !isBatch(data) {
		return nil
	}
	p := batchHdr
	for range binary.LittleEndian.Uint32(data[12:]) {
		l := int(binary.LittleEndian.Uint32(data[p+1:]))
		if k := data[p]; k == kLeaseKeepAlive || k == kLeaseRevoke {
			id, _ := leaseID(data[p+opHdr : p+opHdr+l])
			ops = append(ops, logOp{k, id})
		}
		p += opHdr + l
	}
	return ops
}

func TestTheLeaderRevokesExpiredLeases(t *testing.T) {
	n, ms := leaderNode(t)
	grant(t, n, 7, 2)
	grant(t, n, 8, 60)
	n.leaderChanged()
	n.now = n.now.Add(3 * time.Second)
	n.expireLeases()
	if ops := logOps(t, n, ms); len(ops) > 0 {
		t.Fatalf("the leader's log took %v before a quorum confirmed it leads", ops)
	}
	n.expireLeases()
	n.expireLeases() // not twice while the first is in flight
	if ops := logOps(t, n, ms); !slices.Equal(ops, []logOp{{kLeaseRevoke, 7}}) {
		t.Fatalf("the leader's log took %v, want one revoke of lease 7", ops)
	}
	// A keepalive moves the deadline on.
	n.now = n.now.Add(58 * time.Second)
	if err := n.applyLeaseKeepAlive(appendVarintField(nil, 1, 8), nil, nil); err != nil {
		t.Fatal(err)
	}
	if d := n.leases.deadline[8].Sub(n.now); d != 60*time.Second {
		t.Fatalf("lease 8's deadline is %v away after a keepalive, want 60s", d)
	}
}

// A leader moves a lease's deadline on when its log takes a keepalive,
// before it applies it: a leader whose disk stalled applies late, and must
// not revoke a lease its followers renewed meanwhile and told the client
// so (seed 2 of TestWLStalledLeaderIsReplaced).
func TestTheLeaderRenewsALeaseWhenItsLogTakesTheKeepalive(t *testing.T) {
	n, ms := leaderNode(t)
	grant(t, n, 7, 2)
	n.leaderChanged()
	n.now = n.now.Add(1500 * time.Millisecond)
	n.addOp(0, kLeaseKeepAlive, appendVarintField(nil, 1, 7))
	n.proposeBatch(n.now)
	// Nothing applies, as on a stalled disk.
	n.now = n.now.Add(time.Second)
	n.expireLeases()
	if ops := logOps(t, n, ms); !slices.Equal(ops, []logOp{{kLeaseKeepAlive, 7}}) {
		t.Fatalf("the leader's log took %v, want the keepalive alone", ops)
	}
	if d := n.leases.deadline[7].Sub(n.now); d != time.Second {
		t.Fatalf("lease 7's deadline is %v away, want 1s", d)
	}
}

// A keepalive the leader's log takes before the leader decides to revoke
// the lease renews it, and one it takes after comes after the revoke in
// the log, so every node answers it "lease not found". Before, the leader
// renewed in the Ready after the decision, so a keepalive its log took in
// the same turn, or that waited in its batch, applied alive before the
// revoke: a follower told the client "TTL 2", and the revoke took the
// lease at once (#709; seed 4 of TestWLRecoversFromATornWriteOfTheStage,
// seed 251 of TestSeeds).
func TestAKeepaliveAndTheRevokeAgree(t *testing.T) {
	keepAlive := func(n *node) *pb.Message {
		f, _ := testNode(t)
		f.id = 2
		f.addOp(0, kLeaseKeepAlive, appendVarintField(nil, 1, 7))
		data := slices.Clip(f.batch)
		binary.LittleEndian.PutUint32(data[12:], uint32(f.batchOps))
		return &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(n.id), Entries: []*pb.Entry{{Data: data}}}
	}
	for _, tc := range []struct {
		name string
		// before runs after the lease's TTL ran out, before the leader's
		// tick decides; after runs after it.
		before, after func(n *node)
		want          []logOp
	}{{
		name:   "a follower's keepalive stepped before the tick",
		before: func(n *node) { n.step(keepAlive(n)) },
		want:   []logOp{{kLeaseKeepAlive, 7}},
	}, {
		name:   "the leader's own keepalive waiting in its batch",
		before: func(n *node) { n.addOp(0, kLeaseKeepAlive, appendVarintField(nil, 1, 7)) },
		want:   []logOp{{kLeaseKeepAlive, 7}},
	}, {
		name:  "a follower's keepalive stepped after the tick",
		after: func(n *node) { n.step(keepAlive(n)) },
		want:  []logOp{{kLeaseRevoke, 7}, {kLeaseKeepAlive, 7}},
	}, {
		name:  "the leader's own keepalive after the tick",
		after: func(n *node) { n.addOp(0, kLeaseKeepAlive, appendVarintField(nil, 1, 7)) },
		want:  []logOp{{kLeaseRevoke, 7}, {kLeaseKeepAlive, 7}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			n, ms := leaderNode(t)
			grant(t, n, 7, 2)
			n.leaderChanged()
			logOps(t, n, ms)
			n.now = n.now.Add(2030 * time.Millisecond)
			n.expireLeases()
			logOps(t, n, ms) // a quorum confirms the node leads
			if tc.before != nil {
				tc.before(n)
			}
			// The loop's tick, then its turn's proposal.
			n.expireLeases()
			if tc.after != nil {
				tc.after(n)
			}
			n.proposeBatch(n.now)
			if ops := logOps(t, n, ms); !slices.Equal(ops, tc.want) {
				t.Fatalf("the leader's log took %v, want %v", ops, tc.want)
			}
		})
	}
}

// A leader decides an expiry only within confirmFor of asking a
// ReadIndex that a quorum answered. One deposed without knowing it yet,
// as after a pause, holds deadlines that miss the keepalives the new
// leader took, and decided the expiry as it resumed, before it heard of
// the new leader (#710, seed 376 of TestSeeds).
func TestALeaderRevokesOnlyWhileAQuorumConfirmsItLeads(t *testing.T) {
	n, ms := leaderNode(t)
	grant(t, n, 7, 2)
	n.leaderChanged()
	logOps(t, n, ms)
	// The node resumes from a pause past the lease's TTL.
	n.now = n.now.Add(3 * time.Second)
	n.expireLeases()
	if ops := logOps(t, n, ms); len(ops) > 0 {
		t.Fatalf("the leader's log took %v as it resumed, before a quorum confirmed it leads", ops)
	}
	// Answered, but the node pauses again before its next tick: the
	// answer is too old, and it asks again.
	n.now = n.now.Add(confirmFor)
	n.expireLeases()
	if ops := logOps(t, n, ms); len(ops) > 0 {
		t.Fatalf("the leader's log took %v on an answer asked %v before", ops, confirmFor)
	}
	// Answered within confirmFor: it decides.
	n.now = n.now.Add(confirmFor - time.Millisecond)
	n.expireLeases()
	if ops := logOps(t, n, ms); !slices.Equal(ops, []logOp{{kLeaseRevoke, 7}}) {
		t.Fatalf("the leader's log took %v, want one revoke of lease 7", ops)
	}
	// A node deposed meanwhile takes no answer to the ask it made as
	// leader.
	grant(t, n, 8, 2)
	n.now = n.now.Add(3 * time.Second)
	n.expireLeases()
	seq := n.leases.askSeq
	n.lead = 0
	n.leaderChanged()
	if n.leadConfirmed(seq) {
		t.Fatal("a deposed node took the answer to the ask it made as leader")
	}
}

func TestShortLeasesGetTheMinimumTTL(t *testing.T) {
	n := leaseNode(t)
	grant(t, n, 3, 1)
	if l := n.st.leases[3]; l == nil || l.ttl != minLeaseTTL {
		t.Fatalf("lease granted with a TTL of 1: %+v", l)
	}
}

func TestANestedTxnRunsTheBranchItsComparesPick(t *testing.T) {
	n := leaseNode(t)
	if err := putLeased(n, "/flag", 0); err != nil { // rev 2
		t.Fatal(err)
	}
	// Compare: /flag's create revision equals 2 (target CREATE 1, result EQUAL 0).
	cmpFlag := appendBytesField(appendVarintField(appendVarintField(nil, 2, 1), 5, 2), 3, []byte("/flag"))
	nested := appendBytesField(nil, 1, cmpFlag)
	nested = appendBytesField(nested, 2, reqOp(2, putOp("/yes", "1")))
	nested = appendBytesField(nested, 3, reqOp(2, putOp("/no", "1")))
	outer := txn(reqOp(2, putOp("/outer", "1")), reqOp(4, nested))
	if err := checkRequest(kTxn, outer, 0); err != nil {
		t.Fatal(err)
	}
	rev := n.st.rev
	if err := n.applyTxn(outer, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	n.st.endApply()
	if !live(n, "/outer") || !live(n, "/yes") || live(n, "/no") {
		t.Fatalf("/outer %v /yes %v /no %v, want the outer put and the nested success branch", live(n, "/outer"), live(n, "/yes"), live(n, "/no"))
	}
	if n.st.rev != rev+1 {
		t.Fatalf("the Txn moved the revision from %d to %d, want one step", rev, n.st.rev)
	}
	ids := []string{"/outer", "/yes"}
	for _, k := range ids {
		id, _ := n.st.find([]byte(k))
		if mod, _, _, _ := n.st.state(id); mod != rev+1 {
			t.Fatalf("%s has mod revision %d, want %d", k, mod, rev+1)
		}
	}
}

// A grant that asks for no ID gets one the node chose before proposing,
// in etcd's shape: above the IDs clients ask for, as etcd's are, so that
// a Txn compares leases as etcd does (difffuzz seed-768971567500001: the
// apply's IDs, from the applied index, compared below an asked one).
func TestALeaseIDTheNodeChoosesIsEtcdShaped(t *testing.T) {
	n := leaseNode(t)
	n.id = 0x1234abcd
	asked := appendVarintField(appendVarintField(nil, 1, 60), 2, 5977638232012)
	if got := n.withLeaseID(asked); !bytes.Equal(got, asked) {
		t.Fatal("a grant that asked for an ID got another")
	}
	plain := appendVarintField(nil, 1, 60)
	var last int64
	for range 3 {
		req := n.withLeaseID(plain)
		id, err := grantedID(req)
		if err != nil {
			t.Fatal(err)
		}
		if id>>48 != 0xabcd&0x7fff || id <= last || id <= 5977638232012 {
			t.Fatalf("the node chose lease ID %#x after %#x; want the member's low 16 bits on top (its top bit clear), rising, above asked IDs", id, last)
		}
		last = id
		if err := n.applyLeaseGrant(req, nil, nil); err != nil {
			t.Fatal(err)
		}
		if n.st.leases[id] == nil {
			t.Fatalf("lease %#x was not granted with the ID the node chose", id)
		}
	}
	if !bytes.Equal(plain, appendVarintField(nil, 1, 60)) {
		t.Fatal("choosing an ID changed the request it was given")
	}
}

// grantedID reads the ID, field 2, of a LeaseGrant.
func grantedID(req []byte) (int64, error) {
	id := int64(0)
	for m := req; len(m) > 0; {
		num, _, v, _, rest, err := field(m)
		if err != nil {
			return 0, err
		}
		if num == 2 {
			id = int64(v)
		}
		m = rest
	}
	return id, nil
}

// As etcd's, whose compacted revision starts at -1, a store's first
// Compact may name revision 0, and a second may not (difffuzz
// seed-761104251500003, which failed only where nothing in the run had
// compacted before it).
func TestTheFirstCompactionMayNameRevisionZero(t *testing.T) {
	n := leaseNode(t)
	req := appendVarintField(nil, 1, 0)
	if err := n.applyCompact(req, nil, nil); err != nil {
		t.Fatalf("the first compaction at revision 0: %v", err)
	}
	if got := rejected(n.applyCompact(req, nil, nil)); got != msgCompacted {
		t.Fatalf("a second compaction at revision 0 got %q, want %q", got, msgCompacted)
	}
}

// A Txn's read at revision -1 passes etcd's check while nothing was
// compacted, its first revision being -1, and fails as compacted once
// anything was, a compaction at 0 too (difffuzz seed-774452638500002).
func TestATxnReadAtMinusOneIsCompactedOnceAnythingWas(t *testing.T) {
	n := leaseNode(t)
	minusOne := uint64(1<<64 - 1) // -1 as a varint
	get := appendVarintField(appendBytesField(nil, 1, []byte("/k")), 4, minusOne)
	ops := txn(reqOp(1, get))
	var path []bool
	if err := n.checkTxnOps(ops, true, &path); err != nil {
		t.Fatalf("before any compaction: %v", err)
	}
	if err := n.applyCompact(appendVarintField(nil, 1, 0), nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := rejected(n.checkTxnOps(ops, true, &path)); got != msgCompacted {
		t.Fatalf("after a compaction at 0, a read at -1 got %q, want %q", got, msgCompacted)
	}
}
