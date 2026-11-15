package wl

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"
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

func TestTheLeaderRevokesExpiredLeases(t *testing.T) {
	n := leaseNode(t)
	grant(t, n, 7, 2)
	grant(t, n, 8, 60)
	n.lead = n.id
	n.leaderChanged()
	n.now = n.now.Add(3 * time.Second)
	n.expireLeases()
	n.expireLeases() // not twice while the first is in flight
	if n.batch == nil || n.batchOps != 1 {
		t.Fatalf("the leader proposed %d ops, want one revoke", n.batchOps)
	}
	op := n.batch[batchHdr:]
	if op[0] != kLeaseRevoke {
		t.Fatalf("the op is of kind %d, want a revoke", op[0])
	}
	if id, _ := leaseID(op[opHdr : opHdr+int(binary.LittleEndian.Uint32(op[1:]))]); id != 7 {
		t.Fatalf("the leader revokes lease %d, want 7", id)
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
	if err := checkRequest(kTxn, outer); err != nil {
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
