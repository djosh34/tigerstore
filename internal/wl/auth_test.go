package wl

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
)

// authNode is a node with root and a user alice of role rw, which may
// read and write [a, c), with auth on.
func authNode(t *testing.T) *node {
	t.Helper()
	n := leaseNode(t)
	n.auth, n.tokens, n.logins = newAuthStore(), map[string]*simpleToken{}, map[handle]authLogin{}
	hash := base64.StdEncoding.EncodeToString([]byte("not a real hash"))
	for _, r := range []*etcdpb.InternalRaftRequest{
		{AuthUserAdd: &etcdpb.AuthUserAddRequest{Name: "root", HashedPassword: hash}},
		{AuthRoleAdd: &etcdpb.AuthRoleAddRequest{Name: "root"}},
		{AuthUserGrantRole: &etcdpb.AuthUserGrantRoleRequest{User: "root", Role: "root"}},
		{AuthUserAdd: &etcdpb.AuthUserAddRequest{Name: "alice", HashedPassword: hash}},
		{AuthRoleAdd: &etcdpb.AuthRoleAddRequest{Name: "rw"}},
		{AuthRoleGrantPermission: &etcdpb.AuthRoleGrantPermissionRequest{Name: "rw", Perm: &authpb.Permission{PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("c")}}},
		{AuthUserGrantRole: &etcdpb.AuthUserGrantRoleRequest{User: "alice", Role: "rw"}},
		{AuthEnable: &etcdpb.AuthEnableRequest{}, Header: &etcdpb.RequestHeader{Username: "root", AuthRevision: 7}},
	} {
		if _, err := n.applyAuth(r, 1); err != nil {
			t.Fatalf("%v: %v", r, err)
		}
	}
	return n
}

func TestAWriteIsCheckedWithTheIdentityItCarries(t *testing.T) {
	n := authNode(t)
	alice := authInfo{user: "alice", revision: n.auth.revision}
	put := func(key string) []byte { return appendBytesField(nil, 1, []byte(key)) }
	if err := n.writeAllowed(kPut, put("b"), alice, true); err != nil {
		t.Fatalf("alice's put in [a, c): %v", err)
	}
	if err := n.writeAllowed(kPut, put("d"), alice, true); !errors.Is(err, errPermissionDenied) {
		t.Fatalf("alice's put of d gave %v", err)
	}
	if err := n.writeAllowed(kPut, put("b"), authInfo{}, false); !errors.Is(err, errUserEmpty) {
		t.Fatalf("a put without an identity gave %v", err)
	}
	old := authInfo{user: "alice", revision: n.auth.revision - 1}
	if err := n.writeAllowed(kPut, put("b"), old, true); !errors.Is(err, errAuthOldRevision) {
		t.Fatalf("a put of an older auth revision gave %v", err)
	}
	if err := n.writeAllowed(kLeaseRevoke, appendVarintField(nil, 1, 5), authInfo{}, false); err != nil {
		t.Fatalf("the leader's revoke of an expired lease gave %v", err)
	}
}

func TestACheckpointKeepsTheAuthStore(t *testing.T) {
	n := authNode(t)
	b := n.auth.encode(nil)
	a, err := (&ckptReader{r: bufio.NewReader(bytes.NewReader(b))}).readAuth()
	if err != nil {
		t.Fatal(err)
	}
	if !a.enabled || a.revision != n.auth.revision || len(a.users) != 2 || len(a.roles) != 2 {
		t.Fatalf("read back enabled %v revision %d, %d users, %d roles", a.enabled, a.revision, len(a.users), len(a.roles))
	}
	if err := a.permitted(authInfo{user: "alice", revision: a.revision}, []byte("a"), []byte("c"), true); err != nil {
		t.Fatalf("alice's permissions read back: %v", err)
	}
}

// A linearizable read is checked again once its read index is applied,
// as etcd checks a read after its barrier: a permission revoked while it
// waited is refused it. Checked only as it came in, alice would read b
// past the revoke.
func TestALinearizableReadIsCheckedAfterItsIndex(t *testing.T) {
	n := authNode(t)
	c, s := flowClient(t)
	c.node = n
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.now = time.Now()
	n.tokens["alice-token"] = &simpleToken{user: "alice"}
	s.token = "alice-token"

	if err := n.request(c, s, rangeMsg("b")); err != nil {
		t.Fatal(err)
	}
	if len(n.reads) != 1 {
		t.Fatalf("%d reads waiting for their index, want 1", len(n.reads))
	}
	revoke := &etcdpb.InternalRaftRequest{AuthRoleRevokePermission: &etcdpb.AuthRoleRevokePermissionRequest{Role: "rw", Key: []byte("a"), RangeEnd: []byte("c")}, Header: &etcdpb.RequestHeader{Username: "root", AuthRevision: n.auth.revision}}
	if _, err := n.applyAuth(revoke, 2); err != nil {
		t.Fatal(err)
	}
	n.readBatches = append(n.readBatches, &readBatch{reads: n.reads, buf: n.readBuf, known: true, index: n.applied})
	n.serveReads()
	if got := n.gs.calls[kRange].handled[codes.PermissionDenied]; got != 1 {
		t.Fatalf("%d reads refused for a permission revoked while they waited, want 1 (answers %v)", got, n.gs.calls[kRange].handled)
	}
}
