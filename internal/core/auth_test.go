package core

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestCovers checks the merged permission spans against etcd's interval
// tree: a key, a range, a prefix and the open range from a key.
func TestCovers(t *testing.T) {
	perms := []*authpb.Permission{
		{PermType: authpb.Permission_READ, Key: []byte("c")},
		{PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("c")},
		{PermType: authpb.Permission_WRITE, Key: []byte("x"), RangeEnd: []byte{0}},
		{PermType: authpb.Permission_READ, Key: []byte("m"), RangeEnd: []byte("o")},
		{PermType: authpb.Permission_READ, Key: []byte("n"), RangeEnd: []byte("p")},
	}
	a := newAuthState()
	a.roles["r"] = &authpb.Role{Name: []byte("r"), KeyPermission: perms}
	a.users["u"] = &authpb.User{Name: []byte("u"), Roles: []string{"r"}}
	a.rebuild()
	p := a.perms["u"]
	for _, c := range []struct {
		key, end string
		write    bool
		want     bool
	}{
		{"a", "", false, true},
		{"b", "", true, true},
		{"c", "", false, true},
		{"c", "", true, false},
		// [a, c) and the key c touch: [a, c\x00) is readable as one.
		{"a", "c\x00", false, true},
		{"a", "d", false, false},
		{"a", "c", true, true},
		{"a", "c\x00", true, false},
		// [m, o) and [n, p) overlap.
		{"m", "p", false, true},
		{"m", "p\x00", false, false},
		{"l", "n", false, false},
		{"x", "\x00", true, true},
		{"zzz", "", true, true},
		{"w", "\x00", true, false},
		{"x", "\x00", false, false},
	} {
		spans := p.read
		if c.write {
			spans = p.write
		}
		var end []byte
		if c.end != "" {
			end = []byte(c.end)
		}
		if got := covers(spans, []byte(c.key), end); got != c.want {
			t.Errorf("covers(%q, %q, write %v) = %v", c.key, c.end, c.write, got)
		}
	}
}

func hashOf(password string) string {
	return base64.StdEncoding.EncodeToString([]byte("hash of " + password))
}

// login authenticates user with a simple token's prefix and returns the
// token.
func (s *single) login(id uint64, user string) Caller {
	s.t.Helper()
	resp, ok := s.ok(id, &pb.InternalRaftRequest{Authenticate: &pb.InternalAuthenticateRequest{Name: user, SimpleToken: "prefix" + user}}).(*pb.AuthenticateResponse)
	if !ok || !strings.HasPrefix(resp.Token, "prefix"+user+".") {
		s.t.Fatalf("authenticate %s: %v", user, resp)
	}
	return Caller{Token: resp.Token}
}

// doAs submits op as caller.
func (s *single) doAs(id uint64, c Caller, op proto.Message) Reply {
	s.t.Helper()
	as := s.as
	s.as = c
	defer func() { s.as = as }()
	return s.do(id, op)
}

// TestAuth follows etcd v3.7.2's auth store: enabling needs root with the
// root role; with auth on, a request needs an identity, writes are
// checked against the user's merged permissions when they apply, admin
// operations need root, a write taken before an auth change fails with
// the old revision, simple tokens expire and changes of a user drop
// them; and users, roles, the switch and the revision survive a restart
// from a checkpoint, while tokens do not.
func TestAuth(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) {
		tinyCheckpoints(cfg)
		cfg.AuthTokenTicks = 20
	}).capable()
	auth := func(id uint64, r *pb.InternalRaftRequest, want error) proto.Message {
		t.Helper()
		rep := s.do(id, r)
		if !errors.Is(rep.Err, want) {
			t.Fatalf("%v: %v, want %v", r, rep.Err, want)
		}
		return rep.Resp
	}
	auth(1, &pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}}, rpctypes.ErrGRPCRootUserNotExist)
	auth(2, &pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", HashedPassword: hashOf("pw")}}, nil)
	auth(3, &pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", HashedPassword: hashOf("pw")}}, rpctypes.ErrGRPCUserAlreadyExist)
	auth(4, &pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}}, rpctypes.ErrGRPCRootRoleNotExist)
	auth(5, &pb.InternalRaftRequest{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "root", Role: "root"}}, nil)
	auth(6, &pb.InternalRaftRequest{AuthRoleAdd: &pb.AuthRoleAddRequest{Name: "r"}}, nil)
	auth(7, &pb.InternalRaftRequest{AuthRoleGrantPermission: &pb.AuthRoleGrantPermissionRequest{Name: "r", Perm: &authpb.Permission{
		PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("c"),
	}}}, nil)
	auth(8, &pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "alice", HashedPassword: hashOf("pw")}}, nil)
	auth(9, &pb.InternalRaftRequest{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "alice", Role: "r"}}, nil)
	auth(10, &pb.InternalRaftRequest{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "alice", Role: "nope"}}, rpctypes.ErrGRPCRoleNotFound)
	auth(11, &pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}}, nil)
	st := auth(12, &pb.InternalRaftRequest{AuthStatus: &pb.AuthStatusRequest{}}, nil).(*pb.AuthStatusResponse)
	if !st.Enabled || st.AuthRevision != 7 {
		t.Fatalf("status: %v", st)
	}

	// No identity.
	for i, op := range []proto.Message{
		&pb.PutRequest{Key: []byte("a"), Value: []byte("1")},
		&pb.RangeRequest{Key: []byte("a")},
		&pb.InternalRaftRequest{AuthRoleList: &pb.AuthRoleListRequest{}},
		&pb.LeaseGrantRequest{TTL: 10},
		&pb.LeaseRevokeRequest{ID: 1},
		&pb.MemberListRequest{},
	} {
		if r := s.do(uint64(200+i), op); !errors.Is(r.Err, rpctypes.ErrGRPCUserEmpty) {
			t.Fatalf("%T without an identity: %v", op, r.Err)
		}
	}
	if r := s.doAs(25, Caller{Token: "bogus.1"}, &pb.RangeRequest{Key: []byte("a")}); !errors.Is(r.Err, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("bogus token: %v", r.Err)
	}

	root := s.login(30, "root")
	alice := s.login(31, "alice")
	allowed := []proto.Message{
		&pb.PutRequest{Key: []byte("a"), Value: []byte("1")},
		&pb.PutRequest{Key: []byte("b"), Value: []byte("1"), PrevKv: true},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c")},
		&pb.DeleteRangeRequest{Key: []byte("b")},
		&pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_VERSION, Result: pb.Compare_GREATER}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("bb")}}}},
		},
		&pb.InternalRaftRequest{AuthUserGet: &pb.AuthUserGetRequest{Name: "alice"}},
		&pb.InternalRaftRequest{AuthRoleGet: &pb.AuthRoleGetRequest{Role: "r"}},
	}
	for i, op := range allowed {
		if r := s.doAs(uint64(40+i), alice, op); r.Err != nil {
			t.Fatalf("alice %T: %v", op, r.Err)
		}
	}
	if err := s.n.Authorize(alice, &pb.StatusRequest{}); err != nil {
		t.Fatalf("alice's status: %v", err)
	}
	if err := s.n.Authorize(Caller{}, &pb.StatusRequest{}); !errors.Is(err, rpctypes.ErrGRPCUserEmpty) {
		t.Fatalf("status without an identity: %v", err)
	}
	if err := s.n.Authorize(alice, &pb.SnapshotRequest{}); !errors.Is(err, rpctypes.ErrGRPCPermissionDenied) {
		t.Fatalf("alice's snapshot: %v", err)
	}
	if err := s.n.Authorize(root, &pb.SnapshotRequest{}); err != nil {
		t.Fatalf("root's snapshot: %v", err)
	}
	denied := []proto.Message{
		&pb.PutRequest{Key: []byte("c"), Value: []byte("1")},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("d")},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}},
		&pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("z")},
		&pb.TxnRequest{Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("z")}}}}},
		&pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("z"), Target: pb.Compare_VERSION}}},
		&pb.CompactionRequest{Revision: 1},
		&pb.InternalRaftRequest{AuthUserGet: &pb.AuthUserGetRequest{Name: "root"}},
		&pb.InternalRaftRequest{AuthRoleGet: &pb.AuthRoleGetRequest{Role: "root"}},
		&pb.InternalRaftRequest{AuthUserList: &pb.AuthUserListRequest{}},
		&pb.InternalRaftRequest{AuthDisable: &pb.AuthDisableRequest{}},
	}
	for i, op := range denied {
		if r := s.doAs(uint64(60+i), alice, op); !errors.Is(r.Err, rpctypes.ErrGRPCPermissionDenied) {
			t.Fatalf("alice %T: %v, want permission denied", op, r.Err)
		}
	}
	users := s.doAs(80, root, &pb.InternalRaftRequest{AuthUserList: &pb.AuthUserListRequest{}})
	if l, ok := users.Resp.(*pb.AuthUserListResponse); !ok || !slices.Equal(l.Users, []string{"alice", "root"}) {
		t.Fatalf("user list: %v %v", users.Resp, users.Err)
	}
	if r := s.doAs(81, root, &pb.PutRequest{Key: []byte("z"), Value: []byte("1")}); r.Err != nil {
		t.Fatalf("root put: %v", r.Err)
	}
	// etcd checks the keys of a lease for LeaseTimeToLive only when the
	// request asks for them.
	s.doAs(83, root, &pb.LeaseGrantRequest{ID: 7, TTL: 100})
	if r := s.doAs(84, root, &pb.PutRequest{Key: []byte("z"), Value: []byte("1"), Lease: 7}); r.Err != nil {
		t.Fatalf("root put with a lease: %v", r.Err)
	}
	if r := s.doAs(85, alice, &pb.LeaseTimeToLiveRequest{ID: 7}); r.Err != nil {
		t.Fatalf("alice's time to live without keys: %v", r.Err)
	}
	if r := s.doAs(86, alice, &pb.LeaseTimeToLiveRequest{ID: 7, Keys: true}); !errors.Is(r.Err, rpctypes.ErrGRPCPermissionDenied) {
		t.Fatalf("alice's time to live with keys: %v, want permission denied", r.Err)
	}
	if r := s.doAs(82, root, &pb.InternalRaftRequest{AuthUserRevokeRole: &pb.AuthUserRevokeRoleRequest{Name: "root", Role: "root"}}); !errors.Is(r.Err, rpctypes.ErrGRPCInvalidAuthMgmt) {
		t.Fatalf("revoking root from root: %v", r.Err)
	}

	// A write that took alice's identity before a change of auth applies
	// after it, with the old revision.
	s.n.Submit(Request{ID: 90, Op: &pb.InternalRaftRequest{AuthRoleAdd: &pb.AuthRoleAddRequest{Name: "other"}}, Caller: root})
	s.n.Submit(Request{ID: 91, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}, Caller: alice})
	for _, r := range s.run() {
		if r.ID == 90 && r.Err != nil {
			t.Fatalf("role add: %v", r.Err)
		}
		if r.ID == 91 && !errors.Is(r.Err, rpctypes.ErrGRPCAuthOldRevision) {
			t.Fatalf("put with the old revision: %v", r.Err)
		}
	}

	// A new password drops the user's tokens.
	auth(100, &pb.InternalRaftRequest{
		Header:                 &pb.RequestHeader{Username: "root", AuthRevision: 100},
		AuthUserChangePassword: &pb.AuthUserChangePasswordRequest{Name: "alice", HashedPassword: hashOf("new")},
	}, rpctypes.ErrGRPCUserEmpty)
	if r := s.doAs(101, root, &pb.InternalRaftRequest{AuthUserChangePassword: &pb.AuthUserChangePasswordRequest{Name: "alice", HashedPassword: hashOf("new")}}); r.Err != nil {
		t.Fatalf("change password: %v", r.Err)
	}
	if r := s.doAs(102, alice, &pb.RangeRequest{Key: []byte("a")}); !errors.Is(r.Err, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("alice's token after a new password: %v", r.Err)
	}

	// An unused token expires; a used one lives on.
	alice = s.login(110, "alice")
	for range 15 {
		s.n.Tick()
		s.run()
	}
	if r := s.doAs(111, root, &pb.RangeRequest{Key: []byte("a")}); r.Err != nil {
		t.Fatalf("root's token: %v", r.Err)
	}
	// Tokens expire once a second.
	for range 15 {
		s.n.Tick()
		s.run()
	}
	if r := s.doAs(112, alice, &pb.RangeRequest{Key: []byte("a")}); !errors.Is(r.Err, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("alice's unused token: %v", r.Err)
	}
	if r := s.doAs(113, root, &pb.RangeRequest{Key: []byte("a")}); r.Err != nil {
		t.Fatalf("root's token after use: %v", r.Err)
	}

	s.as = root
	revision := s.n.state.auth.revision
	old := s
	s = restartFromCheckpoint(t, s, disk, func(i int) proto.Message {
		// The values of the keys so far hold the first segment until
		// they are deleted and compacted.
		switch {
		case i == 0:
			return &pb.DeleteRangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}
		case i%5 == 4:
			return &pb.CompactionRequest{Revision: old.n.kv.Rev()}
		}
		return &pb.PutRequest{Key: []byte("a"), Value: []byte{byte(i)}}
	})
	if r := s.doAs(120, root, &pb.RangeRequest{Key: []byte("a")}); !errors.Is(r.Err, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("root's token after the restart: %v", r.Err)
	}
	if a := s.n.state.auth; !a.enabled || a.revision != revision {
		t.Fatalf("after the restart: enabled %v, revision %d, want %d", a.enabled, a.revision, revision)
	}
	alice = s.login(121, "alice")
	if r := s.doAs(122, alice, &pb.PutRequest{Key: []byte("b"), Value: []byte("1")}); r.Err != nil {
		t.Fatalf("alice after the restart: %v", r.Err)
	}
	if r := s.doAs(123, alice, &pb.PutRequest{Key: []byte("z"), Value: []byte("1")}); !errors.Is(r.Err, rpctypes.ErrGRPCPermissionDenied) {
		t.Fatalf("alice outside her role after the restart: %v", r.Err)
	}
	root = s.login(124, "root")
	roles := s.doAs(125, root, &pb.InternalRaftRequest{AuthRoleList: &pb.AuthRoleListRequest{}})
	// As in etcd, granting the root role does not add it.
	if l, ok := roles.Resp.(*pb.AuthRoleListResponse); !ok || !slices.Equal(l.Roles, []string{"other", "r"}) {
		t.Fatalf("role list after the restart: %v %v", roles.Resp, roles.Err)
	}
	if r := s.doAs(126, root, &pb.InternalRaftRequest{AuthDisable: &pb.AuthDisableRequest{}}); r.Err != nil {
		t.Fatalf("disable: %v", r.Err)
	}
	if r := s.do(127, &pb.PutRequest{Key: []byte("z"), Value: []byte("2")}); r.Err != nil {
		t.Fatalf("put with auth disabled: %v", r.Err)
	}
}

// TestTokenWaitTimesOut sends a request with a token whose Authenticate
// entry is far ahead of the node. It waits for that entry until its
// first deadline, then fails as etcd's does; a wait never renews its
// deadline. A stopped node answers its waits.
func TestTokenWaitTimesOut(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) { cfg.RequestTicks = 10 }).capable()
	s.ok(1, &pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", HashedPassword: hashOf("pw")}})
	s.ok(2, &pb.InternalRaftRequest{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "root", Role: "root"}})
	s.ok(3, &pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}})
	future := Caller{Token: "prefixroot.1000000"}
	s.n.Submit(Request{ID: 10, Op: &pb.RangeRequest{Key: []byte("a")}, Caller: future})
	var got error
	for i := 0; i < 30 && got == nil; i++ {
		s.n.Tick()
		for _, r := range s.run() {
			if r.ID == 10 {
				got = r.Err
			}
		}
	}
	if !errors.Is(got, rpctypes.ErrGRPCInvalidAuthToken) {
		t.Fatalf("a wait for a future token: %v, want an invalid token", got)
	}

	s.n.Submit(Request{ID: 11, Op: &pb.RangeRequest{Key: []byte("a")}, Caller: future})
	s.run()
	s.n.stop()
	if r := s.n.out.Replies; len(r) != 1 || r[0].ID != 11 || !errors.Is(r[0].Err, rpctypes.ErrGRPCStopped) {
		t.Fatalf("replies of a stopped node: %v, want request 11 stopped", r)
	}
}

// TestBigRoleInCheckpoint reads back a checkpoint header with a role far
// above the bound of a name or URL: a role grows with every permission
// granted, and its checkpoint must stay readable.
func TestBigRoleInCheckpoint(t *testing.T) {
	a := newAuthState()
	ro := &authpb.Role{Name: []byte("big")}
	for i := range 5000 {
		ro.KeyPermission = append(ro.KeyPermission, &authpb.Permission{PermType: authpb.Permission_READ, Key: fmt.Appendf(nil, "/registry/key-%05d", i)})
	}
	a.roles["big"] = ro
	b, err := (&ckptHeader{auth: a}).encode()
	if err != nil {
		t.Fatal(err)
	}
	h, err := readHeader(bufio.NewReader(bytes.NewReader(b)))
	if err != nil {
		t.Fatal(err)
	}
	if got := h.auth.roles["big"]; !proto.Equal(got, ro) {
		t.Fatalf("role after the round trip has %d permissions, want %d", len(got.GetKeyPermission()), len(ro.KeyPermission))
	}
	// A damaged length past the bytes that follow fails without a huge
	// buffer.
	if _, err := readHeader(bufio.NewReader(bytes.NewReader(b[:len(b)/2]))); err == nil {
		t.Fatal("a cut header read without an error")
	}
}
