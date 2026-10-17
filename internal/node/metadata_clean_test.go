package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// TestMetadataAfterCleaning keeps node 3 offline while membership, auth
// and alarms change. The real cleaner must copy a cold value into a pack
// and remove EVERY entry through the metadata changes before either a
// restart or snapshot catch-up can pass. The follower's old log cannot
// contain those changes, and the donors no longer have their entries.
func TestMetadataAfterCleaning(t *testing.T) {
	c := &behind{
		t:     t,
		net:   &memNet{queues: map[uint64]chan *raftpb.Message{}, nodes: map[uint64]*Node{}},
		dirs:  map[uint64]string{1: t.TempDir(), 2: t.TempDir(), 3: t.TempDir()},
		nodes: map[uint64]*Node{}, ran: map[uint64]chan error{}, stops: map[uint64]context.CancelFunc{},
		tune: func(_ uint64, cfg *Config) { cfg.Core.StrictReconfigCheck = false },
	}
	t.Cleanup(c.close)
	for id := uint64(1); id <= 3; id++ {
		if _, err := c.start(id); err != nil {
			t.Fatal(err)
		}
	}
	call := func(n *Node, op proto.Message) proto.Message {
		t.Helper()
		for deadline := time.Now().Add(20 * time.Second); ; {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := n.DoAs(ctx, core.RootCaller(), op)
			cancel()
			if err == nil {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatalf("%T: %v", op, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	n := c.nodes[1]
	cold := bytes.Repeat([]byte("cold"), 2048)
	call(n, &pb.PutRequest{Key: []byte("cold"), Value: cold})
	// Do not stop a voter before its format announcement commits.
	for id := uint64(1); id <= 3; id++ {
		for deadline := time.Now().Add(10 * time.Second); ; {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			m, err := c.nodes[id].Metrics(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if m.ClusterVersion == core.MaxVersion {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d stayed at cluster version %d", id, m.ClusterVersion)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := c.stop(3); err != nil {
		t.Fatal(err)
	}
	add := call(n, &pb.MemberAddRequest{IsLearner: true, PeerURLs: []string{"https://old:2380"}}).(*pb.MemberAddResponse)
	removed := add.Member.ID
	call(n, &pb.MemberRemoveRequest{ID: removed})
	add = call(n, &pb.MemberAddRequest{IsLearner: true, PeerURLs: []string{"https://learner:2380"}}).(*pb.MemberAddResponse)
	learner := add.Member.ID
	call(n, &pb.MemberUpdateRequest{ID: learner, PeerURLs: []string{"https://updated:2380"}})
	perm := &authpb.Permission{PermType: authpb.Permission_READWRITE, Key: []byte("a"), RangeEnd: []byte("d")}
	for _, op := range []*pb.InternalRaftRequest{
		{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", Options: &authpb.UserAddOptions{NoPassword: true}}},
		{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "root", Role: "root"}},
		{AuthUserAdd: &pb.AuthUserAddRequest{Name: "alice", Options: &authpb.UserAddOptions{NoPassword: true}}},
		{AuthRoleAdd: &pb.AuthRoleAddRequest{Name: "rw"}},
		{AuthRoleGrantPermission: &pb.AuthRoleGrantPermissionRequest{Name: "rw", Perm: perm}},
		{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "alice", Role: "rw"}},
		{AuthUserAdd: &pb.AuthUserAddRequest{Name: "padding", Options: &authpb.UserAddOptions{NoPassword: true}}},
		{AuthEnable: &pb.AuthEnableRequest{}},
	} {
		call(n, op)
	}
	for _, typ := range []pb.AlarmType{pb.AlarmType_NOSPACE, pb.AlarmType_CORRUPT} {
		call(n, &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: typ, MemberID: 99})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := n.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	through := m.Applied
	// Auth writes are allowed under both alarms. Churn only the padding
	// user's password; alice, her role and every tested metadata field
	// remain untouched after 'through'. This also forces new checkpoints.
	for round := range 100 {
		call(n, &pb.InternalRaftRequest{AuthUserChangePassword: &pb.AuthUserChangePasswordRequest{
			Name: "padding", HashedPassword: fmt.Sprintf("%d-%s", round, strings.Repeat("x", 16<<10)),
		}})
	}
	for id := uint64(1); id <= 2; id++ {
		n := c.nodes[id]
		for deadline := time.Now().Add(20 * time.Second); ; {
			dctx, done := context.WithTimeout(context.Background(), 5*time.Second)
			err := n.Defragment(dctx)
			done()
			if err != nil {
				t.Fatal(err)
			}
			n.log.mu.Lock()
			gone := true
			for index := uint64(1); index <= through; index++ {
				_, err := n.log.w.Entry(index)
				if err == nil {
					gone = false
					break
				}
				if !errors.Is(err, wal.ErrNotOnDisk) {
					n.log.mu.Unlock()
					t.Fatalf("node %d entry %d: %v", id, index, err)
				}
			}
			n.log.mu.Unlock()
			if gone {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d still has metadata entries through %d", id, through)
			}
			time.Sleep(50 * time.Millisecond)
		}
		mctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := n.Metrics(mctx)
		done()
		if err != nil {
			t.Fatal(err)
		}
		if m.CleanedBytes == 0 {
			t.Fatalf("node %d made no value pack", id)
		}
		t.Logf("node %d removed entries 1..%d; cleaned %d bytes", id, through, m.CleanedBytes)
	}
	wantMembers := []*pb.Member{{ID: 1}, {ID: 2}, {ID: 3}, {ID: learner, PeerURLs: []string{"https://updated:2380"}, IsLearner: true}}
	wantAlarms := []*pb.AlarmMember{{MemberID: 99, Alarm: pb.AlarmType_NOSPACE}, {MemberID: 99, Alarm: pb.AlarmType_CORRUPT}}
	wantAuthRevision := call(n, &pb.InternalRaftRequest{AuthStatus: &pb.AuthStatusRequest{}}).(*pb.AuthStatusResponse).AuthRevision
	check := func(n *Node) {
		t.Helper()
		ms := call(n, &pb.MemberListRequest{Linearizable: true}).(*pb.MemberListResponse).Members
		if !slices.EqualFunc(ms, wantMembers, func(a, b *pb.Member) bool { return proto.Equal(a, b) }) {
			t.Fatalf("members: %v, want %v", ms, wantMembers)
		}
		u := call(n, &pb.InternalRaftRequest{AuthUserGet: &pb.AuthUserGetRequest{Name: "alice"}}).(*pb.AuthUserGetResponse)
		if !slices.Equal(u.Roles, []string{"rw"}) {
			t.Fatalf("alice's roles: %v", u.Roles)
		}
		r := call(n, &pb.InternalRaftRequest{AuthRoleGet: &pb.AuthRoleGetRequest{Role: "rw"}}).(*pb.AuthRoleGetResponse)
		if len(r.Perm) != 1 || !proto.Equal(r.Perm[0], perm) {
			t.Fatalf("permissions: %v", r.Perm)
		}
		st := call(n, &pb.InternalRaftRequest{AuthStatus: &pb.AuthStatusRequest{}}).(*pb.AuthStatusResponse)
		if !st.Enabled || st.AuthRevision != wantAuthRevision {
			t.Fatalf("auth status: %v, want enabled at revision %d", st, wantAuthRevision)
		}
		users := call(n, &pb.InternalRaftRequest{AuthUserList: &pb.AuthUserListRequest{}}).(*pb.AuthUserListResponse).Users
		if !slices.Equal(users, []string{"alice", "padding", "root"}) {
			t.Fatalf("users: %v", users)
		}
		a := call(n, &pb.AlarmRequest{}).(*pb.AlarmResponse).Alarms
		if !slices.EqualFunc(a, wantAlarms, func(a, b *pb.AlarmMember) bool { return proto.Equal(a, b) }) {
			t.Fatalf("alarms: %v, want %v", a, wantAlarms)
		}
		rangeResp := call(n, &pb.RangeRequest{Key: []byte("cold")}).(*pb.RangeResponse)
		if len(rangeResp.Kvs) != 1 || !bytes.Equal(rangeResp.Kvs[0].Value, cold) {
			t.Fatal("cold value lost")
		}
		actx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := n.DoAs(actx, core.NewCaller("alice"), &pb.RangeRequest{Key: []byte("cold")}); err != nil {
			t.Fatalf("alice's allowed read: %v", err)
		}
		if _, err := n.DoAs(actx, core.NewCaller("alice"), &pb.RangeRequest{Key: []byte("z")}); !errors.Is(err, rpctypes.ErrGRPCPermissionDenied) {
			t.Fatalf("alice's forbidden read: %v", err)
		}
		_, err := n.DoAs(actx, core.RootCaller(), &pb.MemberRemoveRequest{ID: removed})
		if !errors.Is(err, rpctypes.ErrGRPCMemberNotFound) {
			t.Fatalf("removed member returned: %v", err)
		}
	}
	// Restart both donors, with no running process retaining the metadata.
	for id := uint64(1); id <= 2; id++ {
		if err := c.stop(id); err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 2; id++ {
		if _, err := c.start(id); err != nil {
			t.Fatal(err)
		}
	}
	check(c.nodes[1])
	check(c.nodes[2])
	c.net.mu.Lock()
	before := c.net.fetches
	c.net.mu.Unlock()
	follower, err := c.start(3)
	if err != nil {
		t.Fatal(err)
	}
	check(follower)
	c.net.mu.Lock()
	fetches := c.net.fetches - before
	c.net.mu.Unlock()
	if fetches == 0 {
		t.Fatal("follower caught up without a snapshot fetch")
	}
	t.Logf("follower fetched %d snapshots after cleaning", fetches)
	if err := c.stop(3); err != nil {
		t.Fatal(err)
	}
	follower, err = c.start(3)
	if err != nil {
		t.Fatal(err)
	}
	check(follower)
}
