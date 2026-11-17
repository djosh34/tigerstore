package core

import (
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// A reader of the log leaves out the writes the apply refused for the
// identity they carried, as the apply does: a write proposed before auth
// was turned on and applied after carries none (etcd's ErrUserEmpty), and
// one taken at an older auth revision is refused too. The writes it keeps
// are those the apply takes (#698: TestReleaseUpgrade's old cluster).
func TestLogAuthLeavesOutWritesTheApplyRefused(t *testing.T) {
	put := func(key string) *pb.PutRequest { return &pb.PutRequest{Key: []byte(key), Value: []byte("v")} }
	auth := func(r *pb.InternalRaftRequest) *pending { return &pending{req: Request{Op: r}} }
	write := func(key string, who *authInfo) *pending { return &pending{req: Request{Op: put(key)}, who: who} }
	la := NewLogAuth()
	var kept []string
	apply := func(ps ...*pending) {
		t.Helper()
		data, err := encodeBatch(MaxVersion, 2, 1, ps, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := la.Writes(data, func(op proto.Message) error {
			if p, ok := op.(*pb.PutRequest); ok {
				kept = append(kept, string(p.Key))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(write("before auth", nil))
	apply(
		auth(&pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", Options: &authpb.UserAddOptions{NoPassword: true}}}),
		auth(&pb.InternalRaftRequest{AuthUserGrantRole: &pb.AuthUserGrantRoleRequest{User: "root", Role: "root"}}),
	)
	old := la.st.auth.revision
	apply(auth(&pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "bob", Options: &authpb.UserAddOptions{NoPassword: true}}}))
	rev := la.st.auth.revision
	apply(auth(&pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}}))
	if !la.st.auth.enabled {
		t.Fatal("auth is not on after AuthEnable")
	}
	apply(
		write("no identity", nil),
		write("old revision", &authInfo{user: "root", revision: old}),
		write("bob, no permission", &authInfo{user: "bob", revision: rev}),
		write("root", &authInfo{user: "root", revision: rev}),
	)
	// A request that refuses changes nothing: a second root is not added.
	apply(auth(&pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "root", Options: &authpb.UserAddOptions{NoPassword: true}}}))
	apply(write("root again", &authInfo{user: "root", revision: rev}))
	want := []string{"before auth", "root", "root again"}
	if len(kept) != len(want) {
		t.Fatalf("kept %q, want %q", kept, want)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("kept %q, want %q", kept, want)
		}
	}
}
