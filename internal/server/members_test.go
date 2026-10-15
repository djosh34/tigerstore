package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/wal"
)

// TestLearnerJoinsAndLeaves adds node 4 as a learner, starts it as a node
// that joins the running cluster, and checks etcd v3.7.2's behavior: the
// learner catches up and serves serializable reads and Status but
// refuses linearizable reads, watches and member lists; MemberPromote,
// sent to a follower, reaches the leader once the learner caught up; the
// new voter takes writes and publishes its name; MemberRemove takes it
// out, and it stops. With frequent checkpoints, the first segment is gone
// before the learner starts, so it catches up through a snapshot with a
// checkpoint.
func TestLearnerJoinsAndLeaves(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%v", snapshot), func(t *testing.T) { learnerJoinsAndLeaves(t, snapshot) })
	}
}

func learnerJoinsAndLeaves(t *testing.T, snapshot bool) {
	var c *cluster
	if snapshot {
		c = newClusterWith(t, tinyCheckpoints)
	} else {
		c = newCluster(t)
	}
	cert, _, _, err := c.ca.Issue("client", 88)
	if err != nil {
		t.Fatal(err)
	}
	put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	if snapshot {
		for round := range 20 {
			var rev int64
			for i := range 20 {
				rev = retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
					return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: fmt.Appendf(nil, "v%d %0200d", round, 0)})
				}).Header.Revision
			}
			// a keeps its value, from a newer entry.
			put = retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
				return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
			})
			retry(t, func(ctx context.Context) (*pb.CompactionResponse, error) {
				return c.client(1, cert).Compact(ctx, &pb.CompactionRequest{Revision: rev})
			})
		}
		for id := uint64(1); id <= 3; id++ {
			if _, err := os.Stat(filepath.Join(c.dirs[id], "wal", wal.SegmentName(1))); err == nil {
				t.Fatalf("node %d still has its first segment", id)
			}
		}
	}
	cl := pb.NewClusterClient(c.dial(2, cert))
	if _, err := cl.MemberAdd(context.Background(), &pb.MemberAddRequest{PeerURLs: []string{"not a url"}}); !errors.Is(err, rpctypes.ErrGRPCMemberBadURLs) {
		t.Fatalf("bad URLs: %v", err)
	}
	c.peerLns[4] = listenPeer(t, "")
	c.peerAddrs[4] = c.peerLns[4].Addr().String()
	add := retry(t, func(ctx context.Context) (*pb.MemberAddResponse, error) {
		return cl.MemberAdd(ctx, &pb.MemberAddRequest{PeerURLs: []string{"https://" + c.peerAddrs[4]}, IsLearner: true})
	})
	if add.Member.ID != 4 || !add.Member.IsLearner || len(add.Members) != 4 {
		t.Fatalf("add: %v", add)
	}
	c.joinNode(4)
	c.removed[4] = true

	m4 := pb.NewMaintenanceClient(c.dial(4, cert))
	kv4 := c.client(4, cert)
	retry(t, func(ctx context.Context) (*pb.StatusResponse, error) {
		st, err := m4.Status(ctx, &pb.StatusRequest{})
		if err == nil && (!st.IsLearner || st.RaftAppliedIndex < 4) {
			err = fmt.Errorf("status %v", st)
		}
		return st, err
	})
	got := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
		r, err := kv4.Range(ctx, &pb.RangeRequest{Key: []byte("a"), Serializable: true})
		if err == nil && (len(r.Kvs) != 1 || r.Header.Revision < put.Header.Revision) {
			err = fmt.Errorf("not caught up: %v", r)
		}
		return r, err
	})
	if string(got.Kvs[0].Value) != "1" {
		t.Fatalf("learner read %v", got)
	}
	if _, err := kv4.Range(context.Background(), &pb.RangeRequest{Key: []byte("a")}); !errors.Is(err, rpctypes.ErrGRPCNotSupportedForLearner) {
		t.Fatalf("linearizable read on a learner: %v", err)
	}
	if _, err := pb.NewClusterClient(c.dial(4, cert)).MemberList(context.Background(), &pb.MemberListRequest{}); !errors.Is(err, rpctypes.ErrGRPCNotSupportedForLearner) {
		t.Fatalf("member list on a learner: %v", err)
	}
	w, err := pb.NewWatchClient(c.dial(4, cert)).Watch(context.Background())
	if err == nil {
		_, err = w.Recv()
	}
	if !errors.Is(err, rpctypes.ErrGRPCNotSupportedForLearner) {
		t.Fatalf("watch on a learner: %v", err)
	}

	// The promotion goes to a follower, which hands it to the leader.
	leader := c.nodes[1].Leader()
	follower := uint64(1)
	for follower == leader {
		follower++
	}
	prom := retry(t, func(ctx context.Context) (*pb.MemberPromoteResponse, error) {
		return pb.NewClusterClient(c.dial(follower, cert)).MemberPromote(ctx, &pb.MemberPromoteRequest{ID: 4})
	})
	if prom.Header.MemberId != follower || len(prom.Members) != 4 || prom.Members[3].IsLearner {
		t.Fatalf("promote: %v", prom)
	}
	if _, err := cl.MemberPromote(context.Background(), &pb.MemberPromoteRequest{ID: 4}); !errors.Is(err, rpctypes.ErrGRPCMemberNotLearner) {
		t.Fatalf("promoting a voter: %v", err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv4.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")})
	})
	list := retry(t, func(ctx context.Context) (*pb.MemberListResponse, error) {
		r, err := cl.MemberList(ctx, &pb.MemberListRequest{Linearizable: true})
		if err == nil && r.Members[3].Name != "node4" {
			err = fmt.Errorf("node 4 has not published its name: %v", r)
		}
		return r, err
	})
	for i, m := range list.Members {
		if m.ID != uint64(i+1) || m.IsLearner || len(m.PeerURLs) != 1 {
			t.Fatalf("members: %v", list)
		}
	}

	rm, err := cl.MemberRemove(context.Background(), &pb.MemberRemoveRequest{ID: 4})
	if err != nil || len(rm.Members) != 3 {
		t.Fatalf("remove: %v, %v", rm, err)
	}
	<-c.nodes[4].Done()
	if _, err := cl.MemberRemove(context.Background(), &pb.MemberRemoveRequest{ID: 4}); !errors.Is(err, rpctypes.ErrGRPCMemberNotFound) {
		t.Fatalf("removing a removed member: %v", err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(3, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("3")})
	})
	if _, err := w.Recv(); err != nil && !errors.Is(err, io.EOF) && status.Code(err) == codes.OK {
		t.Fatal(err)
	}
}
