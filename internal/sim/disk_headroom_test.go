package sim

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestCleanerHeadroom keeps cold values at the front of the log while
// overwriting a small hot set. Compaction bounds live data; cleaning must
// reclaim the log before its disk budget raises a cluster-wide alarm.
func TestCleanerHeadroom(t *testing.T) {
	if simrun.Drive(t, 385) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(385)
		cfg.Faults = Faults{DiskBudget: 192 << 10}
		cfg.SegmentBytes = 16 << 10
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if t.Failed() {
				explainSim(t, cfg, s)
			}
			s.shutdown()
		}()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		s.mu.Unlock()
		if lead < 0 {
			t.Fatal("no leader")
		}
		leader := s.nodes[lead]
		call := func(req proto.Message) proto.Message {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			out, err := leader.proc.node.Do(ctx, req)
			if err != nil {
				t.Fatalf("%T: %v", req, err)
			}
			return out
		}
		for i := range 80 {
			call(&pb.PutRequest{Key: fmt.Appendf(nil, "cold/%03d", i), Value: bytes.Repeat([]byte("c"), 1024)})
		}
		var rev int64
		for round := range 20 {
			for i := range 12 {
				r := call(&pb.PutRequest{Key: fmt.Appendf(nil, "hot/%03d", i), Value: bytes.Repeat([]byte{byte(round)}, 1024)}).(*pb.PutResponse)
				rev = r.Header.Revision
			}
			call(&pb.CompactionRequest{Revision: rev})
			time.Sleep(500 * time.Millisecond)
			for _, n := range s.nodes {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				m, err := n.proc.node.Metrics(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("round %d node %d disk %d cleaned %d", round, n.id, m.DiskBytes, m.CleanedBytes)
			}
			a := call(&pb.AlarmRequest{Action: pb.AlarmRequest_GET}).(*pb.AlarmResponse)
			if len(a.Alarms) > 0 {
				t.Fatalf("round %d: alarms %v with only 92 KiB of live values", round, a.Alarms)
			}
		}
	})
}
