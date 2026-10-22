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

// TestCleanerHeadroom writes 80 cold values of 1 KiB at the front of the
// log, then overwrites 12 hot ones and compacts, 20 times, with no faults,
// on a 192 KiB budget of 16 KiB segments. The live values, 92 KiB, are
// near half the budget, so the front stays more than half live until the
// files near the budget. The cleaner must still keep them below it: while
// it took the oldest segments however live only at the budget, a put
// got NOSPACE after round 6, with nothing cleaned (#385).
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
			t.Fatal("no leader after 3 s")
		}
		leader := s.nodes[lead]
		do := func(req proto.Message) proto.Message {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			out, err := leader.proc.node.Do(ctx, req)
			if err != nil {
				t.Fatalf("%T: %v", req, err)
			}
			return out
		}
		value := func(b byte) []byte { return bytes.Repeat([]byte{b}, 1<<10) }
		for i := range 80 {
			do(&pb.PutRequest{Key: fmt.Appendf(nil, "cold/%02d", i), Value: value('c')})
		}
		for round := range 20 {
			var rev int64
			for i := range 12 {
				rev = do(&pb.PutRequest{Key: fmt.Appendf(nil, "hot/%02d", i), Value: value(byte(round))}).(*pb.PutResponse).GetHeader().GetRevision()
			}
			do(&pb.CompactionRequest{Revision: rev})
			time.Sleep(500 * time.Millisecond)
			for _, n := range s.nodes {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				m, err := n.proc.node.Metrics(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("round %d: node %d has %d bytes of files, cleaned %d", round, n.id, m.DiskBytes, m.CleanedBytes)
			}
			if a := do(&pb.AlarmRequest{Action: pb.AlarmRequest_GET}).(*pb.AlarmResponse); len(a.GetAlarms()) > 0 {
				t.Fatalf("round %d: alarms %v with 92 KiB of live values on a 192 KiB budget", round, a.GetAlarms())
			}
		}
	})
}
