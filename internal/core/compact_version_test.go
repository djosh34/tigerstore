package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// Disk pressure can refuse writes before the cluster supports alarm
// entries. Compaction must still let a checkpoint release old values.
// The checkpoint decision cannot depend on one node's local disk usage.
func TestCompactionBeforeAlarmVersion(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		for _, full := range []bool{false, true} {
			t.Run(fmt.Sprintf("version=%d/full=%v", version, full), func(t *testing.T) {
				s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) {
					cfg.MaxVersion = version
					cfg.CheckpointBytes = 1 << 30
				})
				for range 30 {
					s.n.Tick()
					s.run()
				}
				s.ok(1, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
				if full {
					s.n.SetDisk(100, 100)
				}
				del, ok := s.ok(2, &pb.DeleteRangeRequest{Key: []byte("k")}).(*pb.DeleteRangeResponse)
				if !ok {
					t.Fatal("not a delete response")
				}
				if s.n.DurableCheckpoint().Index != 0 || s.n.state.noSpace || s.n.capable() {
					t.Fatal("test needs no checkpoint and no supported alarm entries")
				}
				s.ok(3, &pb.CompactionRequest{Revision: del.Header.Revision})
				if s.n.DurableCheckpoint().Index == 0 {
					t.Fatal("compaction before alarm support did not cut a checkpoint")
				}
			})
		}
	}
}
