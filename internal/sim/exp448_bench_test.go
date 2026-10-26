//go:build !sim

package sim

import (
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/protobuf/proto"
)

type floorValues struct{ b []byte }

func (v *floorValues) Place(b []byte) (mvcc.Ref, error) {
	return mvcc.Ref{Index: 1, Len: uint32(len(b))}, nil
}
func (v *floorValues) Read(r mvcc.Ref, arena *[]byte) ([]byte, bool, error) { return v.b, true, nil }

var floorSink byte

func Benchmark448Apply(b *testing.B) {
	for _, txn := range []bool{false, true} {
		b.Run(fmt.Sprint("txn=", txn), func(b *testing.B) {
			val := make([]byte, 1024)
			put := &pb.PutRequest{Key: []byte("/registry/pods/default/example"), Value: val}
			var op proto.Message = put
			if txn {
				op = &pb.TxnRequest{Compare: []*pb.Compare{{Key: put.Key, Target: pb.Compare_VERSION, Result: pb.Compare_GREATER, TargetUnion: &pb.Compare_Version{Version: -1}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: put}}}}
			}
			var st *core.State
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%1024 == 0 {
					b.StopTimer()
					st = core.NewState(2, &floorValues{val})
					b.StartTimer()
				}
				if _, err := st.Apply(op); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func Benchmark448MemoryCopy(b *testing.B) {
	for _, n := range []int{1024, 4096, 65536} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			src, dst := make([]byte, n), make([]byte, n)
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src[0] = byte(i)
				copy(dst, src)
			}
			floorSink = dst[0]
		})
	}
}
func Benchmark448Simfs(b *testing.B) {
	for _, n := range []int{1024, 4096, 65536} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			d := simfs.New()
			h, err := d.Create("bench")
			if err != nil {
				b.Fatal(err)
			}
			p := make([]byte, n)
			b.SetBytes(int64(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := h.WriteAt(p, 0); err != nil {
					b.Fatal(err)
				}
				if err := h.Sync(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func Benchmark448GRPCCodec(b *testing.B) {
	codec := encoding.GetCodecV2("proto")
	req := &pb.PutRequest{Key: []byte("/registry/pods/default/example"), Value: make([]byte, 1024)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := codec.Marshal(req)
		if err != nil {
			b.Fatal(err)
		}
		var out pb.PutRequest
		if err := codec.Unmarshal(data, &out); err != nil {
			b.Fatal(err)
		}
		data.Free()
	}
}

type floorCluster struct {
	nodes  []*raft.RawNode
	stores []*raft.MemoryStorage
}

func newFloorCluster(b *testing.B) *floorCluster {
	c := &floorCluster{}
	for id := uint64(1); id <= 3; id++ {
		s := raft.NewMemoryStorage()
		s.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}})
		s.SetHardState(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
		n, err := raft.NewRawNode(&raft.Config{ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: s, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, Logger: &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}})
		if err != nil {
			b.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		c.stores = append(c.stores, s)
	}
	c.nodes[0].Campaign()
	c.pump(b)
	return c
}
func (c *floorCluster) pump(b *testing.B) {
	for turns := 0; turns < 100; turns++ {
		var msgs []*raftpb.Message
		any := false
		for i, n := range c.nodes {
			if !n.HasReady() {
				continue
			}
			any = true
			r := n.Ready()
			s := c.stores[i]
			if !raft.IsEmptyHardState(r.HardState) {
				s.SetHardState(r.HardState)
			}
			s.Append(r.Entries)
			msgs = append(msgs, r.Messages...)
			n.Advance(r)
		}
		for _, m := range msgs {
			if err := c.nodes[m.GetTo()-1].Step(m); err != nil {
				b.Fatal(err)
			}
		}
		if !any {
			return
		}
	}
	b.Fatal("raft did not drain")
}
func Benchmark448RaftProposal3(b *testing.B) {
	var c *floorCluster
	p := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			b.StopTimer()
			c = newFloorCluster(b)
			b.StartTimer()
		}
		if err := c.nodes[0].Propose(p); err != nil {
			b.Fatal(err)
		}
		c.pump(b)
	}
}
