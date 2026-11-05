package core

import (
	"fmt"
	"github.com/djosh34/tigerstore/internal/floor"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"testing"
)

var floorSink proto.Message
var floorPutSink Put
var floorDataSink []byte

func BenchmarkFloorDecode(b *testing.B) {
	for _, txn := range []bool{false, true} {
		name := "put"
		if txn {
			name = "txn"
		}
		wire := floor.Wire(txn)
		b.Run(name+"/protobuf", func(b *testing.B) {
			for b.Loop() {
				var m proto.Message = &pb.PutRequest{}
				if txn {
					m = &pb.TxnRequest{}
				}
				if err := proto.Unmarshal(wire, m); err != nil {
					b.Fatal(err)
				}
				floorSink = m
			}
		})
		b.Run(name+"/real", func(b *testing.B) {
			for b.Loop() {
				if txn {
					m := &pb.TxnRequest{}
					if err := DecodeAlias(wire, m); err != nil {
						b.Fatal(err)
					}
					floorSink = m
				} else {
					p, err := ParsePut(wire)
					if err != nil {
						b.Fatal(err)
					}
					floorPutSink = p
				}
			}
		})
	}
}
func BenchmarkFloorEncode(b *testing.B) {
	for _, txn := range []bool{false, true} {
		for _, batch := range []int{64, 256, 1536} {
			name := "put"
			if txn {
				name = "txn"
			}
			wire := floor.Wire(txn)
			ps := make([]*pending, batch)
			for i := range ps {
				r := Request{ID: uint64(1000000 + i)}
				if txn {
					r.Op = &pb.TxnRequest{}
					r.Wire = wire
				} else {
					r.Put, _ = ParsePut(wire)
				}
				ps[i] = &pending{req: r}
			}
			b.Run(fmt.Sprintf("%s/b%d", name, batch), func(b *testing.B) {
				for b.Loop() {
					data, err := encodeBatch(3, 0x123456789abcdef0, 1, ps, batch*(len(wire)+batchFraming))
					if err != nil {
						b.Fatal(err)
					}
					floorDataSink = data
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/item")
			})
		}
	}
}

// Raw Raft only: leader Propose, Ready/Advance, local append and two
// MsgAppResp; no serialization, TLS, disk wait or application. The storage
// slice is compacted outside each timed chunk to bound benchmark memory.
func BenchmarkFloorRaft(b *testing.B) {
	for _, batch := range []int{64, 256, 1536} {
		b.Run(fmt.Sprintf("b%d", batch), func(b *testing.B) {
			st := raft.NewMemoryStorage()
			_ = st.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}})
			_ = st.SetHardState(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
			rn, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: st, MaxSizePerMsg: 4 << 20, MaxInflightMsgs: 256, Logger: quietLogger()})
			if err != nil {
				b.Fatal(err)
			}
			drain := func() {
				for rn.HasReady() {
					rd := rn.Ready()
					_ = st.Append(rd.Entries)
					rn.Advance(rd)
				}
			}
			_ = rn.Campaign()
			drain()
			_ = rn.Step(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2))})
			drain()
			ack := func() {
				last, _ := st.LastIndex()
				for _, id := range []uint64{2, 3} {
					_ = rn.Step(&raftpb.Message{Type: raftpb.MsgAppResp.Enum(), From: new(id), To: new(uint64(1)), Term: new(uint64(2)), Index: new(last)})
				}
				drain()
			}
			ack()
			data := floor.Batch(false, batch)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rn.Propose(data); err != nil {
					b.Fatal(err)
				}
				drain()
				ack()
				if i%128 == 127 {
					b.StopTimer()
					last, _ := st.LastIndex()
					_ = st.Compact(last - 1)
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/item")
		})
	}
}
