package core

import (
	"bytes"
	"testing"

	"github.com/djosh34/tigerstore/internal/wal"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCompressionWaitsForEveryVoter(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) {
		cfg.MaxVersion = 4
		if cfg.ID == 3 { cfg.MaxVersion = 3 }
	})
	for _, want := range []uint32{3, 4} {
		if want == 4 { c.nodes[2].cfg.MaxVersion = 4 }
		for range 100 {
			for _, n := range c.nodes { n.Tick() }
			c.step()
		}
		value := bytes.Repeat([]byte("metadata managedFields namespace "), 100)
		c.nodes[0].Submit(Request{ID: uint64(want), Op: &pb.PutRequest{Key: []byte("key"), Value: value}})
		for range 10 { c.step() }
		for _, n := range c.nodes {
			if n.ActiveVersion() != want { t.Fatalf("version %d, want %d", n.ActiveVersion(), want) }
		}
		fs := formats(c.nodes[0])
		if len(fs) == 0 || fs[len(fs)-1] != byte(want) { t.Fatalf("formats %v, want %d", fs, want) }
		data, err := encodeBatch(want, 1, 1, []*pending{{req: Request{ID: 1, Op: &pb.PutRequest{Key: []byte("key"), Value: value}}}}, 0)
		if err != nil { t.Fatal(err) }
		e := &raftpb.Entry{Data: data}
		stored, err := wal.CompressEntries([]*raftpb.Entry{e})
		if err != nil { t.Fatal(err) }
		if (len(stored[0].Data) < len(data)) != (want == 4) { t.Fatalf("version %d: raw %d stored %d", want, len(data), len(stored[0].Data)) }
	}
}
