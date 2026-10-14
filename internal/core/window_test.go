package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// memCluster runs three cores in memory: messages and disk writes are
// queues that a test can hold back. Nothing is written anywhere.
type memCluster struct {
	t     *testing.T
	nodes []*Node
	jobs  [][]PersistJob
	msgs  []*raftpb.Message
	// holdAcks keeps the followers' append responses from the leader, so
	// nothing commits; holdDisk keeps a node's disk writes from finishing.
	holdAcks bool
	holdDisk [3]bool
	replies  int
}

func newMemCluster(t *testing.T, change func(*Config)) *memCluster {
	c := &memCluster{t: t, jobs: make([][]PersistJob, 3)}
	for id := uint64(1); id <= 3; id++ {
		cfg := DefaultConfig(id, []uint64{1, 2, 3})
		change(&cfg)
		n, err := New(cfg, &wal.Recovered{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
	}
	for range 100 {
		c.nodes[0].Tick()
		c.step()
		if c.nodes[0].isLeader {
			return c
		}
	}
	t.Fatal("node 1 did not become leader")
	return nil
}

// step processes every node once, delivers the messages that are not
// held, and finishes the disk writes that are not held.
func (c *memCluster) step() {
	for i, n := range c.nodes {
		eff, err := n.Process()
		if err != nil {
			c.t.Fatalf("node %d: %v", i+1, err)
		}
		c.jobs[i] = append(c.jobs[i], eff.Persist...)
		c.msgs = append(c.msgs, eff.Send...)
		for _, r := range eff.Replies {
			if r.Err != nil {
				c.t.Fatalf("node %d answered %d with %v", i+1, r.ID, r.Err)
			}
			c.replies++
		}
	}
	msgs := c.msgs
	c.msgs = nil
	for _, m := range msgs {
		if c.holdAcks && m.GetType() == raftpb.MsgAppResp {
			c.msgs = append(c.msgs, m)
			continue
		}
		if err := c.nodes[m.GetTo()-1].Step(m); err != nil {
			c.t.Fatalf("step %s: %v", m.GetType(), err)
		}
	}
	for i, n := range c.nodes {
		if c.holdDisk[i] {
			continue
		}
		for _, j := range c.jobs[i] {
			n.Persisted(j)
		}
		c.jobs[i] = nil
	}
}

// TestWindowBound writes 1 GiB of log through three nodes and checks
// after every step that no node holds more entry bytes in memory than
// its window maximum plus its budget for entries not applied yet, plus
// one entry: while all runs well, while the leader gets no
// acknowledgements and nothing commits, and while the leader's or a
// follower's disk holds back its writes and so what it applies.
func TestWindowBound(t *testing.T) {
	const (
		window, windowMax, unapplied = 8 << 20, 16 << 20, 16 << 20
		valueBytes                   = 256 << 10
		total                        = 1 << 30
	)
	c := newMemCluster(t, func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes, cfg.UnappliedBytes = window, windowMax, unapplied
		cfg.MaxBatchBytes = 1 << 20
		cfg.CheckpointBytes = 1 << 40
	})
	leader := c.nodes[0]
	value := make([]byte, valueBytes)
	limit := int64(windowMax + unapplied + leader.cfg.MaxBatchBytes + batchFraming)
	var sent, peak int64
	id := uint64(0)
	for step := 0; c.replies*valueBytes < total; step++ {
		// Hold something back for 150 steps out of every 400.
		phase := step % 400
		c.holdAcks = phase >= 50 && phase < 200 && step/400%3 == 0
		c.holdDisk[0] = phase >= 50 && phase < 200 && step/400%3 == 1
		c.holdDisk[1] = phase >= 50 && phase < 200 && step/400%3 == 2
		for int(id)-c.replies < 256 {
			id++
			leader.Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id%1000), Value: value}})
			sent += valueBytes
		}
		c.step()
		for i, n := range c.nodes {
			b := n.EntryBytes()
			peak = max(peak, b)
			if b > limit {
				t.Fatalf("step %d: node %d holds %d entry bytes in memory, more than %d", step, i+1, b, limit)
			}
		}
		if step > 200000 {
			t.Fatalf("only %d of %d bytes applied after %d steps", c.replies*valueBytes, total, step)
		}
	}
	t.Logf("%d MiB written; at most %d MiB of entries in memory on a node, against a bound of %d MiB", c.replies*valueBytes>>20, peak>>20, limit>>20)
}

// TestRestartFromCheckpoint writes and compacts with a tiny window and
// frequent checkpoints, so that old segments go, then restarts the node
// from its disk: it loads the newest checkpoint, applies the log after it,
// and holds the same state and every value, read from disk.
func TestRestartFromCheckpoint(t *testing.T) {
	tiny := func(cfg *Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 512, 1024
		cfg.CheckpointBytes, cfg.CheckpointRatio, cfg.CheckpointChunk = 2048, 2, 64
	}
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, tiny)
	id := uint64(0)
	for round := range 30 {
		var rev int64
		for k := range 10 {
			id++
			r := s.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: fmt.Appendf(nil, "value %d of round %d", k, round)})
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			rev = s.n.kv.Rev()
		}
		id++
		if r := s.do(id, &pb.CompactionRequest{Revision: rev}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	root := s.n.DurableCheckpoint()
	if root.Index == 0 {
		t.Fatal("no checkpoint became durable")
	}
	if names := disk.DurableNames(); names[0] == "0000000000000001.wal" {
		t.Fatalf("no segment was removed: %v", names)
	}
	want, applied := s.n.Hash(), s.n.Applied()
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	s = startSingleWith(t, disk, 2, tiny)
	if s.n.Applied() < applied || s.n.DurableCheckpoint() != root {
		t.Fatalf("restarted at %d with root %+v, want %d and %+v", s.n.Applied(), s.n.DurableCheckpoint(), applied, root)
	}
	if s.n.Applied() == applied && s.n.Hash() != want {
		t.Fatal("the restarted node's state differs")
	}
	id++
	r := s.do(id, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	kvs := r.Resp.(*pb.RangeResponse).Kvs
	for k, kv := range kvs {
		if string(kv.Value) != fmt.Sprintf("value %d of round 29", k) {
			t.Fatalf("k%d is %q after the restart", k, kv.Value)
		}
	}
	if len(kvs) != 10 {
		t.Fatalf("%d keys after the restart", len(kvs))
	}
}
