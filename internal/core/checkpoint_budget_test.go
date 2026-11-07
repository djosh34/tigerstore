package core

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestCutDuringACopyWaits cuts a checkpoint and holds its copy while
// enough writes for the next cut apply. No batch of 100 writes may make
// the node hand out more than a few parts at once, and the next cut comes
// once the copy is done (#434).
func TestCutDuringACopyWaits(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.CheckpointBytes, c.CheckpointRatio = 64<<10, 1 })
	s.holdCopy, s.holdCkpt = true, true
	id := uint64(0)
	put := func(n int) {
		for range n {
			id++
			s.n.Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "key/%06d", id), Value: make([]byte, 100)}})
		}
		s.run()
	}
	// Cut one checkpoint and hold its copy, then apply enough for the
	// next.
	for range 40 {
		put(500)
	}
	first := s.n.copy.index
	most := 0
	for range 20 {
		before := len(s.heldCkpt)
		put(100)
		b := 0
		for _, c := range s.heldCkpt[before:] {
			b += len(c.Data)
		}
		most = max(most, b)
	}
	if most > 4*DefaultConfig(1, nil).CheckpointChunk {
		t.Fatalf("100 writes made the node hand out %d bytes of checkpoint at once", most)
	}
	if !s.n.CutWaits() || s.n.copy.index != first {
		t.Fatalf("the cut due during the copy of %d did not wait for it", first)
	}
	s.holdCopy = false
	s.run()
	put(1)
	if s.n.last.index <= first {
		t.Fatalf("no checkpoint was cut after the copy of %d finished", first)
	}
}

// writes records the size of each write.
type writes struct {
	bytes.Buffer
	most int
}

func (w *writes) Write(p []byte) (int, error) {
	w.most = max(w.most, len(p))
	return w.Buffer.Write(p)
}

// TestRestoredCheckpointIsStreamed writes the checkpoint of a restore of
// 50,000 keys: it goes out in parts of about CheckpointChunk bytes, never
// whole, and loads (#436).
func TestRestoredCheckpointIsStreamed(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	for i := range 50_000 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "/registry/pods/default/pod-%06d", i), Value: []byte("v")}})
		if i%500 == 499 {
			s.run()
		}
	}
	s.run()
	var w writes
	root, err := s.n.RestoredCheckpoint([]*pb.Member{{ID: 1, Name: "n1", PeerURLs: []string{"https://n1:2380"}}}, &w)
	if err != nil {
		t.Fatal(err)
	}
	if limit := 2 * DefaultConfig(1, nil).CheckpointChunk; w.most > limit {
		t.Fatalf("a restore of 50,000 keys wrote %d bytes of its checkpoint at once, more than %d", w.most, limit)
	}
	st, _, err := readCheckpoint(s.n.cfg, bufio.NewReader(&w.Buffer), root, s.n.values)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.kv.Counts().Keys; got != 50_000 {
		t.Fatalf("the restored checkpoint holds %d keys, want 50000", got)
	}
}
