package core

import (
	"bufio"
	"bytes"
	"fmt"
	"runtime"
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
	// The copy finishes, but the next cut waits until it is written too.
	s.holdCopy = false
	s.run()
	put(1)
	if s.n.last.index != first || s.n.copy != nil {
		t.Fatalf("a checkpoint was cut before the one at %d was written", first)
	}
	s.holdCkpt = false
	s.writeCheckpoint(s.heldCkpt)
	s.heldCkpt = nil
	put(1)
	if s.n.last.index <= first {
		t.Fatalf("no checkpoint was cut after the one at %d was written", first)
	}
}

// TestCheckpointHeaderInPieces cuts a checkpoint of a state with 20,000
// leases: its header, about 8 bytes a lease, goes out in parts of
// CheckpointChunk, as the keys do (#434).
func TestCheckpointHeaderInPieces(t *testing.T) {
	const chunk = 4 << 10
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.CheckpointChunk = chunk })
	for i := range 20_000 {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.LeaseGrantRequest{ID: int64(i + 1), TTL: 3600}})
		if i%500 == 499 {
			s.run()
		}
	}
	s.run()
	s.holdCkpt = true
	if r := s.do(30_000, &pb.CompactionRequest{Revision: 1}); r.Err != nil {
		t.Fatal(r.Err)
	}
	if len(s.heldCkpt) < 20_000*6/chunk {
		t.Fatalf("a checkpoint of 20,000 leases came in %d parts", len(s.heldCkpt))
	}
	for i, c := range s.heldCkpt {
		if len(c.Data) > chunk+64 {
			t.Fatalf("part %d of a checkpoint with a chunk of %d bytes holds %d", i, chunk, len(c.Data))
		}
	}
}

// writes keeps the size of the largest write, and the bytes if keep.
type writes struct {
	bytes.Buffer
	most int
	keep bool
}

func (w *writes) Write(p []byte) (int, error) {
	w.most = max(w.most, len(p))
	if !w.keep {
		return len(p), nil
	}
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
	members := []*pb.Member{{ID: 1, Name: "n1", PeerURLs: []string{"https://n1:2380"}}}
	var sink writes
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if _, err := s.n.RestoredCheckpoint(members, &sink); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if limit := 2 * DefaultConfig(1, nil).CheckpointChunk; sink.most > limit {
		t.Fatalf("a restore of 50,000 keys wrote %d bytes of its checkpoint at once, more than %d", sink.most, limit)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 1<<20 {
		t.Fatalf("a restore of 50,000 keys into a sink that keeps nothing allocated %d bytes", alloc)
	}
	w := writes{keep: true}
	root, err := s.n.RestoredCheckpoint(members, &w)
	if err != nil {
		t.Fatal(err)
	}
	st, _, err := readCheckpoint(s.n.cfg, bufio.NewReader(&w.Buffer), root, s.n.values)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.kv.Counts().Keys; got != 50_000 {
		t.Fatalf("the restored checkpoint holds %d keys, want 50000", got)
	}
}
