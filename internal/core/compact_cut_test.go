package core

import (
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestCompactionCuts checks that every compaction cuts a checkpoint, in
// every cluster version, and that one applied while a copy is under way
// waits for it: the next entry after the copy cuts, with one copy at a
// time.
func TestCompactionCuts(t *testing.T) {
	for _, version := range []uint32{1, MaxVersion} {
		t.Run(fmt.Sprintf("version=%d", version), func(t *testing.T) {
			s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) {
				cfg.MaxVersion = version
				cfg.CheckpointBytes = 1 << 30
			})
			put := func(id uint64) int64 {
				r, ok := s.ok(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id), Value: []byte("v")}).(*pb.PutResponse)
				if !ok {
					t.Fatal("not a put response")
				}
				return r.Header.Revision
			}
			rev := put(1)
			s.ok(2, &pb.CompactionRequest{Revision: rev})
			if got, want := s.n.DurableCheckpoint().Index, s.n.Applied(); got != want {
				t.Fatalf("the compaction at %d left the checkpoint at %d", want, got)
			}
			rev = put(3)
			s.holdCopy = true
			s.ok(4, &pb.CompactionRequest{Revision: rev})
			copying := s.n.copy.index
			if copying != s.n.Applied() {
				t.Fatalf("copying the checkpoint at %d after the compaction at %d", copying, s.n.Applied())
			}
			s.ok(6, &pb.CompactionRequest{Revision: put(5)})
			if s.n.copy.index != copying {
				t.Fatalf("a compaction during the copy at %d cut at %d", copying, s.n.copy.index)
			}
			s.holdCopy = false
			s.run()
			if got := s.n.DurableCheckpoint().Index; got != copying {
				t.Fatalf("the checkpoint is at %d after the copy at %d", got, copying)
			}
			put(7)
			if got, want := s.n.DurableCheckpoint().Index, s.n.Applied(); got != want {
				t.Fatalf("the entry after the copy is %d, the checkpoint %d", want, got)
			}
		})
	}
}

// TestCheckpointState checks that the state of a checkpoint leaves out
// the base and the checksum, and nothing else.
func TestCheckpointState(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.ok(1, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	h := s.n.state.header(2, 1)
	file := func(base int64, index uint64) []byte {
		h.base, h.index = base, index
		b, err := h.encode()
		if err != nil {
			t.Fatal(err)
		}
		return append(b, "keys"+string(rune(base))...)
	}
	state := func(b []byte) string {
		st, err := CheckpointState(b)
		if err != nil {
			t.Fatal(err)
		}
		return string(st)
	}
	if state(file(1, 2)) != state(file(2, 2)) {
		t.Fatal("the state depends on the base")
	}
	if state(file(1, 2)) == state(file(1, 3)) {
		t.Fatal("the state does not depend on the index")
	}
}

// TestPhysicalCompaction checks that a physical compaction answers once a
// checkpoint at or after it is durable, as etcd v3.7.2 answers once its
// backend commit dropped the compacted versions: at once when no copy is
// under way, and otherwise after the copy and the cut it held back, with
// no write after the compaction. The compaction holds across a restart.
func TestPhysicalCompaction(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) { cfg.CheckpointBytes = 1 << 30 })
	put := func(id uint64) int64 {
		r, ok := s.ok(id, &pb.PutRequest{Key: []byte("k"), Value: fmt.Appendf(nil, "v%d", id)}).(*pb.PutResponse)
		if !ok {
			t.Fatal("not a put response")
		}
		return r.Header.Revision
	}
	rev := put(1)
	put(2)
	s.ok(3, &pb.CompactionRequest{Revision: rev, Physical: true})
	if got, want := s.n.DurableCheckpoint().Index, s.n.Applied(); got != want {
		t.Fatalf("the physical compaction at %d answered with the checkpoint at %d", want, got)
	}

	put(4)
	s.holdCopy = true
	s.ok(5, &pb.CompactionRequest{Revision: put(4)})
	copying := s.n.copy.index
	rev = put(6)
	s.n.Submit(Request{ID: 7, Op: &pb.CompactionRequest{Revision: rev, Physical: true}})
	for _, r := range s.run() {
		if r.ID == 7 {
			t.Fatalf("the physical compaction answered %v during the copy at %d", r, copying)
		}
	}
	compactAt := s.n.Applied()
	s.holdCopy = false
	answered := false
	for _, r := range s.run() {
		if r.ID == 7 {
			answered = r.Err == nil
		}
	}
	if got := s.n.DurableCheckpoint().Index; !answered || got != compactAt {
		t.Fatalf("after the copy: answered %v, the checkpoint at %d, the compaction at %d", answered, got, compactAt)
	}

	s = startSingle(t, disk, 2)
	if got := s.n.kv.CompactRev(); got != rev {
		t.Fatalf("restarted with the compaction at %d, want %d", got, rev)
	}
	if r := s.do(1, &pb.RangeRequest{Key: []byte("k"), Revision: rev - 1}); !errors.Is(r.Err, rpctypes.ErrGRPCCompacted) {
		t.Fatalf("a read below the compaction after the restart: %v", r.Err)
	}
}
