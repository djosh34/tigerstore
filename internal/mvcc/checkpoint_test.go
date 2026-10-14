package mvcc

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// randomOp applies one random put, delete or compaction.
func randomOp(t *testing.T, s *Store, rng *rand.Rand) {
	t.Helper()
	key := []byte(fmt.Sprintf("k%02d", rng.IntN(30)))
	var err error
	switch r := rng.IntN(20); {
	case r == 0 && s.Rev() > 2:
		err = s.Compact(s.CompactRev() + 1 + rng.Int64N(s.Rev()-s.CompactRev()))
		if err != nil && s.CompactRev() >= s.Rev() {
			err = nil
		}
	case r < 5:
		_, err = s.DeleteRange(&pb.DeleteRangeRequest{Key: key})
	default:
		_, err = s.Put(&pb.PutRequest{Key: key, Value: []byte(fmt.Sprintf("v%d", rng.IntN(1000)))})
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestCheckpointCopyIsFrozen copies the index in small chunks while
// writes and compactions go on. The copy loads into the index as it was
// at the freeze, and the live index ends where one without a copy ends.
// The tracked hash matches a full walk all along.
func TestCheckpointCopyIsFrozen(t *testing.T) {
	for seed := range uint64(50) {
		rng := rand.New(rand.NewPCG(seed, 1))
		live, plain, at := New(&Heap{}), New(&Heap{}), New(&Heap{})
		live.TrackHash()
		steps := 100 + rng.IntN(100)
		for range steps {
			r := rng.Uint64()
			for _, s := range []*Store{live, plain, at} {
				randomOp(t, s, rand.New(rand.NewPCG(r, 2)))
			}
		}
		live.Freeze()
		var b []byte
		for done := false; !done; {
			var err error
			b, _, done = live.CopyFrozen(b, len(b)+1+rng.IntN(40))
			r := rng.Uint64()
			for _, s := range []*Store{live, plain} {
				randomOp(t, s, rand.New(rand.NewPCG(r, 2)))
			}
			if live.Hash() != plain.Hash() {
				t.Fatalf("seed %d: the tracked hash differs from the store without a copy", seed)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if live.frozen != nil {
			t.Fatal("the copy is still on")
		}
		loaded := New(&Heap{})
		if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), at.Rev(), at.CompactRev()); err != nil {
			t.Fatalf("seed %d: load: %v", seed, err)
		}
		if loaded.Hash() != at.Hash() {
			t.Fatalf("seed %d: the checkpoint is not the index at the freeze", seed)
		}
		if live.Hash() != plain.Hash() || live.versionSum() != live.sum {
			t.Fatalf("seed %d: after the copy the store differs from one without it", seed)
		}
		for cut := range len(b) - 1 {
			if _, err := New(&Heap{}).Load(bufio.NewReader(bytes.NewReader(b[:cut])), at.Rev(), at.CompactRev()); err == nil {
				t.Fatalf("seed %d: a checkpoint cut at %d of %d bytes loaded", seed, cut, len(b))
			}
		}
	}
}
