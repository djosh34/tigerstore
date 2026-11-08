package mvcc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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

// TestCheckpointLoadsEveryKeyItTakes puts a key longer than the old bound
// of the checkpoint's readers, 4 MiB: the checkpoint that holds it loads
// and its refs walk. A key past MaxKeyBytes is refused by the put (#438).
func TestCheckpointLoadsEveryKeyItTakes(t *testing.T) {
	s := New(&Heap{})
	key := bytes.Repeat([]byte{'k'}, 4<<20+1)
	if _, err := s.Put(&pb.PutRequest{Key: key, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	s.Freeze()
	var b []byte
	for done := false; !done; {
		b, _, done = s.CopyFrozen(b, 1<<20)
	}
	if _, err := New(&Heap{}).Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatalf("a key of %d bytes went in, but the checkpoint holding it does not load: %v", len(key), err)
	}
	if err := WalkRefs(bufio.NewReader(bytes.NewReader(b)), func(Ref) error { return nil }); err != nil {
		t.Fatalf("a key of %d bytes went in, but the walk of the checkpoint's refs fails: %v", len(key), err)
	}
	if err := CheckPutOp(&PutOp{Key: make([]byte, MaxKeyBytes+1)}); !errors.Is(err, rpctypes.ErrGRPCRequestTooLarge) {
		t.Fatalf("a put of a key of MaxKeyBytes+1 bytes: %v, want %v", err, rpctypes.ErrGRPCRequestTooLarge)
	}
}

// TestCheckpointChunkOfAHotKey copies a key with 20,000 versions in
// chunks of 1 KiB: each chunk stops inside the key's versions, the chunks
// load as one checkpoint, and a walk of the refs holds no list of the
// versions (#435).
func TestCheckpointChunkOfAHotKey(t *testing.T) {
	s := New(&Heap{})
	for range 20_000 {
		if _, err := s.Put(&pb.PutRequest{Key: []byte("hot"), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	s.Freeze()
	var all []byte
	for done := false; !done; {
		var b []byte
		b, _, done = s.CopyFrozen(nil, 1024)
		if len(b) > 1024+64 {
			t.Fatalf("a chunk with a limit of 1 KiB holds %d bytes", len(b))
		}
		all = append(all, b...)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(all)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	if k, ok := loaded.get([]byte("hot")); !ok || len(k.vers) != 20_000 {
		t.Fatalf("the loaded checkpoint lost versions of the hot key")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	refs := 0
	if err := WalkRefs(bufio.NewReader(bytes.NewReader(all)), func(Ref) error { refs++; return nil }); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if refs != 20_000 {
		t.Fatalf("the walk visited %d refs, want 20000", refs)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 64<<10 {
		t.Fatalf("a walk of the refs of one key with 20,000 versions allocated %d bytes", alloc)
	}
}

// TestCopyFrozenInSmallChunks copies an index of short, long and hot keys
// in chunks of 97 bytes: each chunk passes the limit by at most one
// version and a key's uvarints, a chunk can stop inside a key of 3 KiB or
// inside a key's 300 versions, and the chunks load as the same index
// (#434, #435).
func TestCopyFrozenInSmallChunks(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 35))
	s := New(&Heap{})
	for i := range 2000 {
		var key []byte
		switch {
		case i%400 == 0:
			key = fmt.Appendf(nil, "long-%d-%s", i, bytes.Repeat([]byte{'x'}, 3000))
		case i%3 == 0:
			key = []byte("hot")
		default:
			key = fmt.Appendf(nil, "k%04d", rng.IntN(500))
		}
		if _, err := s.Put(&pb.PutRequest{Key: key, Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	s.Freeze()
	const limit, slack = 97, 2*10 + 5*10 + 3*10 + 4
	var all []byte
	for done := false; !done; {
		var b []byte
		b, _, done = s.CopyFrozen(nil, limit)
		if len(b) > limit+slack {
			t.Fatalf("a chunk with a limit of %d holds %d bytes", limit, len(b))
		}
		all = append(all, b...)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(all)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	if loaded.Hash() != s.Hash() {
		t.Fatal("the chunks load as another index")
	}
}

// TestLongKeyAtChunkEnd starts a key of 4 MiB with 0 to 30 bytes left in
// a chunk of 1 KiB, its length's uvarint among them: no chunk may hold
// the whole key, and the chunks load (#435).
func TestLongKeyAtChunkEnd(t *testing.T) {
	long := bytes.Repeat([]byte{'b'}, 4<<20)
	for first := 990; first <= 1020; first++ {
		s := New(&Heap{})
		for _, k := range [][]byte{bytes.Repeat([]byte{'a'}, first), long} {
			if _, err := s.Put(&pb.PutRequest{Key: k, Value: []byte("v")}); err != nil {
				t.Fatal(err)
			}
		}
		s.Freeze()
		var all []byte
		for done := false; !done; {
			var b []byte
			b, _, done = s.CopyFrozen(nil, 1024)
			if len(b) > 1024+64 {
				t.Fatalf("with a first key of %d bytes, a chunk with a limit of 1 KiB holds %d bytes", first, len(b))
			}
			all = append(all, b...)
		}
		if _, err := New(&Heap{}).Load(bufio.NewReader(bytes.NewReader(all)), s.Rev(), s.CompactRev()); err != nil {
			t.Fatalf("with a first key of %d bytes: %v", first, err)
		}
	}
}

// TestCheckpointOfAKeyChangedThreeTimesInARevision puts a key at revision
// 2, then deletes it, puts it and deletes it again in one Txn at revision
// 3: the key has 4 versions in a store at revision 3, and the store's own
// checkpoint must load (#548).
func TestCheckpointOfAKeyChangedThreeTimesInARevision(t *testing.T) {
	s := New(&Heap{})
	if _, err := s.Put(&pb.PutRequest{Key: []byte("a"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte{0}}}}
	if _, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		del,
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("again")}}},
		del,
	}}); err != nil {
		t.Fatal(err)
	}
	if s.Rev() != 3 {
		t.Fatalf("setup: the store is at revision %d, want 3", s.Rev())
	}
	s.Freeze()
	var b []byte
	for done := false; !done; {
		b, _, done = s.CopyFrozen(b, 1<<20)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatalf("the store's own checkpoint at revision %d does not load: %v", s.Rev(), err)
	}
	if loaded.Hash() != s.Hash() {
		t.Fatalf("the loaded store's hash is %x, the store's %x", loaded.Hash(), s.Hash())
	}
}

// TestDamagedKeyLengthCostsNoMemory loads a checkpoint of four bytes that
// says its first key takes MaxKeyBytes: the load must fail without making
// room for a key the input does not hold.
func TestDamagedKeyLengthCostsNoMemory(t *testing.T) {
	b := binary.AppendUvarint(nil, MaxKeyBytes)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := New(&Heap{}).Load(bufio.NewReader(bytes.NewReader(b)), 2, 0)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrBadCheckpoint) {
		t.Fatalf("a key length past the input: %v, want ErrBadCheckpoint", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("the load allocated %d bytes for a key the input does not hold", got)
	}
}
