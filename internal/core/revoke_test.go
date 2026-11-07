package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/testmem"
)

// TestRevokeInSteps revokes a lease with 10,000 keys: each turn deletes at
// most revokeKeys of them, a read between the steps sees every key and
// the revision before the revoke, and at the end every key is gone at one
// revision, the watcher gets all 10,000 deletes at it, and a restart that
// replays the revoke has the same state (#505).
func TestRevokeInSteps(t *testing.T) {
	disk := simfs.New()
	s := startSingle(t, disk, 1)
	const keys = 10_000
	grant := s.do(1, &pb.LeaseGrantRequest{ID: 7, TTL: 3600})
	if grant.Err != nil {
		t.Fatal(grant.Err)
	}
	for i := range keys {
		s.n.Submit(Request{ID: uint64(i + 2), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%05d", i), Value: []byte("v"), Lease: 7}})
		if i%500 == 499 {
			s.run()
		}
	}
	s.run()
	before := s.n.kv.Rev()
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l")}))
	s.run()
	s.responses()
	steps := 0
	s.afterTurn = func(Effects) {
		if s.n.revoke == nil {
			return
		}
		steps++
		if n := len(s.n.state.Changes()); n > revokeKeys {
			t.Fatalf("one step of the revoke deleted %d keys", n)
		}
		r, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), CountOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if r.Count != keys || r.Header.Revision != before {
			t.Fatalf("between the revoke's steps a read sees %d keys at revision %d, want %d at %d", r.Count, r.Header.Revision, keys, before)
		}
	}
	// The watcher's client takes nothing during the revoke: its stream
	// has room for 64 KiB.
	s.room = 64 << 10
	s.n.WatchRoom(1, s.room, 1<<20)
	r := s.do(20_000, &pb.LeaseRevokeRequest{ID: 7})
	s.afterTurn = nil
	out := 0
	for _, w := range s.watch {
		out += proto.Size(w.Resp)
	}
	if out > int(s.room)+2*s.n.cfg.PieceBytes {
		t.Fatalf("a stream with room for %d bytes got %d during the revoke", s.room, out)
	}
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if steps < keys/revokeKeys {
		t.Fatalf("the revoke took %d steps", steps)
	}
	if rev := s.n.kv.Rev(); rev != before+1 || r.Resp.(*pb.LeaseRevokeResponse).Header.Revision != rev {
		t.Fatalf("after the revoke the store is at %d, want %d", rev, before+1)
	}
	if got, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), CountOnly: true}); err != nil || got.Count != 0 {
		t.Fatalf("after the revoke %v keys are left: %v", got, err)
	}
	if _, ok := s.n.state.leases[7]; ok {
		t.Fatal("the lease is still there")
	}
	events := 0
	for range 100 {
		for _, w := range s.responses() {
			for _, ev := range w.Events {
				if ev.Type != mvccpb.DELETE || ev.Kv.ModRevision != before+1 {
					t.Fatalf("the watcher got %v", ev)
				}
				events++
			}
		}
		if events == keys {
			break
		}
		// The client took what it had.
		s.n.WatchRoom(1, s.room, 1<<20)
		s.run()
	}
	if events != keys {
		t.Fatalf("the watcher got %d deletes, want %d", events, keys)
	}
	hash := s.n.Hash()
	again := startSingle(t, disk.Copy(), 2)
	if h := again.n.Hash(); h != hash {
		t.Fatalf("a restart that replays the revoke has hash %x, want %x", h, hash)
	}
}

// TestRevokePeakMemory revokes a lease of 50,000 keys with values of
// 1 KiB, once by request and once by its expiry, beside a watcher of
// those keys with prev_kv whose stream has room for 64 KiB. Built at once,
// the watcher's 50,000 events with their previous values would take over
// 50 MiB; in steps, with the watcher behind, the heap peaks at a few MiB
// above what the revoke leaves, its tombstones in the index (#505).
func TestRevokePeakMemory(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "request", true: "expiry"}[expiry], func(t *testing.T) {
			s := startSingle(t, simfs.New(), 1)
			if r := s.do(1, &pb.LeaseGrantRequest{ID: 7, TTL: 3}); r.Err != nil {
				t.Fatal(r.Err)
			}
			for i := range 50_000 {
				s.n.Submit(Request{ID: uint64(i + 2), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%05d", i), Value: make([]byte, 1024), Lease: 7}})
				if i%1000 == 999 {
					s.run()
				}
			}
			s.run()
			s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), PrevKv: true}))
			s.run()
			s.responses()
			s.room = 64 << 10
			s.n.WatchRoom(1, s.room, 1<<20)
			peak := testmem.Peak()
			if expiry {
				for range 100 {
					if _, ok := s.n.state.leases[7]; !ok {
						break
					}
					s.n.Tick()
					s.run()
				}
			} else if r := s.do(60_000, &pb.LeaseRevokeRequest{ID: 7}); r.Err != nil {
				t.Fatal(r.Err)
			}
			held, kept := peak()
			if _, ok := s.n.state.leases[7]; ok {
				t.Fatal("the lease is still there")
			}
			t.Logf("a revoke of 50,000 keys peaks at %d KiB, %d KiB above what it leaves", held>>10, (held-kept)>>10)
			if held-kept > 4<<20 {
				t.Fatalf("a revoke of 50,000 keys peaks at %d KiB above what it leaves", (held-kept)>>10)
			}
		})
	}
}
