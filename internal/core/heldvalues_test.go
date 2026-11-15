package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// get reads key with request id, at revision rev if not 0, and returns
// its value and whether the reply waited for a value read.
func (s *single) get(id uint64, key string, rev int64) (string, bool) {
	s.t.Helper()
	s.hold = true
	s.n.Submit(Request{ID: id, Op: &pb.RangeRequest{Key: []byte(key), Revision: rev}})
	got := replies(s)
	waited := len(got) == 0
	if waited {
		got = release(s.t, s)
	}
	s.hold = false
	r, ok := got[id]
	if !ok || r.Err != nil {
		s.t.Fatalf("read of %s: %v, %v", key, ok, r.Err)
	}
	kvs := rangeResp(r).Kvs
	if len(kvs) != 1 {
		s.t.Fatalf("read of %s: %d keys", key, len(kvs))
	}
	return string(kvs[0].Value), waited
}

// TestHeldValuesServeReads checks that the store holds the newest values
// whose entries left memory, so reading them waits for no disk, and that
// a value an overwrite replaced comes from the disk once its turn is over.
func TestHeldValuesServeReads(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	for i, k := range coldKeys {
		if v, waited := s.get(uint64(500+i), k, 0); v != "v"+k || waited {
			t.Fatalf("read of %s = %q, waited %v", k, v, waited)
		}
	}
	if m := s.n.Metrics(); m.CacheHits < int64(len(coldKeys)) || m.CacheMisses != 0 || m.CacheBytes == 0 {
		t.Fatalf("metrics: %+v", m)
	}
	rev := s.n.kv.Rev()
	s.ok(600, &pb.PutRequest{Key: []byte("a"), Value: []byte("new")})
	s.ok(601, &pb.PutRequest{Key: []byte("c"), IgnoreValue: true})
	if v, waited := s.get(602, "a", 0); v != "new" || waited {
		t.Fatalf("read of the new a = %q, waited %v", v, waited)
	}
	if v, waited := s.get(603, "c", 0); v != "vc" || waited {
		t.Fatalf("read of c after a put with ignore_value = %q, waited %v", v, waited)
	}
	if v, waited := s.get(604, "a", rev); v != "va" || !waited {
		t.Fatalf("read of the old a = %q, waited %v", v, waited)
	}
}

// TestWatchPrevFromMemory overwrites and deletes keys whose values'
// entries left memory, under a watch with prev_kv, as Kubernetes watches:
// the events carry the replaced values without a read of the disk, which
// took 8% of the leader's CPU on crd-heavy (#602).
func TestWatchPrevFromMemory(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	s.n.Watch(1, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a"), RangeEnd: []byte("z"), PrevKv: true}))
	s.run()
	s.responses()
	s.hold = true
	s.n.Submit(Request{ID: 700, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("new")}})
	s.n.Submit(Request{ID: 701, Op: &pb.DeleteRangeRequest{Key: []byte("b")}})
	s.run()
	if len(s.held) != 0 {
		t.Fatalf("the events waited for %d reads of the disk", len(s.held))
	}
	s.hold = false
	prev := map[string]string{}
	for _, r := range s.responses() {
		for _, ev := range r.Events {
			if ev.PrevKv != nil {
				prev[string(ev.Kv.Key)] = string(ev.PrevKv.Value)
			}
		}
	}
	if prev["a"] != "va" || prev["b"] != "vb" {
		t.Fatalf("previous values %v, want a: va, b: vb", prev)
	}
}

// TestHeldValueOnReadMiss checks that a newest value read from the log is
// held again, so the next read of it waits for no disk.
func TestHeldValueOnReadMiss(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	s.n.kv.SetValueLimit(0)
	s.n.kv.Step()
	if v, waited := s.get(500, "d", 0); v != "vd" || !waited {
		t.Fatalf("first read of d = %q, waited %v", v, waited)
	}
	if v, waited := s.get(501, "d", 0); v != "vd" || waited {
		t.Fatalf("second read of d = %q, waited %v", v, waited)
	}
	if m := s.n.Metrics(); m.CacheMisses != 1 {
		t.Fatalf("metrics: %+v", m)
	}
}

// TestHeldValuesShrinkAsOtherMemoryGrows checks that the held values stay
// within what the rest of the process leaves of MemoryBytes, as the
// driver measures it, and that the core's turns take the steps that give
// them up (Working, #647).
func TestHeldValuesShrinkAsOtherMemoryGrows(t *testing.T) {
	const memory = 8 << 10
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = memory })
	if s.n.kv.ValueLimit() != memory || s.n.kv.ValueBytes() == 0 {
		t.Fatalf("limit %d, %d bytes held", s.n.kv.ValueLimit(), s.n.kv.ValueBytes())
	}
	for other := int64(0); other <= memory; other += 1 << 10 {
		s.n.SetOtherMemory(other)
		for turns := 0; s.n.Working(); turns++ {
			if _, err := s.n.Process(); err != nil {
				t.Fatal(err)
			}
			if turns > 1000 {
				t.Fatal("the core's turns do not end the steps of its values")
			}
		}
		if limit, b := s.n.kv.ValueLimit(), s.n.kv.ValueBytes(); limit != memory-other || b > limit {
			t.Fatalf("with %d bytes besides the values: limit %d, %d bytes held", other, limit, b)
		}
	}
	if b := s.n.kv.ValueBytes(); b > 0 {
		t.Fatalf("the rest of the process takes all the memory, yet %d bytes are held", b)
	}
}

// TestCacheLow checks the low mark the memory gauge relies on: values
// let go of and held again between two calls report the low, and the
// next span starts from what is held then.
func TestCacheLow(t *testing.T) {
	s, _ := coldSingle(t, func(cfg *Config) { cfg.MemoryBytes = 1 << 20 })
	if s.n.kv.ValueBytes() == 0 {
		t.Fatal("setup: nothing held")
	}
	s.n.CacheLow()
	s.n.kv.SetValueLimit(0)
	s.n.kv.Step()
	s.n.kv.SetValueLimit(1 << 20)
	s.ok(600, &pb.PutRequest{Key: []byte("x"), Value: make([]byte, 1000)})
	if low := s.n.CacheLow(); low != 0 {
		t.Fatalf("emptied and filled again: low %d, want 0", low)
	}
	if low, now := s.n.CacheLow(), s.n.kv.ValueBytes(); low != now || now == 0 {
		t.Fatalf("next span: low %d, held %d", low, now)
	}
}
