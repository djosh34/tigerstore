package main

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8ssize"
)

func TestMixedScopes(t *testing.T) {
	for _, r := range k8ssize.Resources() {
		key := mixedName(r, 2, 3, 4)
		parts := bytes.Split(key, []byte("/"))
		want := 5
		if clusterScoped(r) {
			want = 4
		}
		if len(parts) != want || !bytes.HasPrefix(key, mixedPrefix(r, 2, 3)) {
			t.Fatalf("wrong scope for %s: %s", r, key)
		}
	}
	for _, r := range []string{"apiservices", "clusterroles", "minions", "namespaces", "storageclasses", "csinodes"} {
		if !clusterScoped(r) {
			t.Fatalf("%s should be cluster scoped", r)
		}
	}
	if clusterScoped("pods") {
		t.Fatal("pods should be namespaced")
	}
}

// TestMixedOwnership checks that no two workers share a key or a LIST
// prefix, and that every key is under its group's prefix.
func TestMixedOwnership(t *testing.T) {
	seen := map[string]int{}
	cfg := config{keys: 2000, conns: 6, mix: defaultMix, seed: 1}
	var loads []*mixedLoad
	for owner := range cfg.conns {
		w, err := newMixedLoad(newWorker(nil, uint64(owner+1)), cfg, owner, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Include newly created keys, not only the preload.
		for range 100 {
			w.live = append(w.live, w.newKey())
		}
		for _, k := range w.live {
			key := string(k.key)
			if previous, ok := seen[key]; ok {
				t.Fatalf("key %s owned by %d and %d", key, previous, owner)
			}
			seen[key] = owner
			if !bytes.HasPrefix(k.key, w.prefixes[k.group]) {
				t.Fatalf("key %s is not under its group %s", k.key, w.prefixes[k.group])
			}
		}
		loads = append(loads, w)
	}
	for _, w := range loads {
		for _, p := range w.prefixes {
			for key, owner := range seen {
				if owner != w.owner && bytes.HasPrefix([]byte(key), p) {
					t.Fatalf("prefix %s of %d holds %s of %d", p, w.owner, key, owner)
				}
			}
		}
	}
}

func TestMixedMix(t *testing.T) {
	for _, s := range []string{"", "18,10,2,50,12", "18,10,2,50,12,9", "18,10,2,50,21,-1", "18,10,x,50,12,8"} {
		if _, err := parseMix(s); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if _, err := parseMix(defaultMix); err != nil {
		t.Fatal(err)
	}
}

func testMixed(t *testing.T, keys int) (*mixedLoad, *conn) {
	t.Helper()
	c := &conn{addr: "h:1", streams: make(map[uint32]*stream), nextID: 1, sendWin: 1 << 30, streamWin: 65535, maxStreams: math.MaxUint32}
	wk := newWorker(c, 1)
	wk.stats.errs = make(map[string]int64)
	w, err := newMixedLoad(wk, config{keys: keys, conns: 1, streams: 16, mix: defaultMix}, 0, newMixedLedger())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range w.live {
		k.mod = 2
	}
	return w, c
}

// TestMixedRequests decodes every call with etcd's own types and compares
// it with what kube-apiserver 1.37 sends through
// go.etcd.io/etcd/client/v3/kubernetes.
func TestMixedRequests(t *testing.T) {
	w, _ := testMixed(t, 1)
	w.lease = 99
	k := &mixedKey{key: []byte("/registry/events/bench-000-0000/object-000000000001"), mod: 17, size: 40}
	k.group = slices.Index(w.resources, "events") * w.namespaces
	value := []byte("the value")
	cmp := func(rev int64) []*pb.Compare {
		return []*pb.Compare{{Target: pb.Compare_MOD, Key: k.key, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}}
	}
	get := []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: k.key}}}}
	for _, tc := range []struct {
		call    mixedCall
		path    string
		want    proto.Message
		written int
		event   int // the key and value bytes of the watch event
	}{
		{mixedCall{op: mixedGet, key: k}, "/etcdserverpb.KV/Range", &pb.RangeRequest{Key: k.key, Limit: 1}, 0, 0},
		{
			mixedCall{op: mixedList, key: k},
			"/etcdserverpb.KV/RangeStream",
			&pb.RangeRequest{Key: w.prefixes[k.group], RangeEnd: prefixEnd(w.prefixes[k.group]), Limit: listPage}, 0, 0,
		},
		// An event keeps its lease when it is updated, as in
		// kube-apiserver's GuaranteedUpdate.
		{mixedCall{op: mixedUpdate, key: k, value: value}, "/etcdserverpb.KV/Txn", &pb.TxnRequest{
			Compare: cmp(17),
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: k.key, Value: value, Lease: 99}}}},
			Failure: get,
		}, 2*len(k.key) + 16 + len(value), 2*len(k.key) + len(value) + 40},
		{mixedCall{op: mixedDelete, key: k}, "/etcdserverpb.KV/Txn", &pb.TxnRequest{
			Compare: cmp(17),
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: k.key}}}},
			Failure: get,
		}, 2*len(k.key) + 8, 2*len(k.key) + 40},
	} {
		path, written, event := w.request(&tc.call)
		got := tc.want.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(w.buf, got); err != nil {
			t.Fatal(err)
		}
		if path != tc.path || !proto.Equal(got, tc.want) || written != int64(tc.written) || event != int64(tc.event) {
			t.Fatalf("call %d: %s %v, %d bytes, event %d; want %s %v, %d, %d", tc.call.op, path, got, written, event, tc.path, tc.want, tc.written, tc.event)
		}
	}
	// A create compares the mod revision with 0, which must be on the
	// wire, and an event takes the events' lease. Its event has no
	// previous key-value.
	k.mod = 0
	_, written, event := w.request(&mixedCall{op: mixedCreate, key: k, value: value})
	got := &pb.TxnRequest{}
	if err := proto.Unmarshal(w.buf, got); err != nil {
		t.Fatal(err)
	}
	want := &pb.TxnRequest{Compare: cmp(0), Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: k.key, Value: value, Lease: 99}}}}}
	if !proto.Equal(got, want) || written != int64(2*len(k.key)+16+len(value)) || event != int64(len(k.key)+len(value)) {
		t.Fatalf("create: %v, %d bytes, event %d", got, written, event)
	}
	// A pod has no lease.
	k.group = slices.Index(w.resources, "pods") * w.namespaces
	w.request(&mixedCall{op: mixedCreate, key: k, value: value})
	if err := proto.Unmarshal(w.buf, got); err != nil || got.Success[0].GetRequestPut().Lease != 0 {
		t.Fatalf("pod create %v: %v", got, err)
	}
	if _, ok := got.Compare[0].TargetUnion.(*pb.Compare_ModRevision); !ok {
		t.Fatal("create sent no mod revision")
	}
	w.request(&mixedCall{op: mixedRev})
	r := &pb.RangeRequest{}
	if err := proto.Unmarshal(w.buf, r); err != nil || r.Limit != 1 || r.RangeEnd != nil || !bytes.HasPrefix(r.Key, []byte("/registry/")) || bytes.Count(r.Key, []byte("/")) != 2 {
		t.Fatalf("revision check %v: %v", r, err)
	}
}

func txnAnswer(t *testing.T, rev int64, ok bool) []byte {
	t.Helper()
	b, err := proto.Marshal(&pb.TxnResponse{Header: &pb.ResponseHeader{Revision: rev}, Succeeded: ok})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestMixedTransitions takes one key through update, delete, create and
// update, and checks where it lives and what the next compare holds.
func TestMixedTransitions(t *testing.T) {
	w, c := testMixed(t, 1)
	k := w.live[0]
	for i, op := range []int{mixedUpdate, mixedDelete, mixedCreate, mixedUpdate} {
		w.pending = &mixedCall{op: op, key: k, value: []byte("next value")}
		if op == mixedCreate {
			w.absent = w.absent[:0]
		}
		st := c.newStream(w)
		st.msgs = 1
		if !w.start(c, st, 16) || !k.busy {
			t.Fatalf("step %d could not start", i)
		}
		if err := w.message(st, txnAnswer(t, int64(i+3), true)); err != nil {
			t.Fatal(err)
		}
		c.finish(st, nil)
		if k.busy {
			t.Fatalf("step %d left the key busy", i)
		}
		if op == mixedDelete {
			if len(w.live) != 0 || len(w.absent) != 1 || k.mod != 0 {
				t.Fatalf("after delete: %d live, %d absent, mod %d", len(w.live), len(w.absent), k.mod)
			}
		} else if len(w.live) != 1 || k.mod != int64(i+3) || k.size != len("next value") {
			t.Fatalf("after step %d: %d live, mod %d, size %d", i, len(w.live), k.mod, k.size)
		}
	}
	if w.wk.stats.acked != 4 || len(w.wk.stats.errs) != 0 {
		t.Fatalf("acked %d, errors %v", w.wk.stats.acked, w.wk.stats.errs)
	}
	w.pending = &mixedCall{op: mixedUpdate, key: k, value: []byte("v")}
	st := c.newStream(w)
	if !w.start(c, st, 16) {
		t.Fatal("could not start")
	}
	if err := w.message(st, txnAnswer(t, 9, false)); !errors.Is(err, errCASFailed) {
		t.Fatalf("a failed CAS: %v", err)
	}
}

// TestMixedChoosesIdleKeys never starts a write of a key with a write in
// flight.
func TestMixedChoosesIdleKeys(t *testing.T) {
	w, c := testMixed(t, 3)
	for range 1000 {
		st := c.newStream(w)
		if !w.start(c, st, 1<<20) {
			continue
		}
		call := w.calls[st.id]
		if call.op >= mixedUpdate && call.op != mixedCreate && !call.key.busy {
			t.Fatal("a write left its key idle")
		}
	}
	busy := map[*mixedKey]int{}
	for _, call := range w.calls {
		if call.op >= mixedUpdate {
			busy[call.key]++
		}
	}
	for k, n := range busy {
		if n > 1 {
			t.Fatalf("%d writes of %s in flight", n, k.key)
		}
	}
}

func TestMixedLedger(t *testing.T) {
	for _, watchFirst := range []bool{false, true} {
		l := newMixedLedger()
		l.window(1, 100)
		if watchFirst {
			l.saw(3, 30)
		}
		l.wrote(3, 10)
		if !watchFirst {
			l.saw(3, 30)
		}
		if len(l.writes)+len(l.events) != 0 || l.lat.n != 1 || l.lat.min != 20 || l.count != 1 {
			t.Fatalf("watch first %v: %+v", watchFirst, l)
		}
		// An event after the window counts no bytes, and a write meant
		// for before it no delay.
		l.wrote(4, 0)
		l.saw(4, 200)
		if l.lat.n != 1 || l.count != 1 {
			t.Fatalf("outside the window: %+v", l)
		}
	}
}

// TestMixedWatchEvent reads the revision of a watch event, and refuses a
// canceled watch.
func TestMixedWatchEvent(t *testing.T) {
	l := newMixedLedger()
	l.window(0, 1<<62)
	w := &mixedWatch{ledger: l, prefix: []byte("/registry/pods/")}
	created, err := proto.Marshal(&pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 1}, Created: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.response(created, 5); err != nil || !w.created {
		t.Fatalf("created: %v", err)
	}
	event, err := proto.Marshal(&pb.WatchResponse{Header: &pb.ResponseHeader{Revision: 9}, Events: []*mvccpb.Event{{
		Kv:     &mvccpb.KeyValue{Key: []byte("/registry/pods/a"), Value: make([]byte, 100), ModRevision: 9},
		PrevKv: &mvccpb.KeyValue{Key: []byte("/registry/pods/a"), Value: make([]byte, 40), ModRevision: 4},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.response(event, 5); err != nil {
		t.Fatal(err)
	}
	if l.count != 1 || l.events[9] != 5 {
		t.Fatalf("event: count %d, events %v", l.count, l.events)
	}
	canceled, err := proto.Marshal(&pb.WatchResponse{Canceled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.response(canceled, 6); err == nil {
		t.Fatal("a canceled watch was taken")
	}
}

// TestMixedPreload keeps every preload Txn within 128 puts and 60 KiB.
func TestMixedPreload(t *testing.T) {
	w, c := testMixed(t, 1000)
	p := &mixedPreload{w: w, batch: make(map[uint32][]*mixedKey)}
	for !p.exhausted() {
		st := c.newStream(p)
		before := len(c.out)
		if !p.start(c, st, 1<<20) {
			t.Fatal("preload could not start")
		}
		if n := len(p.batch[st.id]); n == 0 || n > 128 || len(c.out)-before > 61<<10 {
			t.Fatalf("a Txn of %d puts and %d bytes", n, len(c.out)-before)
		}
		c.finish(st, nil)
	}
}

func TestMixedArrivalScheduleReset(t *testing.T) {
	for _, preloadArrivals := range []int{10, 1000} {
		wk := newWorker(nil, 17)
		for range preloadArrivals {
			_ = wk.gap(300)
		}
		wk.arrivals = newArrivalRNG(17)
		fresh := newWorker(nil, 17)
		for range 100 {
			if wk.gap(500) != fresh.gap(500) {
				t.Fatal("preload completion changed the measurement schedule")
			}
		}
	}
}

func TestMixedRateFlags(t *testing.T) {
	for _, flag := range []string{"-rate", "-preload-rate"} {
		for _, v := range []string{"NaN", "Inf", "-1"} {
			if _, err := parseFlags([]string{"-workload", "mixed", flag, v}); err == nil {
				t.Fatalf("accepted %s %s", flag, v)
			}
		}
	}
	cfg, err := parseFlags([]string{"-workload", "mixed"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.mix != defaultMix || cfg.preloadRate != 300 {
		t.Fatal("wrong defaults")
	}
	for _, args := range [][]string{{"-ramp", "-rate", "10"}, {"-ramp", "-workload", "delete"}, {"-ramp", "-workload", "watch"}} {
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestVariableValuePoolWrap(t *testing.T) {
	v := values{pool: make([]byte, 100), size: 8, off: 80}
	if len(v.next()) != 8 {
		t.Fatal("short value")
	}
	v.size = 30
	if len(v.next()) != 30 || v.off != 30 {
		t.Fatal("variable size did not wrap")
	}
}
