package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// testTraceDir is bench/recordings, or K8S_TRACES.
func testTraceDir() string {
	if dir := os.Getenv("K8S_TRACES"); dir != "" {
		return dir
	}
	return filepath.Join("..", "..", "bench", "recordings")
}

// testTraces returns the recordings in bench/recordings, which every
// replay test runs over.
func testTraces(t *testing.T) []*k8strace.Trace {
	t.Helper()
	dir := testTraceDir()
	names, err := filepath.Glob(filepath.Join(dir, "*.trace.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatalf("no recordings in %s", dir)
	}
	var out []*k8strace.Trace
	for _, n := range names {
		tr, err := k8strace.Read(n)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, tr)
	}
	return out
}

// TestRequestsDecode encodes every call of every recording as a copy
// starting a quarter of the way in would, and decodes it with etcd's own
// types: the requests are what etcd takes.
func TestRequestsDecode(t *testing.T) {
	for _, tr := range testTraces(t) {
		cp := newCopy(tr, newK8sKeys(tr), tr.From(len(tr.Calls)/4), 7)
		for i := range cp.revs {
			cp.revs[i] = int64(i + 1)
		}
		e := encoder{cp: cp, values: make([]byte, 1<<20)}
		var buf []byte
		for i := range tr.Calls {
			c := &tr.Calls[i]
			e.call = int32(i)
			buf = e.request(buf[:0], c)
			var m proto.Message
			switch c.Kind {
			case k8strace.CallRange, k8strace.CallRangeStream:
				m = &pb.RangeRequest{}
			case k8strace.CallPut:
				m = &pb.PutRequest{}
			case k8strace.CallDelete:
				m = &pb.DeleteRangeRequest{}
			case k8strace.CallTxn:
				m = &pb.TxnRequest{}
			case k8strace.CallCompact:
				m = &pb.CompactionRequest{}
			case k8strace.CallLeaseGrant:
				m = &pb.LeaseGrantRequest{}
			case k8strace.CallProgress:
				m = &pb.WatchRequest{}
			default:
				m = &pb.LeaseRevokeRequest{}
			}
			if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(buf, m); err != nil {
				t.Fatalf("%s: call %d (%s): %v", tr.Shape, i, k8strace.KindNames[c.Kind], err)
			}
			if txn, ok := m.(*pb.TxnRequest); ok {
				if err := sameTxn(cp, txn, &c.Op); err != nil {
					t.Fatalf("%s: call %d: %v", tr.Shape, i, err)
				}
			}
		}
	}
}

// TestCopyWaits sends a call only once the replies it waits on came, and
// names the revisions those replies gave: a create, an update that waits
// on it, and a read that waits on the update. A copy that starts at call
// 1 has call 0 behind it, in its start.
func TestCopyWaits(t *testing.T) {
	tr := &k8strace.Trace{
		Keys:    []string{"/registry/pods/", "/registry/pods/ns/p", "/registry/pods0"},
		Watches: []k8strace.WatchStream{{Creates: []k8strace.Watch{{Key: 0, End: 2}}}},
		Calls: []k8strace.Call{
			{Kind: k8strace.CallTxn, Op: k8strace.Op{Kind: k8strace.OpTxn, Compare: []k8strace.Compare{{Target: k8strace.TargetMod, Key: 1}}, Success: []k8strace.Op{{Kind: k8strace.OpPut, Key: 1, End: -1, Size: 5}}}},
			{Kind: k8strace.CallTxn, Waits: []int32{0}, Op: k8strace.Op{Kind: k8strace.OpTxn, Compare: []k8strace.Compare{{Target: k8strace.TargetMod, Key: 1, Rev: 1}}, Success: []k8strace.Op{{Kind: k8strace.OpPut, Key: 1, End: -1, Size: 6}}}},
			{Kind: k8strace.CallRange, Waits: []int32{1}, Op: k8strace.Op{Kind: k8strace.OpRange, Key: 1, End: -1}},
		},
	}
	keys := newK8sKeys(tr)
	cp := newCopy(tr, keys, tr.From(0), 3)
	if i, ok := cp.nextReady(); !ok || i != 0 {
		t.Fatalf("first ready call %d, %v; want 0", i, ok)
	}
	if i, ok := cp.nextReady(); ok {
		t.Fatalf("call %d is ready before call 0's reply", i)
	}
	cp.reply(0, 500)
	if i, ok := cp.nextReady(); !ok || i != 1 {
		t.Fatalf("after call 0's reply: %d, %v; want 1", i, ok)
	}
	if got := cp.rev(tr.Calls[1].Op.Compare[0].Rev, 1); got != 500 {
		t.Errorf("call 1 compares against %d, want call 0's revision 500", got)
	}
	cp.reply(1, 501)
	if i, ok := cp.nextReady(); !ok || i != 2 || !cp.finished() {
		t.Fatalf("after call 1's reply: %d, %v, finished %v", i, ok, cp.finished())
	}
	// The watched prefix stays shared; the pod gets the copy's tag.
	if got := string(cp.keyOf(1)); got != "/registry/pods/c003.ns/p" {
		t.Errorf("key %q", got)
	}
	if got := string(cp.keyOf(0)); got != "/registry/pods/" {
		t.Errorf("watched prefix %q", got)
	}

	// From call 1 on, call 0 is the start's: call 1 is ready at once and
	// compares against the revision the preload gives the key.
	late := newCopy(tr, keys, tr.From(1), 4)
	late.itemRevs[0] = 42
	if i, ok := late.nextReady(); !ok || i != 1 {
		t.Fatalf("a copy from call 1: %d, %v", i, ok)
	}
	if got := late.rev(tr.Calls[1].Op.Compare[0].Rev, 1); got != 42 {
		t.Errorf("compares against %d, want the preload's 42", got)
	}
}

// sameTxn checks a decoded Txn against the trace's: its compares, and
// each op of both branches, with its kind, the copy's key and the value
// size.
func sameTxn(cp *k8sCopy, got *pb.TxnRequest, want *k8strace.Op) error {
	if len(got.Compare) != len(want.Compare) || len(got.Success) != len(want.Success) || len(got.Failure) != len(want.Failure) {
		return fmt.Errorf("%d compares, %d and %d ops; want %d, %d and %d",
			len(got.Compare), len(got.Success), len(got.Failure), len(want.Compare), len(want.Success), len(want.Failure))
	}
	ops := func(got []*pb.RequestOp, want []k8strace.Op) error {
		for i, o := range want {
			var key []byte
			switch r := got[i].Request.(type) {
			case *pb.RequestOp_RequestRange:
				key = r.RequestRange.Key
				if o.Kind != k8strace.OpRange {
					return fmt.Errorf("op %d is a range, want kind %d", i, o.Kind)
				}
			case *pb.RequestOp_RequestPut:
				key = r.RequestPut.Key
				if o.Kind != k8strace.OpPut || len(r.RequestPut.Value) != int(o.Size) {
					return fmt.Errorf("op %d is a put of %d bytes, want kind %d of %d", i, len(r.RequestPut.Value), o.Kind, o.Size)
				}
			case *pb.RequestOp_RequestDeleteRange:
				key = r.RequestDeleteRange.Key
				if o.Kind != k8strace.OpDelete {
					return fmt.Errorf("op %d is a delete, want kind %d", i, o.Kind)
				}
			case *pb.RequestOp_RequestTxn:
				if o.Kind != k8strace.OpTxn {
					return fmt.Errorf("op %d is a txn, want kind %d", i, o.Kind)
				}
				return sameTxn(cp, r.RequestTxn, &want[i])
			default:
				return fmt.Errorf("op %d is empty", i)
			}
			if want := string(cp.keyOf(o.Key)); string(key) != want {
				return fmt.Errorf("op %d has key %q, want %q", i, key, want)
			}
		}
		return nil
	}
	if err := ops(got.Success, want.Success); err != nil {
		return fmt.Errorf("success: %w", err)
	}
	if err := ops(got.Failure, want.Failure); err != nil {
		return fmt.Errorf("failure: %w", err)
	}
	return nil
}

// TestTxnKVs counts the key-values a Txn's answer carries: a range's and
// a nested Txn's as reads, a put's and a delete's previous ones apart.
func TestTxnKVs(t *testing.T) {
	kv := &mvccpb.KeyValue{Key: []byte("/k"), Value: []byte("12345")}
	resp := &pb.TxnResponse{Header: &pb.ResponseHeader{Revision: 9}, Responses: []*pb.ResponseOp{
		{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{kv}}}},
		{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{PrevKv: kv}}},
		{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{PrevKvs: []*mvccpb.KeyValue{kv, kv}}}},
		{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: &pb.TxnResponse{Responses: []*pb.ResponseOp{
			{Response: &pb.ResponseOp_ResponseRange{ResponseRange: &pb.RangeResponse{Kvs: []*mvccpb.KeyValue{kv}}}},
		}}}},
	}}
	m, err := proto.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var st stream
	if err := txnKVs(&st, m); err != nil {
		t.Fatal(err)
	}
	if st.c.KVs != 2 || st.c.PrevKVs != 3 {
		t.Errorf("%d read and %d previous key-values, want 2 and 3", st.c.KVs, st.c.PrevKVs)
	}
}

// TestTally counts what each kind of call makes durable and looks up: a
// put's lease ID, a CAS's compare only toward a branch that writes, a
// compare of a key no op touches as a check, and lease calls and
// compactions as entries of IDs.
func TestTally(t *testing.T) {
	const key, other = 0, 1
	tr := &k8strace.Trace{Keys: []string{"/registry/pods/ns/a", "/registry/pods/ns/b"}, Calls: make([]k8strace.Call, 1)}
	cp := newCopy(tr, newK8sKeys(tr), tr.From(0), 0)
	cp.grants[0] = 5 // call 0 granted lease 1
	n := int64(len(tr.Keys[key]) + len(cp.tag))
	cmp := []k8strace.Compare{{Target: k8strace.TargetMod, Key: key, End: -1, Rev: 0}}
	put := k8strace.Op{Kind: k8strace.OpPut, Key: key, End: -1, Size: 100, Lease: 1}
	get := k8strace.Op{Kind: k8strace.OpRange, Key: key, End: -1}
	for _, c := range []struct {
		name       string
		call       k8strace.Call
		ok, failed tally
	}{
		{
			"put with a lease",
			k8strace.Call{Kind: k8strace.CallPut, Op: put},
			tally{bytes: n + 100 + idBytes, writes: 1, entries: 1, leases: 1},
			tally{},
		},
		{
			"CAS that reads on failure",
			k8strace.Call{Kind: k8strace.CallTxn, Op: k8strace.Op{
				Kind: k8strace.OpTxn, Compare: cmp,
				Success: []k8strace.Op{put}, Failure: []k8strace.Op{get},
			}},
			tally{bytes: 2*n + revisionBytes + 100 + idBytes, writes: 1, creates: 1, entries: 1, leases: 1},
			tally{},
		},
		{
			"compare of a key no op touches",
			k8strace.Call{Kind: k8strace.CallTxn, Op: k8strace.Op{
				Kind:    k8strace.OpTxn,
				Compare: []k8strace.Compare{{Target: k8strace.TargetVersion, Key: other, End: -1}}, Success: []k8strace.Op{get},
			}},
			tally{checks: 1},
			tally{checks: 1},
		},
		{"lease grant", k8strace.Call{Kind: k8strace.CallLeaseGrant, TTL: 60}, tally{bytes: 2 * idBytes, entries: 1, leases: 1}, tally{}},
		{"lease revoke", k8strace.Call{Kind: k8strace.CallLeaseRevoke, Lease: 1}, tally{bytes: idBytes, entries: 1, leases: 1}, tally{}},
		{"lease keepalive", k8strace.Call{Kind: k8strace.CallLeaseKeepAlive, Lease: 1}, tally{leases: 1}, tally{}},
		{"compaction", k8strace.Call{Kind: k8strace.CallCompact}, tally{bytes: idBytes, entries: 1}, tally{}},
	} {
		e := encoder{cp: cp, values: make([]byte, 1<<10)}
		e.request(nil, &c.call)
		if e.ok != c.ok || e.failed != c.failed {
			t.Errorf("%s: %+v and %+v, want %+v and %+v", c.name, e.ok, e.failed, c.ok, c.failed)
		}
	}
}

// TestLeftToCopy0 leaves the watch cache's calls, whole-resource reads and
// progress requests, to copy 0, and every other call to its own copy.
func TestLeftToCopy0(t *testing.T) {
	tr := &k8strace.Trace{
		Keys:    []string{"/registry/pods/", "/registry/pods0", "/registry/pods/ns/p"},
		Watches: []k8strace.WatchStream{{Creates: []k8strace.Watch{{Key: 0, End: 1}}}},
		Calls: []k8strace.Call{
			{Kind: k8strace.CallRangeStream, Op: k8strace.Op{Kind: k8strace.OpRange, Key: 0, End: 1, Limit: 10000}},
			{Kind: k8strace.CallProgress},
			{Kind: k8strace.CallRange, Op: k8strace.Op{Kind: k8strace.OpRange, Key: 2, End: -1}},
		},
	}
	keys := newK8sKeys(tr)
	zero, one := newCopy(tr, keys, tr.From(0), 0), newCopy(tr, keys, tr.From(0), 1)
	for i := range tr.Calls {
		if zero.leftToCopy0(int32(i)) {
			t.Errorf("copy 0 leaves %s to itself", k8strace.KindNames[tr.Calls[i].Kind])
		}
	}
	if !one.leftToCopy0(0) || !one.leftToCopy0(1) || one.leftToCopy0(2) {
		t.Error("copy 1 leaves the list and the progress request, and sends the GET")
	}
}

// TestK8sFlags refuses bad flags for k8s as for any workload, and a
// -traces that is no directory.
func TestK8sFlags(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct {
		name string
		args []string
		ok   bool
	}{
		{"good", []string{"-traces", dir}, true},
		{"no duration", []string{"-traces", dir, "-duration", "0"}, false},
		{"negative rate", []string{"-traces", dir, "-rate", "-5"}, false},
		{"ramp at a rate", []string{"-traces", dir, "-ramp", "-rate", "100"}, false},
		{"fewer copies than connections", []string{"-traces", dir, "-copies", "2", "-conns", "3"}, false},
		{"no traces", []string{"-traces", filepath.Join(dir, "none")}, false},
	} {
		_, err := parseFlags(append([]string{"-workload", "k8s"}, c.args...))
		if (err == nil) != c.ok {
			t.Errorf("%s: error %v", c.name, err)
		}
	}
}
