package main

import (
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
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
			if txn, ok := m.(*pb.TxnRequest); ok && len(txn.Compare) != len(c.Op.Compare) {
				t.Fatalf("%s: call %d: %d compares, want %d", tr.Shape, i, len(txn.Compare), len(c.Op.Compare))
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
	if got := cp.rev(tr.Calls[1].Op.Compare[0].Rev, 1, 1); got != 500 {
		t.Errorf("call 1 compares against %d, want call 0's revision 500", got)
	}
	cp.reply(1, 501)
	if i, ok := cp.nextReady(); !ok || i != 2 || !cp.finished() {
		t.Fatalf("after call 1's reply: %d, %v, finished %v", i, ok, cp.finished())
	}
	// The watched prefix stays shared; the pod gets the copy's tag.
	if got := string(cp.appendKey(nil, 1)); got != "/registry/pods/c003.ns/p" {
		t.Errorf("key %q", got)
	}
	if got := string(cp.appendKey(nil, 0)); got != "/registry/pods/" {
		t.Errorf("watched prefix %q", got)
	}

	// From call 1 on, call 0 is the start's: call 1 is ready at once and
	// compares against the revision the preload gives the key.
	late := newCopy(tr, keys, tr.From(1), 4)
	late.itemRevs[0] = 42
	if i, ok := late.nextReady(); !ok || i != 1 {
		t.Fatalf("a copy from call 1: %d, %v", i, ok)
	}
	if got := late.rev(tr.Calls[1].Op.Compare[0].Rev, 1, 1); got != 42 {
		t.Errorf("compares against %d, want the preload's 42", got)
	}
}
