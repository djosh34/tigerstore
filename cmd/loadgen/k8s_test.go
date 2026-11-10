package main

import (
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

// testTraces returns the recordings in bench/recordings, which every
// replay test runs over.
func testTraces(t *testing.T) []*k8strace.Trace {
	t.Helper()
	dir := os.Getenv("K8S_TRACES")
	if dir == "" {
		dir = filepath.Join("..", "..", "bench", "recordings")
	}
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
