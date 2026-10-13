package core

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/batchcmd"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// Values in MVCC, Range and Watch all point into the immutable committed entry.
// Encoding an outward response copies those bytes into independently owned wire
// output. The custom frontend does not yet expose the core's Watch API.
func TestWireReadsAndWatchValuesReferenceLog(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	s.n.Watch(1, create(&pb.WatchCreateRequest{Key: []byte("k"), PrevKv: true}))
	s.run()
	s.responses()
	var entries [][]byte
	var events []*mvccpb.Event
	for i, value := range []string{"first-log-value", "second-log-value"} {
		id := uint64(i + 1)
		c, body := wireCommand(t, &pb.PutRequest{Key: []byte("k"), Value: []byte(value)})
		s.n.SubmitBatch(id, []batchcmd.Command{c})
		eff, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range eff.Persist {
			for _, e := range j.Entries {
				if len(e.GetData()) == 0 {
					continue
				}
				if err := DecodeBatch(e.GetData(), func(_, _, got uint64, op proto.Message) error {
					if got == id {
						entries = append(entries, op.(*pb.PutRequest).Value)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
		clear(body)
		s.persistAll(eff.Persist)
		for _, r := range s.run() {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
		}
		for _, w := range s.responses() {
			events = append(events, w.Events...)
			wire, err := proto.Marshal(w)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(wire, []byte(value)) {
				t.Fatalf("watch wire lacks %q", value)
			}
			clear(wire)
		}
	}
	if len(entries) != 2 || len(events) != 2 {
		t.Fatalf("entries=%d events=%d", len(entries), len(events))
	}
	same := func(label string, a, b []byte) {
		t.Helper()
		if len(a) == 0 || len(a) != len(b) || &a[0] != &b[0] {
			t.Fatalf("%s does not reference log bytes", label)
		}
	}
	same("create watch", events[0].Kv.Value, entries[0])
	same("update watch", events[1].Kv.Value, entries[1])
	same("previous watch", events[1].PrevKv.Value, entries[0])
	for i := range 2 {
		r, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("k"), Revision: int64(i + 2)})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Kvs) != 1 {
			t.Fatalf("range: %v", r)
		}
		same("historical/current Range", r.Kvs[0].Value, entries[i])
		wire, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(wire, entries[i]) {
			t.Fatal("Range wire lacks log value")
		}
		clear(wire)
	}
	if string(entries[0]) != "first-log-value" || string(entries[1]) != "second-log-value" {
		t.Fatal("response encoding or receive reuse changed log bytes")
	}
}
