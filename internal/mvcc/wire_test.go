package mvcc

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// TestAppendEventIsMarshal checks that the events the store writes itself
// are the bytes protobuf's Marshal writes for Event's message, and as long
// as EventSize says, for puts, updates, deletes, leases, empty values and
// a Txn that writes a key twice, with values held and not.
func TestAppendEventIsMarshal(t *testing.T) {
	for _, held := range []bool{false, true} {
		s := New(&Heap{})
		s.LeaseExists = func(int64) bool { return true }
		if held {
			s.SetValueLimit(1 << 20)
		}
		var changes []Change
		apply := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, s.Changes()...)
		}
		_, err := s.Put(&pb.PutRequest{Key: []byte("a"), Value: []byte("one")})
		apply(err)
		_, err = s.Put(&pb.PutRequest{Key: []byte("a"), Value: bytes.Repeat([]byte("v"), 300), Lease: 7})
		apply(err)
		_, err = s.Put(&pb.PutRequest{Key: []byte("a"), IgnoreValue: true})
		apply(err)
		_, err = s.Put(&pb.PutRequest{Key: []byte("b")})
		apply(err)
		_, err = s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a")})
		apply(err)
		_, err = s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("x")}}},
			{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("b")}}},
		}})
		apply(err)
		for i, c := range changes {
			for _, withPrev := range []bool{false, true} {
				want, err := proto.Marshal(event(t, c, withPrev))
				if err != nil {
					t.Fatal(err)
				}
				got, ok, err := c.AppendEvent([]byte("x"), withPrev)
				if err != nil || !ok {
					t.Fatalf("held %v, change %d: %v %v", held, i, ok, err)
				}
				if !bytes.Equal(got[1:], want) {
					t.Fatalf("held %v, change %d, prev %v:\n got %x\nwant %x", held, i, withPrev, got[1:], want)
				}
				if size := c.EventSize(withPrev); size != 1+protowire.SizeBytes(len(want)) {
					t.Fatalf("held %v, change %d, prev %v: EventSize %d for %d bytes", held, i, withPrev, size, len(want))
				}
			}
		}
	}
}

// coldValues has no value in memory.
type coldValues struct{ Heap }

func (coldValues) Read(Ref, *[]byte) ([]byte, bool, error) { return nil, false, nil }

// TestAppendEventOfAColdValue checks that an event whose value only the
// log has is left to Event, with nothing appended.
func TestAppendEventOfAColdValue(t *testing.T) {
	s := New(&coldValues{})
	put(t, s, "a", "one")
	got, ok, err := s.Changes()[0].AppendEvent([]byte("x"), false)
	if err != nil || ok || string(got) != "x" {
		t.Fatalf("got %q %v %v, want x false nil", got, ok, err)
	}
}
