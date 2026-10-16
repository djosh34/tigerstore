package core

import (
	"bytes"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// FuzzDecodeAlias decodes any bytes as each request that DecodeAlias has a
// written-out decoder for, and requires what proto.Unmarshal gives: the
// same message, or an error from both.
func FuzzDecodeAlias(f *testing.F) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 8 {
		b, err := proto.Marshal(randomTxn(r, 0))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	for _, m := range []proto.Message{
		&pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), PrevKv: true},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, Limit: 3, Revision: 7, KeysOnly: true, MinModRevision: 2},
	} {
		b, err := proto.Marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, newMsg := range []func() proto.Message{
			func() proto.Message { return &pb.TxnRequest{} },
			func() proto.Message { return &pb.DeleteRangeRequest{} },
			func() proto.Message { return &pb.RangeRequest{} },
		} {
			want, got := newMsg(), newMsg()
			wantErr := proto.Unmarshal(b, want)
			err := DecodeAlias(b, got)
			if (err == nil) != (wantErr == nil) {
				t.Fatalf("%T: DecodeAlias error %v, protobuf error %v", got, err, wantErr)
			}
			if err == nil && !proto.Equal(got, want) {
				t.Fatalf("%T:\n got %v\nwant %v", got, got, want)
			}
		}
	})
}

// FuzzParsePut parses any bytes as a PutRequest and requires the fields,
// the size and the error that proto.Unmarshal gives.
func FuzzParsePut(f *testing.F) {
	for _, p := range []*pb.PutRequest{
		{Key: []byte("k"), Value: []byte("value"), Lease: -3, PrevKv: true, IgnoreValue: true, IgnoreLease: true},
		{Key: []byte("/registry/pods/default/a"), Value: bytes.Repeat([]byte{0xff}, 300)},
	} {
		b, err := proto.Marshal(p)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var want pb.PutRequest
		wantErr := proto.Unmarshal(b, &want)
		got, err := ParsePut(b)
		if (err == nil) != (wantErr == nil) {
			t.Fatalf("ParsePut error %v, protobuf error %v", err, wantErr)
		}
		if err != nil {
			return
		}
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Lease != want.Lease ||
			got.PrevKv != want.PrevKv || got.IgnoreValue != want.IgnoreValue || got.IgnoreLease != want.IgnoreLease {
			t.Fatalf("got %+v, want %v", got.PutOp, &want)
		}
		if got.size() != proto.Size(&want) {
			t.Fatalf("size %d, protobuf marshals %d bytes", got.size(), proto.Size(&want))
		}
	})
}

// FuzzReceiveSnapshot feeds any bytes to a follower's snapshot receiver as
// the stream of the snapshot at index and term, into an empty log. It
// must refuse or take the stream without a crash, and a log it took must
// open again. The corpus in testdata/fuzz holds whole streams.
func FuzzReceiveSnapshot(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte, index, term uint64) {
		disk := simfs.New()
		w, _, err := wal.Open(disk)
		if err != nil {
			t.Fatal(err)
		}
		in := &Install{From: 1, Index: index, Term: term}
		if err := ReceiveSnapshot(bytes.NewReader(b), &walSink{w: w}, w, in); err != nil {
			return
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := wal.Open(disk.Durable()); err != nil {
			t.Fatalf("the log of a stream the receiver took does not open: %v", err)
		}
	})
}
