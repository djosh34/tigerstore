package server

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// TestPutResponseEncoding compares the hand-written PutResponse encoding
// with protobuf's.
func TestPutResponseEncoding(t *testing.T) {
	prev := &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("old"), CreateRevision: 2, ModRevision: 5, Version: 3, Lease: 7}
	for _, c := range []struct {
		cluster, member, term uint64
		rev                   int64
		prev                  *mvccpb.KeyValue
	}{
		{0x7469676572, 1, 2, 10, nil},
		{1<<64 - 1, 1<<63 + 5, 1 << 40, 1<<63 - 1, prev},
		{0, 0, 0, 0, nil},
		{3, 2, 0, 1, prev},
	} {
		got, err := appendPutResponse(nil, c.cluster, c.member, c.rev, c.term, c.prev)
		if err != nil {
			t.Fatal(err)
		}
		if c.prev == nil && len(got) > putResponseSize {
			t.Fatalf("%d bytes, above putResponseSize", len(got))
		}
		want, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb.PutResponse{
			Header: &pb.ResponseHeader{ClusterId: c.cluster, MemberId: c.member, Revision: c.rev, RaftTerm: c.term},
			PrevKv: c.prev,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%+v:\n got %x\nwant %x", c, got, want)
		}
	}
}

// TestHeaderDecodeAllocs checks that a gRPC request's header block, once
// its fields are in the HPACK dynamic table, decodes without allocating.
func TestHeaderDecodeAllocs(t *testing.T) {
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"},
		{Name: ":authority", Value: "127.0.0.1:2379"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "user-agent", Value: "grpc-go/1.84.0"},
		{Name: "te", Value: "trailers"},
	}
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	encode := func() []byte {
		block.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		return bytes.Clone(block.Bytes())
	}
	first := encode()
	// The same fields again are all indexed.
	again := encode()
	c := &conn{}
	c.hdec = hpack.NewDecoder(4096, c.onField)
	decode := func(b []byte) {
		c.hdr = reqHeaders{}
		if _, err := c.hdec.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := c.hdec.Close(); err != nil {
			t.Fatal(err)
		}
	}
	decode(first)
	if c.hdr.bad || c.hdr.path != "/etcdserverpb.KV/Put" || c.hdr.contentType != "application/grpc" || c.hdr.method != "POST" {
		t.Fatalf("decoded %+v", c.hdr)
	}
	if n := testing.AllocsPerRun(100, func() { decode(again) }); n != 0 {
		t.Fatalf("%v allocations per header block", n)
	}
}

// TestHeaderBlockRules feeds header blocks through headerFragment: a
// block that breaks a rule resets its stream and leaves the HPACK state in
// step for the next block, which reuses the dynamic table; a block that
// goes on after breaking a rule, or could decode past the limit, ends the
// connection.
func TestHeaderBlockRules(t *testing.T) {
	c := testConn()
	c.hdec = hpack.NewDecoder(4096, c.onField)
	c.hdec.SetMaxStringLength(maxHeaderList)
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	block := func(fields ...hpack.HeaderField) []byte {
		buf.Reset()
		for _, f := range fields {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		return bytes.Clone(buf.Bytes())
	}
	put := []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":path", Value: "/etcdserverpb.KV/Put"},
		{Name: "content-type", Value: "application/grpc"},
	}
	send := func(id uint32, b []byte, last bool) error {
		if c.hdr.id != id {
			c.hdr = reqHeaders{id: id}
		}
		return c.headerFragment(b, last)
	}
	// Stream 1 has an uppercase name; its fields still go into the table.
	if err := send(1, block(append(put, hpack.HeaderField{Name: "X-Upper", Value: "v"})...), true); err != nil {
		t.Fatal(err)
	}
	if c.streams[1] != nil {
		t.Fatal("a bad block opened a stream")
	}
	// Stream 3 reuses the table entries that stream 1 added.
	if err := send(3, block(put...), true); err != nil {
		t.Fatal(err)
	}
	if st := c.streams[3]; st == nil || st.rpc != methods["/etcdserverpb.KV/Put"] {
		t.Fatalf("stream 3 after a bad block: %+v", st)
	}
	// A second :path breaks the block.
	twoPaths := []hpack.HeaderField{put[0], put[1], {Name: ":path", Value: "/x"}, put[2]}
	if err := send(5, block(twoPaths...), true); err != nil || c.streams[5] != nil {
		t.Fatalf("a block with two paths: %v, stream %v", err, c.streams[5])
	}
	// Bad trailers on the open stream 3 reset only that stream.
	if err := send(3, block(hpack.HeaderField{Name: "X-Upper", Value: "v"}), true); err != nil || c.streams[3] != nil {
		t.Fatalf("bad trailers on an open stream: %v, stream %v", err, c.streams[3])
	}
	// A bad block on an even stream ends the connection, as a good one
	// would.
	if err := send(6, block(append(put, hpack.HeaderField{Name: "X-Upper", Value: "v"})...), true); err == nil {
		t.Fatal("a bad block on an even stream was taken")
	}
	// A block that goes on after breaking a rule ends the connection.
	if err := send(7, block(hpack.HeaderField{Name: "X-Upper", Value: "v"}), false); err != nil {
		t.Fatal(err)
	}
	if err := send(7, block(put...), true); err == nil {
		t.Fatal("a continuation after a bad field was decoded")
	}
	// So does a fragment that could decode past the limit.
	c.hdr = reqHeaders{id: 9}
	if err := c.headerFragment(make([]byte, 2*maxHeaderList+1), false); err == nil {
		t.Fatal("an oversized fragment was decoded")
	}
}
