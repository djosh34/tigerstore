package server

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	c := &conn{metrics: newConnMetrics()}
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
	c := testConn(t)
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
		first := c.hdr.id != id
		if first {
			c.hdr = reqHeaders{id: id}
		}
		return c.headerFragment(b, first, last)
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
	if err := c.headerFragment(make([]byte, 2*maxHeaderList+1), true, false); err == nil {
		t.Fatal("an oversized fragment was decoded")
	}
}

// TestRangeReplyEncoding compares the hand-written RangeResponse with
// protobuf's encoding of the same answer on random results.
func TestRangeReplyEncoding(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		res := mvcc.RangeResult{Rev: r.Int64N(1 << 40), Count: r.Int64N(5000) - 1, More: r.IntN(2) == 0}
		for range r.IntN(20) {
			kv := mvcc.KV{
				Key: bytes.Repeat([]byte{'k'}, r.IntN(3)), Value: bytes.Repeat([]byte{'v'}, r.IntN(300)),
				CreateRevision: r.Int64N(1 << 33), ModRevision: r.Int64N(1 << 33), Version: r.Int64N(4),
				Lease: r.Int64N(5) - 2,
			}
			res.KVs = append(res.KVs, kv)
			res.Fields += int64(kv.FieldSize())
		}
		cluster, member, term := uint64(i), r.Uint64(), r.Uint64N(3)
		got, err := appendRangeReply(cluster, member, term, &res)
		if err != nil {
			t.Fatal(err)
		}
		resp := res.Response()
		resp.Header.ClusterId, resp.Header.MemberId, resp.Header.RaftTerm = cluster, member, term
		body, err := proto.MarshalOptions{Deterministic: true}.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		want := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(body)))
		want = append(want, body...)
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d:\n got %x\nwant %x", i, got, want)
		}
		if size := rangeSize(cluster, member, term, &res); size != int64(len(got)-5) {
			t.Fatalf("case %d: sized %d, encoded %d", i, size, len(got)-5)
		}
	}
}

// TestRangeReplyTooLarge checks that an answer over gRPC's 2 GiB message
// limit fails as grpc-go fails it, before anything is allocated.
func TestRangeReplyTooLarge(t *testing.T) {
	v := make([]byte, 1<<20)
	res := mvcc.RangeResult{KVs: make([]mvcc.KV, 2049)}
	for i := range res.KVs {
		res.KVs[i] = mvcc.KV{Key: []byte("k"), Value: v}
		res.Fields += int64(res.KVs[i].FieldSize())
	}
	if _, err := appendRangeReply(1, 2, 3, &res); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("an answer over 2 GiB: %v", err)
	}
}

// TestHeaderCacheTakesWholeBlocks checks that the cache of decoded header
// blocks answers only for a block in one HEADERS frame: bytes it holds,
// as the last fragment of a block that began with part of a field, are
// the rest of that field to the decoder.
func TestHeaderCacheTakesWholeBlocks(t *testing.T) {
	c := testConn(t)
	c.hdec = hpack.NewDecoder(4096, c.onField)
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	block := func() []byte {
		buf.Reset()
		for _, f := range []hpack.HeaderField{
			{Name: ":method", Value: "POST"},
			{Name: ":path", Value: "/etcdserverpb.KV/Put"},
			{Name: "content-type", Value: "application/grpc"},
		} {
			if err := enc.WriteField(f); err != nil {
				t.Fatal(err)
			}
		}
		return bytes.Clone(buf.Bytes())
	}
	send := func(id uint32, b []byte, first, last bool) error {
		if first {
			c.hdr = reqHeaders{id: id}
		}
		return c.headerFragment(b, first, last)
	}
	if err := send(1, block(), true, true); err != nil {
		t.Fatal(err)
	}
	indexed := block()
	if err := send(3, indexed, true, true); err != nil || c.streams[3] == nil {
		t.Fatalf("a block of indexed fields: %v", err)
	}
	if _, ok := c.blocks[string(indexed)]; !ok {
		t.Fatal("a block of indexed fields was not cached")
	}
	// A table size update of 0 empties the table that the cached bytes
	// index (#554); part of a field makes them its rest.
	for _, tc := range []struct {
		id   uint32
		head []byte
	}{{5, []byte{0x20}}, {7, []byte{0x40}}} {
		if _, ok := c.blocks[string(indexed)]; !ok {
			t.Fatalf("stream %d: the indexed block is not cached to test against", tc.id)
		}
		if err := send(tc.id, tc.head, true, false); err != nil {
			t.Fatal(err)
		}
		if err := send(tc.id, indexed, false, true); err == nil && c.streams[tc.id] != nil {
			t.Fatalf("stream %d: the last fragment of a longer block was answered from the cache", tc.id)
		}
	}
}
