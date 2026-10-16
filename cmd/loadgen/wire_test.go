package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestPaddedRevision(t *testing.T) {
	for _, v := range []int64{0, 1, 127, 128, 1 << 40, math.MaxInt64} {
		b := putRevision(nil, v)
		if len(b) != revisionVarintSz {
			t.Fatalf("%d encoded in %d bytes", v, len(b))
		}
		got, n := protowire.ConsumeVarint(b)
		if n != revisionVarintSz || int64(got) != v {
			t.Fatalf("%d decoded as %d from %d bytes", v, got, n)
		}
	}
}

// TestHeadersIndexed starts two calls on one connection: the first adds
// its fields to the table, as grpc-go's encoder does, and the second sends
// only references to them, which decode to the same headers.
func TestHeadersIndexed(t *testing.T) {
	c := &conn{addr: "h:1", nextID: 1, streams: map[uint32]*stream{}}
	tpl, err := newTemplate("/etcdserverpb.KV/Range", putRequest(4, 8), true)
	if err != nil {
		t.Fatal(err)
	}
	dec := hpack.NewDecoder(4096, nil)
	var blocks [][]byte
	for range 3 {
		c.out = c.out[:0]
		c.startStream(&stream{}, tpl)
		fr := http2.NewFramer(nil, bytes.NewReader(c.out))
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		block := bytes.Clone(f.(*http2.HeadersFrame).HeaderBlockFragment())
		hdrs, err := dec.DecodeFull(block)
		if err != nil {
			t.Fatal(err)
		}
		if len(hdrs) != 6 || hdrs[2].Value != "/etcdserverpb.KV/Range" || hdrs[3].Value != "h:1" {
			t.Fatalf("headers %v", hdrs)
		}
		blocks = append(blocks, block)
	}
	if len(blocks[1]) != 6 || !bytes.Equal(blocks[1], blocks[2]) {
		t.Fatalf("later blocks %x and %x, want 6 one-byte references", blocks[1], blocks[2])
	}
}

func TestCASTemplate(t *testing.T) {
	ks := makeKeys(3)
	tpl, err := newTemplate("/etcdserverpb.KV/Txn", casRequest(ks.size, 16), true)
	if err != nil {
		t.Fatal(err)
	}
	c := &conn{addr: "h:1", nextID: 7, streams: map[uint32]*stream{}}
	r := c.startStream(&stream{}, tpl)
	for _, off := range tpl.keys {
		copy(r[off:], ks.key(2))
	}
	copy(r[tpl.value:], "0123456789abcdef")
	patchRevision(r[tpl.rev:], 4242)

	fr := http2.NewFramer(nil, bytes.NewReader(c.out))
	f, err := fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	hf, ok := f.(*http2.HeadersFrame)
	if !ok || hf.StreamID != 7 || !hf.HeadersEnded() || hf.StreamEnded() {
		t.Fatalf("first frame %v", f)
	}
	hdrs, err := hpack.NewDecoder(4096, nil).DecodeFull(hf.HeaderBlockFragment())
	if err != nil {
		t.Fatal(err)
	}
	if hdrs[2].Name != ":path" || hdrs[2].Value != "/etcdserverpb.KV/Txn" {
		t.Fatalf("headers %v", hdrs)
	}
	f, err = fr.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	df, ok := f.(*http2.DataFrame)
	if !ok || df.StreamID != 7 || !df.StreamEnded() {
		t.Fatalf("second frame %v", f)
	}
	m := df.Data()[grpcPrefixLen:]
	if binary.BigEndian.Uint32(df.Data()[1:]) != uint32(len(m)) {
		t.Fatal("wrong gRPC message length")
	}

	// Walk the Txn: compare, success put, failure range.
	key := string(ks.key(2))
	cmp, _ := bytesField(m, txnCompare)
	target, _ := varintField(cmp, cmpTarget)
	ckey, _ := bytesField(cmp, cmpKey)
	rev, _ := varintField(cmp, cmpModRevision)
	if target != cmpTargetMod || string(ckey) != key || rev != 4242 {
		t.Fatalf("compare target %d key %q rev %d", target, ckey, rev)
	}
	succ, _ := bytesField(m, txnSuccess)
	put, _ := bytesField(succ, opPut)
	pkey, _ := bytesField(put, putKey)
	pval, _ := bytesField(put, putValue)
	if string(pkey) != key || string(pval) != "0123456789abcdef" {
		t.Fatalf("put key %q value %q", pkey, pval)
	}
	fail, _ := bytesField(m, txnFailure)
	rng, _ := bytesField(fail, opRange)
	rkey, _ := bytesField(rng, rangeKey)
	if string(rkey) != key {
		t.Fatalf("failure range key %q", rkey)
	}
}

func TestTooLargeRequest(t *testing.T) {
	if _, err := newTemplate("/p", putRequest(50, maxDataFrame), true); err == nil {
		t.Fatal("a request larger than one DATA frame was accepted")
	}
}

func TestPrefixEnd(t *testing.T) {
	for in, want := range map[string]string{"/a/": "/a0", "a\xff": "b", "\xff\xff": "\x00"} {
		if got := string(prefixEnd([]byte(in))); got != want {
			t.Errorf("prefixEnd(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKeys(t *testing.T) {
	const n = 200000
	ks := makeKeys(n)
	seen := make(map[string]bool, n)
	for i := range n {
		k := string(ks.key(i))
		if seen[k] {
			t.Fatalf("key %d %q repeats", i, k)
		}
		seen[k] = true
		if len(k) != ks.size || k[:len(keyPrefix)] != keyPrefix {
			t.Fatalf("key %d %q", i, k)
		}
	}
}
