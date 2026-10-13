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

func TestCASTemplate(t *testing.T) {
	ks := makeKeys(3)
	tpl, err := newTemplate("h:1", "/etcdserverpb.KV/Txn", casRequest(ks.size, 16), true)
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.Clone(tpl.b)
	binary.BigEndian.PutUint32(r[5:], 7)
	binary.BigEndian.PutUint32(r[tpl.data+5:], 7)
	for _, off := range tpl.keys {
		copy(r[off:], ks.key(2))
	}
	copy(r[tpl.value:], "0123456789abcdef")
	patchRevision(r[tpl.rev:], 4242)

	fr := http2.NewFramer(nil, bytes.NewReader(r))
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
	if _, err := newTemplate("h:1", "/p", putRequest(50, maxDataFrame), true); err == nil {
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
