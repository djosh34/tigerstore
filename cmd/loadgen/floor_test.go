package main

import (
	"bytes"
	"github.com/djosh34/tigerstore/internal/floor"
	"testing"
)

func TestFloorWire(t *testing.T) {
	for _, txn := range []bool{false, true} {
		m := putRequest(len(floor.Key(0)), 1024)
		path := "/etcdserverpb.KV/Put"
		if txn {
			m = casRequest(len(floor.Key(0)), 1024)
			path = "/etcdserverpb.KV/Txn"
		}
		for _, at := range m.keys {
			copy(m.b[at:], floor.Key(0))
		}
		if txn {
			patchRevision(m.b[m.rev:], 1)
		}
		if !bytes.Equal(m.b, floor.Wire(txn)) {
			t.Fatalf("fixture mismatch txn=%v", txn)
		}
		c := &conn{addr: "127.0.0.1:20010"}
		c.headerBlock(path)
		h := c.headerBlock(path)
		t.Logf("txn=%v protobuf=%d HPACK=%d H2_request=%d", txn, len(m.b), len(h), len(m.b)+5+18+len(h))
	}
}
