package server

import (
	"bytes"
	"testing"

	"golang.org/x/net/http2/hpack"
)

// TestOnlyIndexed checks which header blocks hold only indexed fields,
// and that such a block decodes to the same fields again while no other
// block changed the tables.
func TestOnlyIndexed(t *testing.T) {
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
	path := hpack.HeaderField{Name: ":path", Value: "/etcdserverpb.KV/Txn"}
	method := hpack.HeaderField{Name: ":method", Value: "POST"}
	first := block(method, path)
	again := block(method, path)
	timeout := block(method, path, hpack.HeaderField{Name: "grpc-timeout", Value: "5S", Sensitive: true})
	for _, c := range []struct {
		b    []byte
		want bool
	}{
		{first, false},              // :path goes into the table
		{again, true},               // both fields are now indexed
		{timeout, false},            // a literal never indexed
		{[]byte{0xff, 0x80}, false}, // an integer cut short
		{[]byte{0xff, 0x80, 0x01}, true},
		{[]byte{0x3f, 0xe1, 0x1f}, false}, // a table size update
	} {
		if got := onlyIndexed(c.b); got != c.want {
			t.Errorf("% x: onlyIndexed %v, want %v", c.b, got, c.want)
		}
	}
	var fields []string
	dec := hpack.NewDecoder(4096, func(f hpack.HeaderField) { fields = append(fields, f.Name+"="+f.Value) })
	for _, b := range [][]byte{first, again, again} {
		fields = nil
		if _, err := dec.Write(b); err != nil {
			t.Fatal(err)
		}
		if len(fields) != 2 || fields[1] != ":path=/etcdserverpb.KV/Txn" {
			t.Fatalf("decoded %q", fields)
		}
	}
}
