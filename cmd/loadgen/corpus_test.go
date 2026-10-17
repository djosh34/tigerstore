package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestCorpusPutFrames(t *testing.T) {
	values := [][]byte{[]byte("k8s\x00metadata"), bytes.Repeat([]byte("managedFields"), 2600)}
	ks := makeKeys(10)
	for _, random := range []bool{false, true} {
		w, err := newCorpusPut(base{ks: ks}, values, random, 10)
		if err != nil {
			t.Fatal(err)
		}
		other, err := newCorpusPut(base{ks: ks}, values, random, 11)
		if err != nil {
			t.Fatal(err)
		}
		for i, tpl := range w.templates {
			framed := bytes.Clone(tpl.b)
			copy(framed[tpl.keys[0]:], ks.key(3))
			var body []byte
			for _, at := range tpl.frames {
				n := int(framed[at])<<16 | int(framed[at+1])<<8 | int(framed[at+2])
				body = append(body, framed[at+9:at+9+n]...)
			}
			if int(binary.BigEndian.Uint32(body[1:])) != len(body)-5 {
				t.Fatal("bad gRPC length")
			}
			var put pb.PutRequest
			if err := proto.Unmarshal(body[5:], &put); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(put.Key, ks.key(3)) || len(put.Value) != len(values[i]) {
				t.Fatal("key or value length changed")
			}
			if !random && !bytes.Equal(put.Value, values[i]) {
				t.Fatal("real value changed")
			}
			if random && bytes.Equal(tpl.b, other.templates[i].b) {
				t.Fatal("connections share random values")
			}
		}
	}
}
