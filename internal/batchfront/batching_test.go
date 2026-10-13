package batchfront

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

func TestBufferedRequestsAdmittedTogetherAndFramerDataOwned(t *testing.T) {
	n := startNode(t)
	c, peer := testConn(t)
	c.s.n = n
	var b bytes.Buffer
	b.WriteString(http2.ClientPreface)
	f := http2.NewFramer(&b, nil)
	const count = 8
	for i := range count {
		id := uint32(2*i + 1)
		h, err := headerBlock(hpack.HeaderField{Name: ":path", Value: "/etcdserverpb.KV/Put"})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: h, EndHeaders: true}); err != nil {
			t.Fatal(err)
		}
		payload, err := proto.Marshal(&pb.PutRequest{Key: []byte(strconv.Itoa(i)), Value: bytes.Repeat([]byte{byte(i + 1)}, 100)})
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 5+len(payload))
		binary.BigEndian.PutUint32(body[1:5], uint32(len(payload)))
		copy(body[5:], payload)
		if err := f.WriteData(id, true, body); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- c.readLoop() }()
	if _, err := peer.Write(b.Bytes()); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for c.s.Stats().AdmissionSends == 0 {
		if time.Now().After(until) {
			t.Fatal("reader did not flush before blocking on next frame")
		}
		time.Sleep(time.Millisecond)
	}
	st := c.s.Stats()
	if st.Commands != count || st.AdmissionSends != 1 {
		t.Fatalf("buffered batch not combined: %+v", st)
	}
	var completed int
	for completed < count {
		select {
		case batch := <-c.out:
			completed += len(batch)
		case <-time.After(time.Second):
			t.Fatal("missing completion")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := range count {
		r, err := n.Do(ctx, &pb.RangeRequest{Key: []byte(strconv.Itoa(i))})
		if err != nil {
			t.Fatal(err)
		}
		kvs := r.(*pb.RangeResponse).Kvs
		if len(kvs) != 1 || !bytes.Equal(kvs[0].Value, bytes.Repeat([]byte{byte(i + 1)}, 100)) {
			t.Fatalf("Framer reuse changed key %d: %v", i, kvs)
		}
	}
	c.close()
	<-done
}
