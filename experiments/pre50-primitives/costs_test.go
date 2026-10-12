package main

import (
	"bytes"
	"io"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestTLSRecordAdapter(t *testing.T) {
	dir := t.TempDir()
	if err := genCert(dir); err != nil {
		t.Fatal(err)
	}
	c, s, mc, ms, err := tlsPair(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Conn.Close()
	defer ms.Conn.Close()
	var wire bytes.Buffer
	mc.writer = &wire
	want := bytes.Repeat([]byte{0x73}, 1024)
	for i := 0; i < 3; i++ {
		n, e := c.Write(want)
		if e != nil || n != len(want) {
			t.Fatalf("write %d %v", n, e)
		}
	}
	if wire.Len() != 3*1046 {
		t.Fatalf("record size %d", wire.Len())
	}
	ms.reader = bytes.NewReader(wire.Bytes())
	got := make([]byte, 1024)
	for i := 0; i < 3; i++ {
		if _, e := io.ReadFull(s, got); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("bad plaintext")
		}
	}
}
func TestMessagesAndStockAppend(t *testing.T) {
	request := &pb.PutRequest{Key: []byte("/registry/pods/ns/pre50-00000001"), Value: bytes.Repeat([]byte{0x61}, 1024)}
	b, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.PutRequest
	if err = proto.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(request, &decoded) {
		t.Fatal("bad request")
	}
	response := &pb.PutResponse{Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 2, Revision: 12345, RaftTerm: 3}}
	b, err = proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) >= 32 {
		t.Fatalf("unexpectedly large response %d", len(b))
	}
	storage := raft.NewMemoryStorage()
	for i := 1; i <= 10; i++ {
		e := &raftpb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(uint64(i)), Type: raftpb.EntryNormal.Enum(), Data: request.Value}
		if err = storage.Append([]*raftpb.Entry{e}); err != nil {
			t.Fatal(err)
		}
	}
	last, err := storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if last != 10 {
		t.Fatalf("last index %d", last)
	}
}
func TestFrameRoundTrip(t *testing.T) {
	var wire bytes.Buffer
	want := bytes.Repeat([]byte{1}, 1035)
	if err := sendFrame(&wire, want); err != nil {
		t.Fatal(err)
	}
	got, err := receiveFrame(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("frame mismatch")
	}
}
