package main

import (
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestInternalRaftBoundaryNotPayloadBoundary(t *testing.T) {
	key := []byte("/alloc50/large")
	value := make([]byte, limit)
	n := limit
	for raftSize(key, value[:n]) > limit {
		n--
	}
	if n >= limit || raftSize(key, value[:n]) != limit || raftSize(key, value[:n+1]) != limit+1 {
		t.Fatalf("bad boundary n=%d sizes=%d/%d", n, raftSize(key, value[:n]), raftSize(key, value[:n+1]))
	}
	request := &pb.PutRequest{Key: key, Value: value[:n]}
	if proto.Size(request) >= limit {
		t.Fatal("forgot InternalRaftRequest overhead")
	}
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(pb.PutRequest)
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Value, request.Value) {
		t.Fatal("boundary payload changed")
	}
}

func TestSlowPutsFitAdvertisedCredit(t *testing.T) {
	wire, err := proto.Marshal(&pb.PutRequest{Key: []byte("/alloc50/slow"), Value: valueOfSize(1024)})
	if err != nil {
		t.Fatal(err)
	}
	if 4096*(5+len(wire)) > 16<<20 {
		t.Fatal("slow workload exceeds server connection credit")
	}
	if 5+len(wire) > 4<<20 {
		t.Fatal("slow request exceeds server stream credit")
	}
	if bytes.Equal(valueOfSize(1024), make([]byte, 1024)) {
		t.Fatal("value fixture is empty")
	}
}
