package main

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	ew "go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func offlineFixture() *history.History {
	a := history.KeyValue{Key: "a", Value: "first", CreateRevision: 2, ModRevision: 2, Version: 1}
	b := history.KeyValue{Key: "b", Value: "second", CreateRevision: 3, ModRevision: 3, Version: 1}
	updated := a
	updated.Value = "updated"
	updated.ModRevision = 4
	updated.Version = 2
	h := &history.History{Target: 5}
	add := func(client int, q history.Request, p *history.Response) {
		i := int64(len(h.Ops))
		h.Ops = append(h.Ops, history.Op{Client: client, Call: i*10 + 1, Return: i*10 + 2, Req: q, Resp: p})
		if q.Mutates() {
			h.Log = append(h.Log, q)
		}
	}
	add(0, history.Request{Put: &history.PutRequest{Key: "a", Value: a.Value, PrevKV: true}}, &history.Response{Revision: 2, Put: &history.PutResponse{}})
	add(1, history.Request{Put: &history.PutRequest{Key: "b", Value: b.Value, PrevKV: true}}, &history.Response{Revision: 3, Put: &history.PutResponse{}})
	txn := func(n int64) *history.TxnRequest {
		return &history.TxnRequest{Compare: []history.Compare{{Key: "a", Target: history.TargetMod, Result: history.Equal, Number: n}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a", Value: updated.Value, PrevKV: true}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: "a"}}}}
	}
	add(2, history.Request{Txn: txn(0)}, &history.Response{Revision: 3, Txn: &history.TxnResponse{Results: []history.TxnOpResponse{{Range: &history.RangeResponse{KVs: []history.KeyValue{a}, Count: 1}}}}})
	add(0, history.Request{Txn: txn(2)}, &history.Response{Revision: 4, Txn: &history.TxnResponse{Succeeded: true, Results: []history.TxnOpResponse{{Put: &history.PutResponse{PrevKV: &a}}}}})
	add(1, history.Request{Delete: &history.DeleteRequest{Key: "a", End: "c", PrevKV: true}}, &history.Response{Revision: 5, Delete: &history.DeleteResponse{Deleted: 2, PrevKVs: []history.KeyValue{updated, b}}})
	for i := range 3 {
		add(20+i, history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: "a", Target: history.TargetVersion, Result: history.Equal, Number: -1}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: "a", Value: "never"}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: "a"}}}}}, &history.Response{Revision: 5, Txn: &history.TxnResponse{Results: []history.TxnOpResponse{{Range: &history.RangeResponse{}}}}})
	}
	add(2, history.Request{Range: &history.RangeRequest{Key: "\x00", End: history.AllKeys}}, &history.Response{Revision: 5, Range: &history.RangeResponse{}})
	for i := range 3 {
		h.Watches = append(h.Watches, history.Watch{Client: 100 + i, Req: history.WatchRequest{Key: "a", End: "c", StartRevision: 1, PrevKV: true, ProgressNotify: true}, Responses: []history.WatchResponse{
			{Time: 1, Revision: 1, Created: true},
			{Time: 12, Revision: 2, Events: []history.Event{{Type: history.EventPut, KV: a}}},
			{Time: 22, Revision: 3, Events: []history.Event{{Type: history.EventPut, KV: b}}},
			{Time: 42, Revision: 4, Events: []history.Event{{Type: history.EventPut, KV: updated, PrevKV: &a}}},
			{Time: 52, Revision: 5, Events: []history.Event{{Type: history.EventDelete, KV: history.KeyValue{Key: "a", ModRevision: 5}, PrevKV: &updated}, {Type: history.EventDelete, KV: history.KeyValue{Key: "b", ModRevision: 5}, PrevKV: &b}}},
			{Time: 62, Revision: 5, Progress: true},
		}})
	}
	return h
}

// One offline test. It never starts a server or injects a fault into a process.
func TestOfflineCLI(t *testing.T) {
	dir := t.TempDir()
	h := offlineFixture()
	input := filepath.Join(dir, "history.json")
	output := filepath.Join(dir, "checks.json")
	if err := save(input, h); err != nil {
		t.Fatal(err)
	}
	invoke := func(timeout string) error {
		original := os.Args
		defer func() { os.Args = original }()
		os.Args = []string{"client-tool", "check", "--history", input, "--out", output, "--checker-timeout", timeout}
		return runCLI()
	}
	if err := invoke("5s"); err != nil {
		t.Fatal(err)
	}
	assertReports := func(want bool) {
		var reports []checkerResult
		if err := load(output, &reports); err != nil {
			t.Fatal(err)
		}
		if len(reports) != 4 {
			t.Fatalf("got %d reports", len(reports))
		}
		for _, r := range reports {
			if r.Passed != want {
				t.Fatalf("%s passed=%v want%v: %s", r.Name, r.Passed, want, r.Error)
			}
		}
	}
	assertReports(true)
	for _, missing := range []string{"log", "target", "malformed"} {
		bad := offlineFixture()
		switch missing {
		case "log":
			bad.Log = nil
		case "target":
			bad.Target = 0
		case "malformed":
			bad.Ops[0].Resp = nil
		}
		if err := save(input, bad); err != nil {
			t.Fatal(err)
		}
		if err := invoke("5s"); err == nil {
			t.Fatalf("missing %s passed", missing)
		}
		assertReports(false)
	}
	// Extend a valid sequential fixture so even a fast machine exceeds 1ns.
	h = offlineFixture()
	for i := range 500 {
		h.Ops = append(h.Ops, history.Op{Client: 4, Call: int64(100 + i*10), Return: int64(101 + i*10), Req: history.Request{Range: &history.RangeRequest{Key: "\x00", End: history.AllKeys}}, Resp: &history.Response{Revision: 5, Range: &history.RangeResponse{}}})
	}
	if err := save(input, h); err != nil {
		t.Fatal(err)
	}
	if err := invoke("1ns"); err == nil {
		t.Fatal("bounded checker passed 1ns timeout")
	}
	var timed []checkerResult
	if err := load(output, &timed); err != nil {
		t.Fatal(err)
	}
	if len(timed) != 4 || timed[3].Name != "Linearizable" || timed[3].Passed || !(strings.Contains(timed[3].Error, "deadline") || strings.Contains(timed[3].Error, "timed out")) {
		t.Fatalf("missing timeout verdict: %+v", timed)
	}
	// Build real etcd WAL files offline, with an uncommitted mutation tail.
	walDir := filepath.Join(dir, "node", "member", "wal")
	w, err := ew.Create(zap.NewNop(), walDir, []byte("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(&pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("a"), Value: []byte("first"), PrevKv: true}})
	if err != nil {
		t.Fatal(err)
	}
	tail, err := proto.Marshal(&pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("tail"), Value: []byte("uncommitted")}})
	if err != nil {
		t.Fatal(err)
	}
	ents := []*raftpb.Entry{{Term: proto.Uint64(1), Index: proto.Uint64(1), Type: raftpb.EntryNormal.Enum(), Data: wire}, {Term: proto.Uint64(1), Index: proto.Uint64(2), Type: raftpb.EntryNormal.Enum(), Data: tail}}
	if err = w.Save(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}, ents); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	n, err := etcdLog(filepath.Join(dir, "node"))
	if err != nil {
		t.Fatal(err)
	}
	if err = validatePrefix(n); err != nil {
		t.Fatal(err)
	}
	prefix, err := committedPrefix([]recovered{n})
	if err != nil || len(prefix) != 1 {
		t.Fatalf("commit prefix: %d %v", len(prefix), err)
	}
	decoded, err := decodeEtcd(prefix[0].Data)
	if err != nil || len(decoded) != 1 || decoded[0].Put.PrevKV != true {
		t.Fatalf("etcd request: %+v %v", decoded, err)
	}
	// Shared committed indexes must agree; an uncommitted tail is irrelevant.
	other := recovered{commit: 1, entries: []*raftpb.Entry{{Term: proto.Uint64(2), Index: proto.Uint64(1), Type: raftpb.EntryNormal.Enum(), Data: wire}}}
	if _, err = committedPrefix([]recovered{n, other}); err == nil {
		t.Fatal("conflicting committed term accepted")
	}
	if err = validatePrefix(recovered{commit: 2, entries: ents[:1]}); err == nil {
		t.Fatal("commit past tail accepted")
	}
	// Independent Tiger framing preserves complete Txn and rejects trailing bytes.
	tx := &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 2}}}, Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("updated"), PrevKv: true}}}}, Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}}}
	body, err := proto.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	batch := binary.AppendUvarint(nil, 1)
	for _, v := range []uint64{1, 10, 20} {
		batch = binary.AppendUvarint(batch, v)
	}
	batch = append(batch, 3)
	batch = binary.AppendUvarint(batch, uint64(len(body)))
	batch = append(batch, body...)
	qs, err := decodeTigerBatch(batch)
	want, e := request(tx)
	if err != nil || e != nil || len(qs) != 1 || !reflect.DeepEqual(qs[0], want) {
		t.Fatalf("batch txn: %v %v", err, e)
	}
	if _, err = decodeTigerBatch(append(batch, 0)); err == nil {
		t.Fatal("trailing batch bytes accepted")
	}
	if err = campaignRequest(history.Request{Put: &history.PutRequest{Key: "a", Value: string([]byte{0xff})}}); err == nil {
		t.Fatal("non-UTF8 accepted")
	}
	if next := resumeRevision(1, history.WatchResponse{Revision: 100, Events: []history.Event{{KV: history.KeyValue{ModRevision: 4}}}}); next != 5 {
		t.Fatalf("event header advanced resume to%d", next)
	}
	if next := resumeRevision(5, history.WatchResponse{Revision: 100, Progress: true}); next != 101 {
		t.Fatal(next)
	}
	if transientWatch(errors.New("fixture")) {
		t.Fatal("unknown watch error treated transient")
	}
}
