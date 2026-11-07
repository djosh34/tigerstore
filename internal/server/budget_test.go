package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// heapInUse is the heap in use after a collection.
func heapInUse() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse)
}

// rawCall opens a TLS connection to node id that speaks HTTP/2 frames by
// hand, sends one gRPC call of path with req, and returns the framer. The
// client grants no more than the default windows of 64 KiB, so it takes
// nothing more until the test reads and credits it.
func rawCall(t *testing.T, c *cluster, id uint64, cert tls.Certificate, path string, req proto.Message) (*http2.Framer, func()) {
	t.Helper()
	tc, err := tls.Dial("tcp", c.running[id].addr, &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost", NextProtos: []string{"h2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(tc.SetDeadline(time.Now().Add(60 * time.Second)))
	_, err = io.WriteString(tc, http2.ClientPreface)
	must(err)
	fr := http2.NewFramer(tc, tc)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	must(fr.WriteSettings())
	var hb bytes.Buffer
	enc := hpack.NewEncoder(&hb)
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: path},
		{Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		must(enc.WriteField(f))
	}
	must(fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true}))
	b, err := proto.Marshal(req)
	must(err)
	must(fr.WriteData(1, true, append(binary.BigEndian.AppendUint32([]byte{0}, uint32(len(b))), b...)))
	return fr, func() { _ = tc.Close() }
}

// readAnswer reads stream 1 to its end, crediting every byte back, and
// returns the gRPC message it carried.
func readAnswer(t *testing.T, fr *http2.Framer) []byte {
	t.Helper()
	var body []byte
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("after %d bytes of the answer: %v", len(body), err)
		}
		switch f := f.(type) {
		case *http2.DataFrame:
			body = append(body, f.Data()...)
			if n := len(f.Data()); n > 0 {
				if err := fr.WriteWindowUpdate(0, uint32(n)); err != nil {
					t.Fatal(err)
				}
				if err := fr.WriteWindowUpdate(1, uint32(n)); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.MetaHeadersFrame:
			if f.StreamEnded() {
				if st := f.PseudoValue("status"); st == "" {
					for _, h := range f.Fields {
						if h.Name == "grpc-status" && h.Value != "0" {
							t.Fatalf("the call failed: %v", f.Fields)
						}
					}
				}
				return body
			}
		case *http2.RSTStreamFrame:
			t.Fatalf("the stream was reset with %v after %d bytes", f.ErrCode, len(body))
		}
	}
}

// TestAnswersHeldBackBySlowClient asks for answers of 48 MiB on a client
// that takes nothing past its first 64 KiB: a Range, a Range inside a
// Txn, and a DeleteRange with its previous values. While the client
// waits, the process must hold about a piece of the answer, not all of
// it: the node builds the next piece only once the stream took the last.
// Then the client reads the rest, which must be the whole answer in one
// message (#505).
func TestAnswersHeldBackBySlowClient(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 96)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	const keys, size = 3072, 16 << 10
	value := bytes.Repeat([]byte("v"), size)
	for batch := range keys / 64 {
		var ops []*pb.RequestOp
		for j := range 64 {
			ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{
				Key: fmt.Appendf(nil, "big/%05d", batch*64+j), Value: value,
			}}})
		}
		retry(t, func(ctx context.Context) (*pb.TxnResponse, error) {
			return kv.Txn(ctx, &pb.TxnRequest{Success: ops})
		})
	}
	all := &pb.RangeRequest{Key: []byte("big/"), RangeEnd: []byte("big0"), Serializable: true}
	// heldBack sends req on a client that takes nothing, checks that the
	// process then holds at most limit bytes more, and reads the answer.
	heldBack := func(path string, req, resp proto.Message, limit int64) {
		t.Helper()
		before := heapInUse()
		fr, closeConn := rawCall(t, c, 1, cert, path, req)
		defer closeConn()
		// Let the node and the writer go as far as they would.
		time.Sleep(time.Second)
		held := heapInUse() - before
		t.Logf("%s of %d MiB holds %d KiB while its client takes nothing", path, keys*size>>20, held>>10)
		if held > limit {
			t.Fatalf("%s of %d MiB holds %d MiB while its client takes nothing", path, keys*size>>20, held>>20)
		}
		msg := readAnswer(t, fr)
		if len(msg) < 5 || int(binary.BigEndian.Uint32(msg[1:5])) != len(msg)-5 {
			t.Fatalf("%s: the answer is %d bytes, its prefix says %d", path, len(msg), binary.BigEndian.Uint32(msg[1:5]))
		}
		if err := proto.Unmarshal(msg[5:], resp); err != nil {
			t.Fatal(err)
		}
	}
	check := func(what string, kvs []*mvccpb.KeyValue) {
		t.Helper()
		if len(kvs) != keys {
			t.Fatalf("%s holds %d keys, want %d", what, len(kvs), keys)
		}
		for i, kv := range kvs {
			if string(kv.Key) != fmt.Sprintf("big/%05d", i) || !bytes.Equal(kv.Value, value) {
				t.Fatalf("key %d of %s is %q", i, what, kv.Key)
			}
		}
	}
	var r pb.RangeResponse
	heldBack("/etcdserverpb.KV/Range", all, &r, 8<<20)
	check("the Range", r.Kvs)
	var txn pb.TxnResponse
	heldBack("/etcdserverpb.KV/Txn", &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("big/"), RangeEnd: []byte("big0")}}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("after"), Value: []byte("1")}}},
	}}, &txn, 8<<20)
	if len(txn.Responses) != 2 || txn.Responses[1].GetResponsePut() == nil {
		t.Fatalf("the Txn answered %d ops", len(txn.Responses))
	}
	check("the Txn's range", txn.Responses[0].GetResponseRange().Kvs)
	// A watch from revision 1 catches up over the whole history, and one
	// with prev_kv from now gets the delete below: 3,072 events of one
	// revision with 48 MiB of previous values. Neither client reads until
	// the delete is done. A stream holds at most its room of 8 MiB, and
	// the server's queue 4 MiB more.
	before := heapInUse()
	watch := func(w *pb.WatchCreateRequest) (*http2.Framer, func()) {
		return rawCall(t, c, 1, cert, "/etcdserverpb.Watch/Watch", &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: w}})
	}
	catchUp, closeCatchUp := watch(&pb.WatchCreateRequest{Key: []byte("big/"), RangeEnd: []byte("big0"), StartRevision: 1})
	defer closeCatchUp()
	big, closeBig := watch(&pb.WatchCreateRequest{Key: []byte("big/"), RangeEnd: []byte("big0"), PrevKv: true})
	defer closeBig()
	time.Sleep(time.Second)
	var del pb.DeleteRangeResponse
	// The watcher with prev_kv takes the delete's events, as much as its
	// stream has room for: 8 MiB, and 4 MiB in the server's queue.
	heldBack("/etcdserverpb.KV/DeleteRange", &pb.DeleteRangeRequest{Key: []byte("big/"), RangeEnd: []byte("big0"), PrevKv: true}, &del, 8<<20+12<<20)
	if del.Deleted != keys {
		t.Fatalf("the DeleteRange deleted %d keys, want %d", del.Deleted, keys)
	}
	check("the DeleteRange's previous values", del.PrevKvs)
	time.Sleep(time.Second)
	held := heapInUse() - before
	t.Logf("two watches of 48 MiB each hold %d KiB while their clients take nothing", held>>10)
	if held > 2*(12<<20)+8<<20 {
		t.Fatalf("two watches of 48 MiB each hold %d MiB while their clients take nothing", held>>20)
	}
	var puts []*mvccpb.KeyValue
	readWatch(t, catchUp, func(r *pb.WatchResponse) bool {
		for _, ev := range r.Events {
			if ev.Type == mvccpb.PUT {
				puts = append(puts, ev.Kv)
			}
		}
		return len(puts) == keys
	})
	check("the catch-up", puts)
	readWatch(t, big, func(r *pb.WatchResponse) bool {
		if len(r.Events) == 0 {
			return false
		}
		if len(r.Events) != keys {
			t.Fatalf("the revision of %d deletes came in a response of %d events", keys, len(r.Events))
		}
		var prev []*mvccpb.KeyValue
		for _, ev := range r.Events {
			prev = append(prev, ev.PrevKv)
		}
		check("the deletes' previous values", prev)
		return true
	})
}

// readWatch reads the gRPC messages of stream 1, crediting every byte
// back, until done says so for one.
func readWatch(t *testing.T, fr *http2.Framer, done func(*pb.WatchResponse) bool) {
	t.Helper()
	var buf []byte
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("reading a watch: %v", err)
		}
		switch f := f.(type) {
		case *http2.DataFrame:
			buf = append(buf, f.Data()...)
			if n := len(f.Data()); n > 0 {
				if err := fr.WriteWindowUpdate(0, uint32(n)); err != nil {
					t.Fatal(err)
				}
				if err := fr.WriteWindowUpdate(1, uint32(n)); err != nil {
					t.Fatal(err)
				}
			}
		case *http2.MetaHeadersFrame:
			if f.StreamEnded() {
				t.Fatalf("the watch ended: %v", f.Fields)
			}
		case *http2.RSTStreamFrame:
			t.Fatalf("the watch was reset with %v", f.ErrCode)
		}
		for len(buf) >= 5 && len(buf) >= 5+int(binary.BigEndian.Uint32(buf[1:5])) {
			n := int(binary.BigEndian.Uint32(buf[1:5]))
			var r pb.WatchResponse
			if err := proto.Unmarshal(buf[5:5+n], &r); err != nil {
				t.Fatal(err)
			}
			buf = buf[5+n:]
			if done(&r) {
				return
			}
		}
	}
}
