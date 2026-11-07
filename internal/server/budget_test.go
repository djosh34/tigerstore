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

// TestRangeHeldBackBySlowClient asks for a Range of 48 MiB on a client
// that takes nothing past its first 64 KiB. While it waits, the process
// must hold about a piece of the answer, not all of it: the node builds
// the next piece only once the stream took the last. Then the client reads
// the rest, which must be the whole answer in one message (#505).
func TestRangeHeldBackBySlowClient(t *testing.T) {
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
	before := heapInUse()
	r := &pb.RangeRequest{Key: []byte("big/"), RangeEnd: []byte("big0"), Serializable: true}
	fr, closeConn := rawCall(t, c, 1, cert, "/etcdserverpb.KV/Range", r)
	defer closeConn()
	// Let the node and the writer go as far as they would.
	time.Sleep(time.Second)
	held := heapInUse() - before
	t.Logf("a Range of %d MiB holds %d KiB while its client takes nothing", keys*size>>20, held>>10)
	if held > 8<<20 {
		t.Fatalf("a Range of %d MiB holds %d MiB while its client takes nothing", keys*size>>20, held>>20)
	}
	msg := readAnswer(t, fr)
	if len(msg) < 5 || int(binary.BigEndian.Uint32(msg[1:5])) != len(msg)-5 {
		t.Fatalf("the answer is %d bytes, its prefix says %d", len(msg), binary.BigEndian.Uint32(msg[1:5]))
	}
	var resp pb.RangeResponse
	if err := proto.Unmarshal(msg[5:], &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != keys || resp.Count != keys {
		t.Fatalf("the answer holds %d keys, count %d, want %d", len(resp.Kvs), resp.Count, keys)
	}
	for i, kv := range resp.Kvs {
		if string(kv.Key) != fmt.Sprintf("big/%05d", i) || !bytes.Equal(kv.Value, value) {
			t.Fatalf("key %d of the answer is %q", i, kv.Key)
		}
	}
}
