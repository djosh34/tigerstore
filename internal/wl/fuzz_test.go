package wl

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"
)

// h2Request is the frames, after the preface and SETTINGS, of a client
// that makes one unary call of method with request m.
func h2Request(method string, m proto.Message) []byte {
	var buf, hb bytes.Buffer
	fr := http2.NewFramer(&buf, nil)
	enc := hpack.NewEncoder(&hb)
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: method},
		{Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		_ = enc.WriteField(f)
	}
	_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: hb.Bytes(), EndHeaders: true})
	b, _ := proto.Marshal(m)
	msg := append([]byte{0, byte(len(b) >> 24), byte(len(b) >> 16), byte(len(b) >> 8), byte(len(b))}, b...)
	_ = fr.WriteData(1, true, msg)
	_ = fr.WritePing(false, [8]byte{1})
	_ = fr.WriteWindowUpdate(0, 1000)
	return buf.Bytes()
}

// FuzzHTTP2 sends any bytes as frames on a client connection of node 1,
// after the preface and a SETTINGS frame, then closes its side. The node
// must answer and close the connection, or reset it, within 5 s, without
// stopping, and keep serving: the cluster's own check at the end fails a
// node that stopped on its own.
func FuzzHTTP2(f *testing.F) {
	f.Add(h2Request("/etcdserverpb.KV/Range", &pb.RangeRequest{Key: []byte("a")}))
	f.Add(h2Request("/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}))
	f.Add(h2Request("/etcdserverpb.KV/Txn", &pb.TxnRequest{Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b")}}}}}))
	f.Add(h2Request("/etcdserverpb.Watch/Watch", &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("a")}}}))
	f.Add(h2Request("/etcdserverpb.Maintenance/Status", &pb.StatusRequest{}))
	var settings bytes.Buffer
	if err := http2.NewFramer(&settings, nil).WriteSettings(); err != nil {
		f.Fatal(err)
	}
	tc := newTestCluster(f)
	_ = tc.client(f) // the cluster takes a write
	addr := tc.endpoints[0]
	f.Fuzz(func(t *testing.T, b []byte) {
		conn, err := dialRaw(addr, tc.clientTLS())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck // closing a fuzz connection
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		in := append([]byte(http2.ClientPreface), settings.Bytes()...)
		if _, err := conn.Write(append(in, b...)); err != nil {
			// The node may reset a connection it refuses, but not stop
			// reading.
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("the node read nothing for 5 s: %v", err)
			}
			return
		}
		_ = conn.CloseWrite()
		if _, err := io.Copy(io.Discard, conn); errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("the node neither answered and closed the connection nor reset it within 5 s")
		}
	})
}
