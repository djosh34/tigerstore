package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/server"
)

// alone is the transport of a one-node cluster: it has no peers.
type alone struct{}

func (alone) Send(*raftpb.Message) {}
func (alone) Fetch(context.Context, uint64, uint64, uint64) (io.ReadCloser, error) {
	return nil, errors.New("no peers")
}

func (alone) Call(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("no peers")
}
func (alone) SetPeers(map[uint64]string, []uint64) {}
func (alone) ActiveSince(uint64) time.Time         { return time.Time{} }
func (alone) Close() error                         { return nil }

var (
	fuzzOnce sync.Once
	fuzzAddr string
	fuzzErr  error
)

// fuzzServer starts, once per process, a one-node cluster whose server
// takes HTTP/2 without TLS, and returns its address.
func fuzzServer(dir string) (string, error) {
	fuzzOnce.Do(func() {
		var n *node.Node
		n, fuzzErr = node.New(node.Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: dir, Tick: 10 * time.Millisecond})
		if fuzzErr != nil {
			return
		}
		go func() { _ = n.Run(context.Background(), alone{}) }()
		var s *server.Server
		if s, fuzzErr = server.New(n, server.Config{NoGateway: true}); fuzzErr != nil {
			return
		}
		var ln net.Listener
		if ln, fuzzErr = net.Listen("tcp", "127.0.0.1:0"); fuzzErr != nil {
			return
		}
		go func() { _ = s.Serve(ln) }()
		fuzzAddr = ln.Addr().String()
	})
	return fuzzAddr, fuzzErr
}

// h2Request is the frames of a client that opens a connection and makes
// one unary call of method with request m.
func h2Request(method string, m proto.Message) []byte {
	var buf, hb bytes.Buffer
	fr := http2.NewFramer(&buf, nil)
	enc := hpack.NewEncoder(&hb)
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"},
		{Name: ":path", Value: method}, {Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
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

// FuzzHTTP2 sends any bytes as frames on a client connection, after the
// preface and a SETTINGS frame, then closes its side. The server must
// answer or close the connection without a crash, and keep serving.
func FuzzHTTP2(f *testing.F) {
	f.Add(h2Request("/etcdserverpb.KV/Range", &pb.RangeRequest{Key: []byte("a")}))
	f.Add(h2Request("/etcdserverpb.KV/Put", &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}))
	f.Add(h2Request("/etcdserverpb.Maintenance/Status", &pb.StatusRequest{}))
	var settings bytes.Buffer
	if err := http2.NewFramer(&settings, nil).WriteSettings(); err != nil {
		f.Fatal(err)
	}
	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, b []byte) {
		addr, err := fuzzServer(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close() //nolint:errcheck // closing a fuzz connection
		if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		in := append([]byte(http2.ClientPreface), settings.Bytes()...)
		if _, err := c.Write(append(in, b...)); err != nil {
			return
		}
		_ = c.(*net.TCPConn).CloseWrite()
		_, _ = io.Copy(io.Discard, c)
	})
}
