package server_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

// plain is a compressor that the server does not know.
type plain struct{}

func (plain) Compress(w io.Writer) (io.WriteCloser, error) { return nopCloser{w}, nil }
func (plain) Decompress(r io.Reader) (io.Reader, error)    { return r, nil }
func (plain) Name() string                                 { return "plain" }

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func init() { encoding.RegisterCompressor(plain{}) }

// TestGzip checks gRPC compression as etcd v3.7.2's grpc-go server has
// it: gzip requests and responses on unary and streaming calls, and
// grpc-go's answer to an encoding it does not know.
func TestGzip(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 81)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv := pb.NewKVClient(conn)
	big := bytes.Repeat([]byte("compressible "), 10_000)
	put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("g"), Value: big}, grpc.UseCompressor(gzip.Name))
	})
	ctx := context.Background()
	r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("g")}, grpc.UseCompressor(gzip.Name))
	if err != nil || len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, big) {
		t.Fatalf("gzip range: %v", err)
	}
	w, err := pb.NewWatchClient(conn).Watch(ctx, grpc.UseCompressor(gzip.Name))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("g"), StartRevision: put.Header.Revision}}}); err != nil {
		t.Fatal(err)
	}
	for {
		resp, err := w.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Events) > 0 {
			if !bytes.Equal(resp.Events[0].Kv.Value, big) {
				t.Fatal("gzip watch: wrong value")
			}
			break
		}
	}
	_, err = kv.Range(ctx, &pb.RangeRequest{Key: []byte("g")}, grpc.UseCompressor("plain"))
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), `grpc: Decompressor is not installed for grpc-encoding "plain"`) {
		t.Fatalf("an unknown encoding: %v", err)
	}
}

// TestWatchFragments checks etcd's watch fragmentation: a watcher that
// asks for fragments gets a response of 2 MiB or more in parts below
// that, all but the last marked, and another watcher gets it whole.
func TestWatchFragments(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 82)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv := pb.NewKVClient(conn)
	var first int64
	for i := range 3 {
		put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte("f"), Value: bytes.Repeat([]byte{byte('a' + i)}, 1<<20)})
		})
		if i == 0 {
			first = put.Header.Revision
		}
	}
	for _, fragment := range []bool{true, false} {
		w, err := pb.NewWatchClient(conn).Watch(context.Background(), grpc.MaxCallRecvMsgSize(8<<20))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("f"), StartRevision: first, Fragment: fragment}}}); err != nil {
			t.Fatal(err)
		}
		var parts []string
		for events := 0; events < 3; {
			resp, err := w.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Events) == 0 {
				continue
			}
			events += len(resp.Events)
			parts = append(parts, strings.Repeat("e", len(resp.Events))+map[bool]string{true: "+", false: ""}[resp.Fragment])
		}
		want := "eee"
		if fragment {
			want = "e+ e+ e"
		}
		if got := strings.Join(parts, " "); got != want {
			t.Fatalf("fragment %v: responses %q, want %q", fragment, got, want)
		}
	}
}
