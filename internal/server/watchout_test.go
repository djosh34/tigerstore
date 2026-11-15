package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// TestWatchChunksHoldTheirResponses runs two watch streams on one
// connection through many chunks of watch output (watchout.go). A chunk
// that went back to the server too early is overwritten (poisonBodies, set by arena_test.go),
// so every event's value is checked.
func TestWatchChunksHoldTheirResponses(t *testing.T) {
	const puts = 200
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	first := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("o/start"), Value: []byte("0")})
	})
	conn, err := grpc.NewClient(c.running[2].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader)
	value := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 1000+i*7) }
	var streams []pb.Watch_WatchClient
	for range 2 {
		ws, err := pb.NewWatchClient(conn).Watch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
			Key: []byte("o/k"), RangeEnd: []byte("o/l"), StartRevision: first.Header.Revision + 1,
		}}}); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, ws)
	}
	for i := range puts {
		if _, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "o/k%04d", i), Value: value(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for s, ws := range streams {
		got := 0
		for got < puts {
			r, err := ws.Recv()
			if err != nil {
				t.Fatalf("stream %d after %d events: %v", s, got, err)
			}
			for _, ev := range r.Events {
				if want := fmt.Sprintf("o/k%04d", got); string(ev.Kv.Key) != want || !bytes.Equal(ev.Kv.Value, value(got)) {
					t.Fatalf("stream %d, event %d: key %q with %d bytes of value, want %s with its value", s, got, ev.Kv.Key, len(ev.Kv.Value), want)
				}
				got++
			}
		}
	}
}
