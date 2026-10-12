package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func testRaw(t *testing.T) (*backend, func() *grpc.ClientConn) {
	t.Helper()
	old := *certDir
	*certDir = t.TempDir()
	t.Cleanup(func() { *certDir = old })
	if e := makeCert(); e != nil {
		t.Fatal(e)
	}
	serverTLS, e := tlsConfig(true)
	if e != nil {
		t.Fatal(e)
	}
	clientTLS, e := tlsConfig(false)
	if e != nil {
		t.Fatal(e)
	}
	l, e := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if e != nil {
		t.Fatal(e)
	}
	b := newBackend()
	var mu sync.Mutex
	var accepted []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			accepted = append(accepted, c)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				if e := c.(*tls.Conn).Handshake(); e != nil {
					c.Close()
					return
				}
				_ = (&rawConn{Conn: c, b: b}).serve()
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		mu.Lock()
		for _, c := range accepted {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return b, func() *grpc.ClientConn {
		c, e := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithInitialWindowSize(65535), grpc.WithInitialConnWindowSize(65535))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
}
func TestRawManyStreams(t *testing.T) {
	b, dial := testRaw(t)
	cs := []pb.KVClient{pb.NewKVClient(dial()), pb.NewKVClient(dial())}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 256 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 20 {
				r, e := cs[i%2].Put(ctx, &pb.PutRequest{Key: []byte(fmt.Sprintf("key-%d", i)), Value: bytes.Repeat([]byte("v"), 1024)})
				if e != nil || r.Header == nil {
					t.Errorf("Put: %v", e)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rev != 5121 || len(b.kv) != 256 {
		t.Fatalf("revision=%d keys=%d", b.rev, len(b.kv))
	}
}
func TestRawFragmentationAndFlowControl(t *testing.T) {
	_, dial := testRaw(t)
	c := pb.NewKVClient(dial())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	value := bytes.Repeat([]byte("x"), 1<<20)
	for range 4 {
		if _, e := c.Put(ctx, &pb.PutRequest{Key: []byte("large"), Value: value}); e != nil {
			t.Fatal(e)
		}
		r, e := c.Range(ctx, &pb.RangeRequest{Key: []byte("large")})
		if e != nil {
			t.Fatal(e)
		}
		if len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Value, value) {
			t.Fatal("Range differs")
		}
	}
}
func TestRawWatchAndCancel(t *testing.T) {
	b, dial := testRaw(t)
	conn := dial()
	kv := pb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	watchCtx, stop := context.WithCancel(ctx)
	if _, e := kv.Put(ctx, &pb.PutRequest{Key: []byte("watched"), Value: []byte("old")}); e != nil {
		t.Fatal(e)
	}
	w, e := pb.NewWatchClient(conn).Watch(watchCtx)
	if e != nil {
		t.Fatal(e)
	}
	if e := w.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("watched"), PrevKv: true}}}); e != nil {
		t.Fatal(e)
	}
	created, e := w.Recv()
	if e != nil || !created.Created {
		t.Fatalf("create: %v %v", created, e)
	}
	for range 3 {
		if _, e := kv.Put(ctx, &pb.PutRequest{Key: []byte("watched"), Value: bytes.Repeat([]byte("x"), 256<<10)}); e != nil {
			t.Fatal(e)
		}
		r, e := w.Recv()
		if e != nil || len(r.Events) != 1 || r.Events[0].PrevKv == nil {
			t.Fatalf("watch: %v %v", r, e)
		}
	}
	stop()
	end := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		n := len(b.watchers)
		b.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatal("watch remains registered after cancellation")
		}
		time.Sleep(time.Millisecond)
	}
	if _, e := kv.Put(ctx, &pb.PutRequest{Key: []byte("watched"), Value: []byte("after")}); e != nil {
		t.Fatal(e)
	}
}
func TestLeanGenerator(t *testing.T) {
	_, dial := testRaw(t)
	conn := dial()
	old := *addr
	*addr = conn.Target()
	defer func() { *addr = old }()
	l, e := newLeanClient(0)
	if e != nil {
		t.Fatal(e)
	}
	defer l.close()
	if r := l.run(50*time.Millisecond, false); r.err != nil {
		t.Fatal(r.err)
	}
	r := l.run(100*time.Millisecond, true)
	if r.err != nil {
		t.Fatal(r.err)
	}
	var n int64
	for _, v := range r.hist {
		n += v
	}
	if n != r.n || n < 128 {
		t.Fatalf("completions=%d histogram=%d", r.n, n)
	}
}

func TestRawUnimplemented(t *testing.T) {
	_, dial := testRaw(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, e := pb.NewKVClient(dial()).Compact(ctx, &pb.CompactionRequest{Revision: 1})
	if status.Code(e) != codes.Unimplemented {
		t.Fatalf("got %v", e)
	}
}
