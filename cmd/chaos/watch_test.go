package main

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resetOnce fails the first watch stream once it delivered its created
// response and reset is closed, before any event, as a node that installs
// a snapshot closes its watch streams. clientv3 then resumes the watch on
// a new stream.
type resetOnce struct {
	grpc.ClientStream
	reset   <-chan struct{}
	done    *atomic.Bool
	created bool
}

func (s *resetOnce) RecvMsg(m any) error {
	if s.created && s.done.CompareAndSwap(false, true) {
		select {
		case <-s.reset:
			return status.Error(codes.Unavailable, "test: the stream resets after the created response")
		case <-s.Context().Done():
			return s.Context().Err()
		}
	}
	err := s.ClientStream.RecvMsg(m)
	if r, ok := m.(*pb.WatchResponse); ok && err == nil && r.GetCreated() {
		s.created = true
	}
	return err
}

// TestSlowWatchAfterReset is #256's case on etcd v3.7.2 itself: the slow
// watch's stream resets after its created response and before its first
// event, and clientv3 resumes it. It must still deliver exactly the events
// of a reference watch from its start, with nothing at or before the
// revision it was opened after.
func TestSlowWatchAfterReset(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger, cfg.LogLevel = "zap", "error"
	for _, u := range []*[]url.URL{&cfg.ListenClientUrls, &cfg.AdvertiseClientUrls, &cfg.ListenPeerUrls, &cfg.AdvertisePeerUrls} {
		*u = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	}
	cfg.InitialCluster = cfg.Name + "=" + cfg.AdvertisePeerUrls[0].String()
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd did not start")
	}
	ep := e.Clients[0].Addr().String()
	var done atomic.Bool
	reset := make(chan struct{})
	cl, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep}, Logger: zap.NewNop(), DialTimeout: 2 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			s, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil || method != "/etcdserverpb.Watch/Watch" {
				return s, err
			}
			return &resetOnce{ClientStream: s, reset: reset, done: &done}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close() //nolint:errcheck // closing a test client
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, prefix+"k0", "before"); err != nil {
		t.Fatal(err)
	}
	start, wc, err := watchAfter(ctx, cl, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	close(reset)
	put, err := cl.Put(ctx, prefix+"k1", "after")
	if err != nil {
		t.Fatal(err)
	}
	end := put.Header.Revision
	got, last, _ := readWatch(wc, end)
	if !done.Load() {
		t.Fatal("the stream never reset")
	}
	ref, _, _ := readWatch(cl.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(start+1)), end)
	if err := sameEvents(got, ref, last); err != nil || last != end {
		t.Fatalf("the slow watch after %d reached %d of %d: %v; got %+v, want %+v", start, last, end, err, got, ref)
	}
}
