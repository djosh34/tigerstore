package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// diagResetStream drops the first watch after its created response, before
// its first event. Snapshot install closes tigerstore streams this way.
type diagResetStream struct {
	grpc.ClientStream
	reset   <-chan struct{}
	cancel  context.CancelFunc
	created bool
}

func (s *diagResetStream) RecvMsg(m any) error {
	if s.created {
		select {
		case <-s.reset:
			s.cancel()
			return status.Error(codes.Unavailable, "diag: snapshot-like stream reset before first event")
		case <-s.Context().Done():
			return s.Context().Err()
		}
	}
	err := s.ClientStream.RecvMsg(m)
	if r, ok := m.(*pb.WatchResponse); ok && err == nil && r.Created {
		s.created = true
	}
	return err
}

type diagRequestStream struct {
	grpc.ClientStream
	requests chan<- int64
}

func (s *diagRequestStream) SendMsg(m any) error {
	if r, ok := m.(*pb.WatchRequest); ok && r.GetCreateRequest() != nil {
		s.requests <- r.GetCreateRequest().StartRevision
	}
	return s.ClientStream.SendMsg(m)
}

// TestDiag256SlowWatchBaseline runs the actual chaos watch setup and comparison
// against both implementations, with a deterministic reset before the first event.
func TestDiag256SlowWatchBaseline(t *testing.T) {
	for _, backend := range []string{"TIGERSTORE", "ETCD"} {
		t.Run(backend, func(t *testing.T) {
			ep := diag256Backend(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			direct, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, Logger: zap.NewNop()})
			if err != nil {
				t.Fatal(err)
			}
			defer direct.Close()
			seed, err := direct.Put(ctx, prefix+"k0", "seed")
			if err != nil {
				t.Fatal(err)
			}
			reset := make(chan struct{})
			requests := make(chan int64, 8)
			var first atomic.Bool
			intercept := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				if method != "/etcdserverpb.Watch/Watch" {
					return streamer(ctx, desc, cc, method, opts...)
				}
				child, stop := context.WithCancel(ctx)
				stream, err := streamer(child, desc, cc, method, opts...)
				if err != nil {
					stop()
					return nil, err
				}
				var wrapped grpc.ClientStream = &diagRequestStream{ClientStream: stream, requests: requests}
				if first.CompareAndSwap(false, true) {
					wrapped = &diagResetStream{ClientStream: wrapped, reset: reset, cancel: stop}
				}
				return wrapped, nil
			}
			slow, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, Logger: zap.NewNop(), DialOptions: []grpc.DialOption{grpc.WithStreamInterceptor(intercept)}})
			if err != nil {
				t.Fatal(err)
			}
			defer slow.Close()
			wc, start, err := startSlowWatch(ctx, slow)
			if err != nil {
				t.Fatal(err)
			}
			initial := <-requests
			close(reset)
			var resumed int64
			select {
			case resumed = <-requests:
			case <-ctx.Done():
				t.Fatal("watch did not resume")
			}
			last, err := direct.Put(ctx, prefix+"k1", "after")
			if err != nil {
				t.Fatal(err)
			}
			got, reached, canceled := readWatch(wc, last.Header.Revision)
			if canceled || reached < last.Header.Revision || len(got) == 0 {
				t.Fatalf("did not reach final put: %v %d %v", got, reached, canceled)
			}
			ref, _, _ := readWatch(direct.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(start+1)), reached)
			t.Logf("seed=%d baseline=%d initial-request=%d resumed-request=%d first-event=%d reference-first=%d", seed.Header.Revision, start, initial, resumed, got[0].Rev, ref[0].Rev)
			if err := sameEvents(got, ref, reached); err != nil {
				t.Fatalf("the slow watch from %d: %v", start, err)
			}
		})
	}
}

func diag256Backend(t *testing.T, backend string) string {
	t.Helper()
	bin := os.Getenv("DIAG_" + backend)
	if bin == "" {
		t.Skip("set DIAG_" + backend + " to the backend binary")
	}
	free := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		return addr
	}
	client, peer := free(), free()
	dir := t.TempDir()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	_, cert, key, err := ca.Issue("n1", 2)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"ca.pem": ca.PEM, "cert.pem": cert, "key.pem": key} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(bin, "--name", "n1", "--data-dir", filepath.Join(dir, "data"), "--listen-client-urls", "http://"+client, "--advertise-client-urls", "http://"+client, "--listen-peer-urls", "https://"+peer, "--initial-advertise-peer-urls", "https://"+peer, "--initial-cluster", "n1=https://"+peer, "--peer-cert-file", filepath.Join(dir, "cert.pem"), "--peer-key-file", filepath.Join(dir, "key.pem"), "--peer-trusted-ca-file", filepath.Join(dir, "ca.pem"), "--log-level", "warn")
	log, err := os.Create(filepath.Join(dir, "backend.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
		log.Close()
		if t.Failed() {
			b, _ := os.ReadFile(log.Name())
			t.Logf("%s log: %s", backend, b)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := clientv3.New(clientv3.Config{Endpoints: []string{client}, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Get(ctx, "ready"); err != nil {
		t.Fatal(fmt.Errorf("backend not ready: %w", err))
	}
	return client
}
