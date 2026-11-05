// Package stack measures native floors of plan 5's retained production paths.
package stack

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/testcert"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const pipeSize = 256 << 10

var hosts = []string{"node1", "node2", "node3", "member", "updated-member"}

func must(b testing.TB, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
func certs(b testing.TB) (*testcert.CA, tls.Certificate) {
	b.Helper()
	ca, err := testcert.NewCA()
	must(b, err)
	cert, _, _, err := ca.IssueFor("tigerstore-sim", 2, nil, hosts)
	must(b, err)
	return ca, cert
}

// bufconn has finite buffers, deadlines and no kernel networking. The simulator
// likewise copies writes into in-memory streams, but also models delay/faults.
func BenchmarkPeer(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			ca, cert := certs(b)
			a, z := bufconn.Listen(pipeSize), bufconn.Listen(pipeSize)
			got := make(chan *raftpb.Message, 1)
			listeners := map[string]*bufconn.Listener{"node1:2380": a, "node2:2380": z}
			dial := func(ctx context.Context, addr string) (net.Conn, error) { return listeners[addr].DialContext(ctx) }
			recv, err := peer.Start(peer.Config{ID: 2, Addr: "node2:2380", Listeners: []net.Listener{z}, Peers: map[uint64]string{1: "node1:2380"}, Certificate: cert, CA: ca.Pool, Dial: dial, Unreachable: func(uint64) {}, Logf: b.Logf, Deliver: func(m *raftpb.Message) { got <- m }})
			must(b, err)
			send, err := peer.Start(peer.Config{ID: 1, Addr: "node1:2380", Listeners: []net.Listener{a}, Peers: map[uint64]string{2: "node2:2380"}, Certificate: cert, CA: ca.Pool, Dial: dial, Unreachable: func(uint64) {}, Logf: b.Logf, Deliver: func(*raftpb.Message) {}})
			must(b, err)
			b.Cleanup(func() { must(b, send.Close()); must(b, recv.Close()) })
			msg := &raftpb.Message{Type: raftpb.MessageType_MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Entries: []*raftpb.Entry{{Term: new(uint64(1)), Index: new(uint64(1)), Type: raftpb.EntryType_EntryNormal.Enum(), Data: make([]byte, size)}}}
			// Wait for the asynchronous sender's connection before timing; Send drops
			// messages while connecting by design.
			deadline := time.Now().Add(10 * time.Second)
			for send.ActiveSince(2).IsZero() {
				if time.Now().After(deadline) {
					b.Fatal("peer did not connect")
				}
				time.Sleep(time.Millisecond)
			}
			send.Send(msg)
			select {
			case <-got:
			case <-time.After(10 * time.Second):
				b.Fatal("warmup timeout")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				send.Send(msg)
				select {
				case m := <-got:
					if len(m.Entries[0].Data) != size {
						b.Fatal("bad payload")
					}
				case <-ctx.Done():
					b.Fatal("delivery timeout")
				}
			}
		})
	}
}

func BenchmarkCertificates(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		ca, err := testcert.NewCA()
		must(b, err)
		_, _, _, err = ca.IssueFor("tigerstore-sim", 2, nil, hosts)
		must(b, err)
	}
}

func BenchmarkHandshake(b *testing.B) {
	for _, kind := range []string{"Client", "Peer"} {
		b.Run(kind, func(b *testing.B) {
			ca, cert := certs(b)
			serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2", "http/1.1"}}
			clientCfg := &tls.Config{RootCAs: ca.Pool, ServerName: "node1", NextProtos: []string{"h2"}}
			if kind == "Peer" {
				serverCfg.MinVersion = tls.VersionTLS13
				serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
				serverCfg.ClientCAs = ca.Pool
				serverCfg.NextProtos = []string{"tigerstore-peer/2"}
				clientCfg.MinVersion = tls.VersionTLS13
				clientCfg.Certificates = []tls.Certificate{cert}
				clientCfg.NextProtos = serverCfg.NextProtos
			}
			// Deliberately retain Go's default key exchange, as the simulator does.
			// Includes pipe + TLS connection allocation, two goroutines' work, no tickets/resumption.
			ln := bufconn.Listen(16 << 10)
			defer ln.Close()
			errs := make(chan error, 1)
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					tc := tls.Server(c, serverCfg)
					err = tc.Handshake()
					errs <- err
					c.Close()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				c, err := ln.Dial()
				must(b, err)
				tc := tls.Client(c, clientCfg)
				must(b, tc.Handshake())
				must(b, <-errs)
				c.Close()
			}
		})
	}
}

func newClient(b *testing.B) (*clientv3.Client, *node.Node) {
	b.Helper()
	ca, cert := certs(b)
	// A tmpfs is an in-memory disk with real concurrent file/mmap semantics.
	// Using simfs directly would require simulator-wide locking (it isn't thread safe).
	dir, err := os.MkdirTemp("/dev/shm", "exp485-")
	must(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })
	cfg := core.DefaultConfig(1, []uint64{1})
	cfg.Members = []*pb.Member{{ID: 1, PeerURLs: []string{"https://node1:2380"}}}
	cfg.CheckpointBytes = 1 << 30
	cfg.CheckpointRatio = 100
	n, err := node.New(node.Config{Core: cfg, DataDir: dir, Tick: 10 * time.Millisecond, SegmentBytes: 1 << 20, Memory: func(int64) (int64, bool) { return 0, false }})
	must(b, err)
	pln := bufconn.Listen(pipeSize)
	tr, err := peer.Start(peer.Config{ID: 1, Addr: "node1:2380", Listeners: []net.Listener{pln}, Certificate: cert, CA: ca.Pool, Deliver: n.Deliver, Unreachable: n.Unreachable, Serve: n.Serve, Answer: n.Answer})
	must(b, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, tr) }()
	srv, err := server.New(n, server.Config{TLS: &tls.Config{Certificates: []tls.Certificate{cert}}, NoGateway: true})
	must(b, err)
	ln := bufconn.Listen(pipeSize)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"https://node1:2379"}, TLS: &tls.Config{RootCAs: ca.Pool}, Logger: zap.NewNop(), DialTimeout: 10 * time.Second, DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) })}})
	must(b, err)
	b.Cleanup(func() {
		must(b, cli.Close())
		must(b, srv.Stop())
		must(b, <-served)
		cancel()
		must(b, <-done)
		must(b, tr.Close())
	})
	deadline := time.Now().Add(10 * time.Second)
	for n.Leader() != 1 {
		if time.Now().After(deadline) {
			b.Fatal("no leader")
		}
		time.Sleep(time.Millisecond)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	_, err = cli.Put(ctx2, "key", strings.Repeat("v", 1024))
	must(b, err)
	return cli, n
}

func BenchmarkClient(b *testing.B) {
	for _, op := range []string{"Put1KiB", "Range1Key", "ComparePut1KiB", "PutAndWatch1KiB"} {
		b.Run(op, func(b *testing.B) {
			if op == "PutAndWatch1KiB" && os.Getenv("EXP485_STUB") == "1" {
				b.Skip("stub KV writes deliberately do not emit node watch events")
			}
			cli, _ := newClient(b)
			value := strings.Repeat("v", 1024)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			var wc clientv3.WatchChan
			if op == "PutAndWatch1KiB" {
				wc = cli.Watch(ctx, "key", clientv3.WithCreatedNotify())
				select {
				case r := <-wc:
					if !r.Created {
						b.Fatal("watch not created")
					}
				case <-ctx.Done():
					b.Fatal(ctx.Err())
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				switch op {
				case "Put1KiB":
					_, err := cli.Put(ctx, "key", value)
					must(b, err)
				case "Range1Key":
					r, err := cli.Get(ctx, "key")
					must(b, err)
					if len(r.Kvs) != 1 || len(r.Kvs[0].Value) != 1024 {
						b.Fatal("wrong range")
					}
				case "ComparePut1KiB":
					r, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.Version("key"), ">", 0)).Then(clientv3.OpPut("key", value)).Commit()
					must(b, err)
					if !r.Succeeded {
						b.Fatal("txn failed")
					}
				case "PutAndWatch1KiB":
					_, err := cli.Put(ctx, "key", value)
					must(b, err)
					select {
					case r := <-wc:
						if r.Err() != nil || len(r.Events) != 1 || len(r.Events[0].Kv.Value) != 1024 {
							b.Fatalf("bad watch: %+v", r)
						}
					case <-ctx.Done():
						b.Fatal(ctx.Err())
					}
				}
			}
		})
	}
}

// One large independent region per worker (128 MiB working set per worker,
// source+destination). This is copy throughput, not guaranteed DRAM-only
// bandwidth: a runner may have an LLC as large as one worker's working set.
// All workers copy once per op.
func BenchmarkMemory(b *testing.B) {
	for _, workers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("Threads%d", workers), func(b *testing.B) {
			const size = 64 << 20
			var wg sync.WaitGroup
			starts := make([]chan bool, workers)
			done := make(chan byte, workers)
			for i := range workers {
				src, dst := make([]byte, size), make([]byte, size)
				for j := range src {
					src[j] = byte(j)
				}
				for j := range dst {
					dst[j] = 1
				}
				starts[i] = make(chan bool)
				wg.Go(func() {
					for range starts[i] {
						copy(dst, src)
						done <- dst[size-1]
					}
					runtime.KeepAlive(dst)
				})
			}
			defer func() {
				for _, c := range starts {
					close(c)
				}
				wg.Wait()
			}()
			b.SetBytes(int64(size * workers))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, c := range starts {
					c <- true
				}
				for range workers {
					<-done
				}
			}
		})
	}
}

var digest [32]byte

func BenchmarkSHA256(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		digest = sha256.Sum256(data)
	}
}
func BenchmarkAESGCM(b *testing.B) {
	block, err := aes.NewCipher(make([]byte, 16))
	must(b, err)
	gcm, err := cipher.NewGCM(block)
	must(b, err)
	nonce := make([]byte, gcm.NonceSize())
	data := make([]byte, 1<<20)
	dst := make([]byte, 0, len(data)+gcm.Overhead())
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		dst = gcm.Seal(dst[:0], nonce, data, nil)
	}
	runtime.KeepAlive(dst)
}
