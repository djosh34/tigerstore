package batchfront

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/testcert"
)

func startHTTPFront(t *testing.T, n *node.Node, mode string) (string, *tls.Config, *Server) {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("server", 123)
	if err != nil {
		t.Fatal(err)
	}
	clientCert, _, _, err := ca.Issue("client", 124)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(n, server.Config{TLS: &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: ca.Pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}}, mode)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return ln.Addr().String(), &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{clientCert}, ServerName: "localhost", MinVersion: tls.VersionTLS13}, s
}

func httpClients(t *testing.T, cfg *tls.Config) []*http.Client {
	t.Helper()
	h1 := &http.Transport{TLSClientConfig: cfg.Clone(), TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	h2 := &http2.Transport{TLSClientConfig: cfg.Clone()}
	t.Cleanup(func() { h1.CloseIdleConnections(); h2.CloseIdleConnections() })
	return []*http.Client{{Transport: h1, Timeout: 3 * time.Second}, {Transport: h2, Timeout: 3 * time.Second}}
}

func checkGET(t *testing.T, client *http.Client, endpoint, path string, protocol, code int, body string) {
	t.Helper()
	r, err := client.Get("https://" + endpoint + path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if r.ProtoMajor != protocol || r.StatusCode != code || string(got) != body {
		t.Fatalf("%s: HTTP/%d status %d body %q", path, r.ProtoMajor, r.StatusCode, got)
	}
}

func TestHTTPSharedMTLSPort(t *testing.T) {
	for _, mode := range []string{"rpc", "admit", "full"} {
		t.Run(mode, func(t *testing.T) {
			endpoint, cfg, s := startHTTPFront(t, startNode(t), mode)
			clients := httpClients(t, cfg)
			for i, client := range clients {
				checkGET(t, client, endpoint, "/version", i+1, 200, versionReply().body)
				checkGET(t, client, endpoint, "/health", i+1, 200, healthReply(nil).body)
				checkGET(t, client, endpoint, "/health?serializable=true", i+1, 200, healthReply(nil).body)
			}
			// POST a unary gRPC call with the HTTP/2 transport that already
			// served GETs. This proves both use the same connection, not a mux.
			wire := []byte{0, 0, 0, 0, 0}
			req, err := http.NewRequest(http.MethodPost, "https://"+endpoint+"/etcdserverpb.KV/Range", bytes.NewReader(wire))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/grpc")
			r, err := clients[1].Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if r.Trailer.Get("Grpc-Status") != "3" {
				t.Fatalf("empty key trailers: %v", r.Trailer)
			}
			cc, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(cfg.Clone())))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cc.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := pb.NewKVClient(cc).Put(ctx, &pb.PutRequest{Key: []byte("health"), Value: []byte("v")}); err != nil {
				t.Fatal(err)
			}
			checkGET(t, clients[1], endpoint, "/version", 2, 200, versionReply().body)
			if s.Stats().ControlSends == 0 || s.Stats().ResponseSends == 0 {
				t.Fatalf("missing sends: %+v", s.Stats())
			}
			for _, proto := range []string{"h2", "http/1.1"} {
				noCert := cfg.Clone()
				noCert.Certificates = nil
				noCert.NextProtos = []string{proto}
				tc, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", endpoint, noCert)
				if err == nil {
					_ = tc.SetDeadline(time.Now().Add(time.Second))
					_, err = tc.Write([]byte("GET /version HTTP/1.1\r\nHost: localhost\r\n\r\n"))
					if err == nil {
						var b [1]byte
						_, err = tc.Read(b[:])
					}
					_ = tc.NetConn().Close()
				}
				if err == nil {
					t.Fatalf("%s accepted no client certificate", proto)
				}
			}
		})
	}
}

func TestHTTPHealthWithoutLeader(t *testing.T) {
	n, err := node.New(node.Config{Core: core.DefaultConfig(1, []uint64{1, 2, 3}), DataDir: t.TempDir(), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, discardPeer{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	endpoint, cfg, _ := startHTTPFront(t, n, "full")
	for i, client := range httpClients(t, cfg) {
		checkGET(t, client, endpoint, "/health", i+1, 503, noLeaderReply().body)
		checkGET(t, client, endpoint, "/health?serializable=True", i+1, 503, noLeaderReply().body)
		checkGET(t, client, endpoint, "/health?serializable=true", i+1, 200, healthReply(nil).body)
		checkGET(t, client, endpoint, "/version", i+1, 200, versionReply().body)
	}
}

func TestHTTPHealthLateCompletion(t *testing.T) {
	c, _ := testConn(t)
	if !c.start(1) {
		t.Fatal("start")
	}
	c.mu.Lock()
	c.budgets[1].http = true
	c.mu.Unlock()
	c.admit(1)
	c.Complete([]node.Completion{{Tag: 1, Err: context.DeadlineExceeded}})
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.RangeResponse{}}})
	batch := <-c.out
	if len(batch) != 1 || !bytes.Contains(batch[0].wire, []byte("RANGE ERROR:context deadline exceeded")) {
		t.Fatalf("reply: %q", batch[0].wire)
	}
	select {
	case <-c.out:
		t.Fatal("duplicate HTTP completion")
	default:
	}
	c.reset(1)
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.RangeResponse{}}})
	select {
	case <-c.out:
		t.Fatal("late reset completion")
	default:
	}
	if healthTimeout != 7*time.Second {
		t.Fatal("health timeout")
	}
	read := healthRead(false)
	if string(read.Key) != "health" || read.Serializable || !read.KeysOnly || read.Limit != 1 {
		t.Fatalf("health read: %v", read)
	}
}
