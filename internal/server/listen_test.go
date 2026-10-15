package server_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/server"
)

// TestListenerTraffic checks what each kind of client listener serves, as
// etcd's do: without TLS, HTTP/2 goes to gRPC alone, so other HTTP/2
// requests get 415, and HTTP/1.1 to the HTTP endpoints; a listener of
// --listen-client-urls next to --listen-client-http-urls serves gRPC
// only, and one of the latter HTTP only; a TLS listener that serves gRPC
// only offers h2 alone.
func TestListenerTraffic(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	listen := func(tlsOn bool, traffic server.Traffic) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go c.servers[1].ServeListener(server.Listener{Listener: ln, TLS: tlsOn, Traffic: traffic}) //nolint:errcheck // ends with the cluster's Stop
		return ln.Addr().String()
	}
	grpcOK := func(addr string, creds credentials.TransportCredentials) error {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck // closing a test client
		ctx, cancel := context.WithTimeout(context.Background(), 2e9)
		defer cancel()
		_, err = pb.NewKVClient(conn).Range(ctx, &pb.RangeRequest{Key: []byte("k")})
		return err
	}
	get := func(client *http.Client, url string) (int, string, error) {
		resp, err := client.Get(url)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close() //nolint:errcheck // a test response
		_, err = io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("Grpc-Status"), err
	}
	http1 := &http.Client{Transport: &http.Transport{}}
	h2c := &http.Client{Transport: &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	})

	all := listen(false, server.TrafficAll)
	if err := grpcOK(all, insecure.NewCredentials()); err != nil {
		t.Errorf("gRPC without TLS: %v", err)
	}
	if code, _, err := get(http1, "http://"+all+"/version"); err != nil || code != http.StatusOK {
		t.Errorf("HTTP/1.1 without TLS: %d, %v", code, err)
	}
	if code, gs, err := get(h2c, "http://"+all+"/version"); err != nil || code != http.StatusUnsupportedMediaType || gs != "3" {
		t.Errorf("HTTP/2 without TLS: %d, grpc-status %q, %v", code, gs, err)
	}

	grpcOnly := listen(false, server.TrafficGRPC)
	if err := grpcOK(grpcOnly, insecure.NewCredentials()); err != nil {
		t.Errorf("gRPC on a gRPC listener: %v", err)
	}
	if _, _, err := get(http1, "http://"+grpcOnly+"/version"); err == nil {
		t.Error("HTTP/1.1 on a gRPC listener got an answer")
	}

	httpOnly := listen(false, server.TrafficHTTP)
	if code, _, err := get(http1, "http://"+httpOnly+"/version"); err != nil || code != http.StatusOK {
		t.Errorf("HTTP/1.1 on an HTTP listener: %d, %v", code, err)
	}
	if err := grpcOK(httpOnly, insecure.NewCredentials()); err == nil {
		t.Error("gRPC on an HTTP listener went through")
	}

	tlsGRPC := listen(true, server.TrafficGRPC)
	ctls := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost"}
	if err := grpcOK(tlsGRPC, credentials.NewTLS(ctls)); err != nil {
		t.Errorf("gRPC on a TLS gRPC listener: %v", err)
	}
	https1 := &http.Client{Transport: &http.Transport{TLSClientConfig: ctls}}
	if _, _, err := get(https1, "https://"+tlsGRPC+"/version"); err == nil {
		t.Error("HTTPS/1.1 on a TLS gRPC listener got an answer")
	}
}

// TestRequestLimits checks etcd's --max-request-bytes and --max-txn-ops
// set below their defaults.
func TestRequestLimits(t *testing.T) {
	c := newClusterWith(t, func(c *cluster) {
		c.serverConfig = func(cfg *server.Config) { cfg.MaxRequestBytes = 4096 }
		c.coreConfig = func(cfg *core.Config) { cfg.MaxRequestBytes, cfg.MaxTxnOps = 4096, 2 }
	})
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 3000)})
	})
	if _, err := kv.Put(context.Background(), &pb.PutRequest{Key: []byte("k"), Value: make([]byte, 5000)}); !errors.Is(err, rpctypes.ErrGRPCRequestTooLarge) {
		t.Errorf("a put over --max-request-bytes: %v", err)
	}
	op := &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k")}}}
	if _, err := kv.Txn(context.Background(), &pb.TxnRequest{Success: []*pb.RequestOp{op, op}}); err != nil {
		t.Errorf("a txn of --max-txn-ops: %v", err)
	}
	if _, err := kv.Txn(context.Background(), &pb.TxnRequest{Success: []*pb.RequestOp{op, op, op}}); !errors.Is(err, rpctypes.ErrGRPCTooManyOps) {
		t.Errorf("a txn over --max-txn-ops: %v", err)
	}
}
