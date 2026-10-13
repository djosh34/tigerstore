package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	grpcpeer "google.golang.org/grpc/peer"

	"github.com/djosh34/tigerstore/internal/testcert"
)

type authHealth struct {
	healthpb.UnimplementedHealthServer
	states chan credentials.TLSInfo
}

func (h *authHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	p, _ := grpcpeer.FromContext(ctx)
	if p != nil {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			h.states <- info
		}
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (h *authHealth) Watch(_ *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	if err := stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestNativeTLSRoutingAuthAndShutdown(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("server", 1)
	if err != nil {
		t.Fatal(err)
	}
	clientCert, _, _, err := ca.Issue("client", 2)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: ca.Pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	s := New(nil, Config{TLS: cfg})
	h := &authHealth{states: make(chan credentials.TLSInfo, 1)}
	healthpb.RegisterHealthServer(s.g, h)
	requestAddrs := make(chan string, 4)
	// Test Request.TLS as well as the production version handler.
	s.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("HTTP request lost verified TLS state")
		}
		requestAddrs <- r.RemoteAddr
		s.handler().ServeHTTP(w, r)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve(ln) }()
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Error(err)
		}
	})
	clientTLS := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: ca.Pool, ServerName: "localhost"}
	for _, h2 := range []bool{false, true} {
		tr := &http.Transport{TLSClientConfig: clientTLS.Clone(), ForceAttemptHTTP2: h2}
		hc := &http.Client{Transport: tr, Timeout: 3 * time.Second}
		resp, err := hc.Get("https://" + ln.Addr().String() + "/version")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		wantProto := 1
		if h2 {
			wantProto = 2
		}
		if err != nil || resp.StatusCode != 200 || resp.ProtoMajor != wantProto ||
			string(body) != `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}` {
			t.Fatalf("HTTP/%d version: %s %d %q %v", wantProto, resp.Proto, resp.StatusCode, body, err)
		}
		firstAddr := <-requestAddrs
		// Reuse the same connection after the synthetic SETTINGS ACK was
		// consumed. The reader must now pass every frame through unchanged.
		resp, err = hc.Get("https://" + ln.Addr().String() + "/version")
		if err != nil {
			t.Fatal(err)
		}
		body, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.ProtoMajor != wantProto || resp.StatusCode != 200 ||
			string(body) != `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}` || <-requestAddrs != firstAddr {
			t.Fatalf("HTTP/%d second request did not reuse a working connection: %q %v", wantProto, body, err)
		}
		t.Cleanup(tr.CloseIdleConnections)
	}
	noCert := clientTLS.Clone()
	noCert.Certificates = nil
	badTransport := &http.Transport{TLSClientConfig: noCert, ForceAttemptHTTP2: true}
	t.Cleanup(badTransport.CloseIdleConnections)
	if resp, err := (&http.Client{Transport: badTransport, Timeout: 3 * time.Second}).Get("https://" + ln.Addr().String() + "/version"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("HTTP accepted a client without a certificate")
	}

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hc := healthpb.NewHealthClient(conn)
	if _, err := hc.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	select {
	case info := <-h.states:
		if !info.State.HandshakeComplete || len(info.State.VerifiedChains) == 0 ||
			len(info.State.PeerCertificates) != 1 || info.State.PeerCertificates[0].Subject.CommonName != "client" ||
			info.SecurityLevel != credentials.PrivacyAndIntegrity {
			t.Fatalf("gRPC TLS auth info = %+v", info)
		}
	case <-ctx.Done():
		t.Fatal("gRPC handler lost TLS auth info")
	}
	watch, err := hc.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	// Hold both an unfinished TLS handshake and a completed handshake with
	// no first request. Neither may hold shutdown until the sniff timeout.
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close() //nolint:errcheck // closing a test socket
	idleTLS := clientTLS.Clone()
	idleTLS.NextProtos = []string{"h2"}
	idle, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", ln.Addr().String(), idleTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close() //nolint:errcheck // closing a test socket
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on an idle client")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
	if _, err := watch.Recv(); err == nil {
		t.Fatal("stream survived Stop")
	}
	if c, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		_ = c.Close()
		t.Fatal("listener survived Stop")
	}
}

func TestNativeSettingsAckReader(t *testing.T) {
	settings := []byte{0, 0, 0, 4, 0, 0, 0, 0, 0}
	ack := []byte{0, 0, 0, 4, 1, 0, 0, 0, 0}
	headers := []byte{0, 0, 3, 1, 4, 0, 0, 0, 1, 'a', 'b', 'c'}
	input := bytes.Join([][]byte{settings, ack, headers, ack, ack}, nil)
	want := bytes.Join([][]byte{settings, headers, ack}, nil)
	r := &settingsAckReader{conn: bytes.NewReader(input), remaining: 2}
	// Read across every header and payload boundary. Only the two ACKs
	// for matcher SETTINGS disappear; the real server's ACK stays intact.
	var got []byte
	for {
		var b [1]byte
		n, err := r.Read(b[:])
		got = append(got, b[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("filtered frames = %x, want %x", got, want)
	}
}

func TestNativeStopBeforeServe(t *testing.T) {
	s := New(nil, Config{TLS: &tls.Config{}})
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(ln); err != nil {
		t.Fatal(err)
	}
}
