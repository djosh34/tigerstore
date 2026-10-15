package server_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestGRPCHealth checks the gRPC health service against grpc-go's
// health.Server as etcd v3.7.2 runs it.
func TestGRPCHealth(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 80)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(c.running[1].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hc := healthpb.NewHealthClient(conn)

	if r, err := hc.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check of the server = %v, %v", r, err)
	}
	if _, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: "etcdserverpb.KV"}); status.Code(err) != codes.NotFound || status.Convert(err).Message() != "unknown service" {
		t.Fatalf("Check of a service = %v", err)
	}
	r, err := hc.List(ctx, &healthpb.HealthListRequest{})
	if err != nil || len(r.Statuses) != 1 || r.Statuses[""].GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("List = %v, %v", r, err)
	}

	for service, want := range map[string]healthpb.HealthCheckResponse_ServingStatus{
		"":        healthpb.HealthCheckResponse_SERVING,
		"unknown": healthpb.HealthCheckResponse_SERVICE_UNKNOWN,
	} {
		wctx, wcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		w, err := hc.Watch(wctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatal(err)
		}
		if r, err := w.Recv(); err != nil || r.Status != want {
			t.Fatalf("Watch of %q = %v, %v", service, r, err)
		}
		// The status does not change, so the stream stays open with
		// nothing more to send.
		if r, err := w.Recv(); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("Watch of %q after its status = %v, %v", service, r, err)
		}
		wcancel()
	}

	err = conn.Invoke(ctx, "/grpc.health.v1.Health/Nothing", &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != "unknown method Nothing for service grpc.health.v1.Health" {
		t.Fatalf("unknown health method = %v", err)
	}
}

// TestReadyzLivez checks /readyz and /livez as etcd v3.7.2 answers them,
// on a healthy cluster, without a quorum and on a stopped node.
func TestReadyzLivez(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 81)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	hc := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	type answer struct {
		code    int
		body    string
		ctype   string
		nosniff string
		allow   string
	}
	do := func(method, path string) answer {
		req, err := http.NewRequest(method, "https://"+c.running[1].addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // closing a test response
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ProtoMajor != 2 {
			t.Fatalf("%s %s over HTTP/%d", method, path, resp.ProtoMajor)
		}
		return answer{resp.StatusCode, string(b), resp.Header.Get("Content-Type"), resp.Header.Get("X-Content-Type-Options"), resp.Header.Get("Allow")}
	}
	get := func(path string) answer { return do(http.MethodGet, path) }
	const plain = "text/plain; charset=utf-8"
	ok := func(body string) answer { return answer{200, body, plain, "nosniff", ""} }
	failed := func(body string) answer { return answer{503, body, plain, "nosniff", ""} }
	all := "[+]data_corruption ok\n[+]serializable_read ok\n[+]linearizable_read ok\n[+]non_learner ok\n"
	for path, want := range map[string]answer{
		"/readyz":               ok("ok\n"),
		"/readyz?verbose":       ok(all + "ok\n"),
		"/readyz?verbose=false": ok(all + "ok\n"),
		"/readyz?exclude=linearizable_read&exclude=nothing&verbose": ok("[+]data_corruption ok\n[+]serializable_read ok\n[+]non_learner ok\nok\n"),
		"/readyz/linearizable_read?verbose":                         ok("[+]linearizable_read ok\nok\n"),
		"/readyz/non_learner?exclude=non_learner&verbose":           ok("[+]non_learner ok\nok\n"),
		"/readyz/data_corruption":                                   ok("ok\n"),
		"/livez?verbose":                                            ok("[+]serializable_read ok\nok\n"),
		"/livez?exclude=serializable_read&verbose":                  ok("ok\n"),
		"/livez/serializable_read":                                  ok("ok\n"),
		"/readyz/":                                                  {404, "404 page not found\n", plain, "nosniff", ""},
		"/livez/linearizable_read":                                  {404, "404 page not found\n", plain, "nosniff", ""},
	} {
		if got := get(path); got != want {
			t.Errorf("GET %s = %+v, want %+v", path, got, want)
		}
	}
	for _, method := range []string{http.MethodHead, http.MethodPost} {
		want := answer{405, "Method Not Allowed\n", plain, "nosniff", "GET"}
		if method == http.MethodHead {
			want.body = ""
		}
		if got := do(method, "/readyz"); got != want {
			t.Errorf("%s /readyz = %+v, want %+v", method, got, want)
		}
	}

	// Without a quorum the linearizable read fails; the node still
	// serves serializable reads, so it is live.
	c.running[2].stop()
	c.running[3].stop()
	delete(c.running, 2)
	delete(c.running, 3)
	got := get("/readyz?verbose")
	if got.code != 503 || !strings.HasPrefix(got.body, "[+]data_corruption ok\n[+]serializable_read ok\n[-]linearizable_read failed: ") ||
		!strings.HasSuffix(got.body, "\n[+]non_learner ok\n\n") || got.ctype != plain || got.nosniff != "nosniff" {
		t.Fatalf("/readyz without a quorum = %+v", got)
	}
	if got := get("/livez"); got != ok("ok\n") {
		t.Fatalf("/livez without a quorum = %+v", got)
	}
	if got := get("/readyz?exclude=linearizable_read"); got != ok("ok\n") {
		t.Fatalf("/readyz without a quorum and its linearizable read = %+v", got)
	}

	// A stopped node fails its serializable read too.
	c.running[1].stopNode()
	if got, want := get("/livez"), failed("[-]serializable_read failed: etcdserver: server stopped\n\n"); got != want {
		t.Fatalf("/livez of a stopped node = %+v, want %+v", got, want)
	}
}
