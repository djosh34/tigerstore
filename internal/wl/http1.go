package wl

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	gw "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	etcdservergw "go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	v3electiongw "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb/gw"
	v3lockgw "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb/gw"
	"google.golang.org/grpc"
)

// The client port's HTTP/1.1 side. A client that offers HTTP/1.1, such
// as curl, a probe or the bench's metrics scraper, keeps crypto/tls's
// connection and is served by net/http in goroutines of its own; the
// loop never sees it. It serves etcd's gRPC gateway under /v3/, and
// /version, /health, /metrics and /debug/pprof/.
//
// The gateway is etcd's (embed/serve.go): grpc-gateway's mux with etcd's
// marshaler options and the handlers generated for etcd's services. As
// etcd's does, it is a gRPC client of the node's own client port, over a
// loopback connection (selfClient); so the loop serves it as any other
// client.

// http1 is a node's HTTP/1.1 server.
type http1 struct {
	srv  *http.Server
	ln   *connListener
	done chan struct{}
}

// gatewayHandlers are the services etcd's gateway serves that the node
// serves too.
var gatewayHandlers = []func(context.Context, *gw.ServeMux, *grpc.ClientConn) error{
	etcdservergw.RegisterKVHandler,
	etcdservergw.RegisterWatchHandler,
	etcdservergw.RegisterLeaseHandler,
	etcdservergw.RegisterClusterHandler,
	etcdservergw.RegisterMaintenanceHandler,
	v3lockgw.RegisterLockHandler,
	v3electiongw.RegisterElectionHandler,
}

// startHTTP1 starts the HTTP/1.1 server. Its gateway is a client of the
// node through conn.
func startHTTP1(ctx context.Context, conn *grpc.ClientConn, m *nodeMetrics) (*http1, error) {
	// etcd's: proto field names, no unpopulated fields, unknown fields
	// ignored.
	jm := &gw.JSONPb{}
	jm.UseProtoNames, jm.EmitUnpopulated, jm.DiscardUnknown = true, false, true
	gmux := gw.NewServeMux(gw.WithMarshalerOption(gw.MIMEWildcard, &gw.HTTPBodyMarshaler{Marshaler: jm}))
	for _, register := range gatewayHandlers {
		if err := register(ctx, gmux, conn); err != nil {
			return nil, err
		}
	}
	// etcd's streams take websockets too, as POST.
	gateway := wsproxy.WebsocketProxy(gmux,
		wsproxy.WithRequestMutator(func(_ *http.Request, out *http.Request) *http.Request {
			out.Method = http.MethodPost
			return out
		}),
		wsproxy.WithMaxRespBodyBufferSize(math.MaxInt32),
	)
	mux := http.NewServeMux()
	mux.Handle("/v3/", gateway)
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		// etcd's health: true while the node has a leader.
		w.Header().Set("Content-Type", "application/json")
		if m.hasLeader.Load() != 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"health":"false","reason":"RAFT NO LEADER"}`))
			return
		}
		_, _ = w.Write([]byte(`{"health":"true","reason":""}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(m.text())
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	h := &http1{
		srv: &http.Server{
			// etcd moves /v3beta/ to /v3/.
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if rest, ok := strings.CutPrefix(r.URL.Path, "/v3beta/"); ok {
					r.URL.Path = "/v3/" + rest
				}
				mux.ServeHTTP(w, r)
			}),
			ReadHeaderTimeout: time.Minute,
		},
		ln:   &connListener{conns: make(chan net.Conn), closed: make(chan struct{})},
		done: make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		_ = h.srv.Serve(h.ln)
	}()
	return h, nil
}

// serve hands the server a connection after its handshake. The server
// takes it unless it stopped.
func (h *http1) serve(c *tls.Conn) {
	select {
	case h.ln.conns <- c:
	case <-h.ln.closed:
		_ = c.Close()
	}
}

// stop closes the server and its connections.
func (h *http1) stop() error {
	err := h.srv.Close()
	<-h.done
	return err
}

// connListener hands net/http the connections the node accepted and
// shook hands with.
type connListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *connListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *connListener) Addr() net.Addr { return &net.TCPAddr{} }
