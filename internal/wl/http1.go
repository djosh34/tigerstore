package wl

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"net/http"
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
// loop never sees it. It serves etcd's HTTP endpoints (httpapi.go) and
// its gRPC gateway under /v3/.
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

// startHTTP1 starts the HTTP/1.1 server with api. Its gateway is a client
// of the node through conn.
func startHTTP1(ctx context.Context, conn *grpc.ClientConn, api *httpAPI) (*http1, error) {
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
	api.gateway = gateway
	h := &http1{
		srv:  &http.Server{Handler: api, ReadHeaderTimeout: time.Minute},
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
