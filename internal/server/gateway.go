package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

	gw "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/tmc/grpc-websocket-proxy/wsproxy"
	etcdservergw "go.etcd.io/etcd/api/v3/etcdserverpb/gw"
	v3electiongw "go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb/gw"
	v3lockgw "go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb/gw"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The HTTP side of the client port follows etcd v3.7.2's (embed/serve.go):
// an access controller in front of every HTTP request, the gRPC gateway
// under /v3/, and the other endpoints (http.go) behind it.
//
// The gateway is etcd's own: grpc-gateway's mux with etcd's marshaler
// options and the handlers generated for etcd's services, which call the
// gRPC services as a client. etcd's gateway dials its own client port;
// tigerstore's dials this server through an in-process connection, which
// carries no certificate. grpc-gateway forwards the Authorization header
// as metadata and marks every call with grpcgateway-* metadata, for which
// etcd ignores a certificate's name anyway.

// gatewayHandlers are etcd's.
var gatewayHandlers = []func(context.Context, *gw.ServeMux, *grpc.ClientConn) error{
	etcdservergw.RegisterKVHandler,
	etcdservergw.RegisterWatchHandler,
	etcdservergw.RegisterLeaseHandler,
	etcdservergw.RegisterClusterHandler,
	etcdservergw.RegisterMaintenanceHandler,
	etcdservergw.RegisterAuthHandler,
	v3lockgw.RegisterLockHandler,
	v3electiongw.RegisterElectionHandler,
}

// newGateway builds the gateway on a client connection to s itself.
func (s *Server) newGateway() (http.Handler, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient("passthrough:///tigerstore",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			client, server := net.Pipe()
			s.mu.Lock()
			if s.stopped {
				s.mu.Unlock()
				_ = server.Close()
				return client, nil
			}
			s.wg.Add(1)
			s.mu.Unlock()
			go func() {
				defer s.wg.Done()
				s.servePlain(server)
			}()
			return client, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	)
	if err != nil {
		return nil, nil, err
	}
	// etcd's: proto field names, no unpopulated fields, unknown fields
	// ignored.
	m := &gw.JSONPb{}
	m.UseProtoNames, m.EmitUnpopulated, m.DiscardUnknown = true, false, true
	mux := gw.NewServeMux(gw.WithMarshalerOption(gw.MIMEWildcard, &gw.HTTPBodyMarshaler{Marshaler: m}))
	for _, register := range gatewayHandlers {
		if err := register(s.ctx, mux, conn); err != nil {
			return nil, nil, errors.Join(err, conn.Close())
		}
	}
	// etcd's streams take websockets too, as POST.
	ws := wsproxy.WebsocketProxy(mux,
		wsproxy.WithRequestMutator(func(_ *http.Request, out *http.Request) *http.Request {
			out.Method = http.MethodPost
			return out
		}),
		wsproxy.WithMaxRespBodyBufferSize(0x7fffffff),
	)
	return ws, conn, nil
}

// ServeHTTP is etcd's accessController and HTTP mux: it moves /v3beta/
// to /v3/, checks the Host of a request without TLS against the
// whitelist, refuses gateway requests with a named client certificate
// while auth is on, adds the CORS headers, answers OPTIONS, and then
// serves the gateway or the other endpoints.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/v3beta/") {
		r.URL.Path = strings.Replace(r.URL.Path, "/v3beta/", "/v3/", 1)
	}
	if r.TLS == nil {
		if host := hostname(r); !allowed(s.cfg.HostWhitelist, host) {
			http.Error(w, errCVE20185702(host), http.StatusMisdirectedRequest)
			return
		}
	} else if s.cfg.TLS != nil && s.cfg.TLS.ClientAuth == tls.RequireAndVerifyClientCert && s.gateway != nil &&
		s.n.AuthEnabled() && strings.HasPrefix(r.URL.Path, "/v3/") {
		for _, chain := range r.TLS.VerifiedChains {
			if len(chain) > 0 && chain[0].Subject.CommonName != "" {
				http.Error(w, "CommonName of client sending a request against gateway will be ignored and not used as expected", http.StatusBadRequest)
				return
			}
		}
	}
	if allowed(s.cfg.CORS, "*") {
		addCORSHeader(w, "*")
	} else if origin := r.Header.Get("Origin"); allowed(s.cfg.CORS, origin) {
		addCORSHeader(w, origin)
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	switch {
	case s.gateway != nil && strings.HasPrefix(r.URL.Path, "/v3/"):
		s.gateway.ServeHTTP(w, r)
		return
	case r.URL.Path == "/metrics":
		s.serveMetrics(w, r)
		return
	case r.URL.Path == "/debug/vars":
		s.serveVars(w, r)
		return
	case s.pprof != nil && isPprof(r.URL.Path):
		s.pprof.ServeHTTP(w, r)
		return
	}
	writeAnswer(w, r, s.answerHTTP(r.Context(), r.Method, r.URL.RequestURI()))
}

// MetricsHandler serves what etcd serves on --listen-metrics-urls:
// /metrics, /health, /livez and /readyz, without CORS headers.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, check := checkPaths[r.URL.Path]
		switch {
		case r.URL.Path == "/metrics":
			s.serveMetrics(w, r)
		case r.URL.Path == "/health" || check:
			writeAnswer(w, r, s.answerHTTP(r.Context(), r.Method, r.URL.RequestURI()))
		default:
			http.NotFound(w, r)
		}
	})
}

// writeAnswer writes an answer of answerHTTP.
func writeAnswer(w http.ResponseWriter, r *http.Request, a httpAnswer) {
	h := w.Header()
	h.Set("Content-Type", a.contentType)
	h.Set("Content-Length", strconv.Itoa(len(a.body)))
	if a.allow != "" {
		h.Set("Allow", a.allow)
	}
	if a.nosniff {
		h.Set("X-Content-Type-Options", "nosniff")
	}
	w.WriteHeader(a.code)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(a.body))
	}
}

// allowed is etcd's OriginAllowed and IsHostWhitelisted: an empty list or
// one with "*" allows all.
func allowed(list map[string]struct{}, v string) bool {
	if len(list) == 0 {
		return true
	}
	if _, ok := list["*"]; ok {
		return true
	}
	_, ok := list[v]
	return ok
}

// hostname is etcd's httputil.GetHostname: the request's Host without
// its port.
func hostname(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.Host); err == nil {
		return host
	}
	return r.Host
}

// errCVE20185702 is etcd's answer to a Host that is not whitelisted.
func errCVE20185702(host string) string {
	return fmt.Sprintf(`
etcd received your request, but the Host header was unrecognized.

To fix this, choose one of the following options:
- Enable TLS, then any HTTPS request will be allowed.
- Add the hostname you want to use to the whitelist in settings.
  - e.g. etcd --host-whitelist %q

This requirement has been added to help prevent "DNS Rebinding" attacks (CVE-2018-5702).
`, host)
}

// addCORSHeader is etcd's.
func addCORSHeader(w http.ResponseWriter, origin string) {
	w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Origin", origin)
	w.Header().Add("Access-Control-Allow-Headers", "accept, content-type, authorization")
}
