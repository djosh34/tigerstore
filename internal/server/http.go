package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
)

// The HTTP endpoints share the client port with gRPC, as in etcd v3.7.2
// (server/etcdserver/api/etcdhttp). kubeadm's preflight reads /version.
// They are served over HTTP/2 and, for probes that do not offer h2 in
// TLS, over HTTP/1.1.

// healthTimeout bounds the read that /health makes: etcd's request
// timeout, 5 s plus two election timeouts of 1 s.
const healthTimeout = 7 * time.Second

// http1Idle closes an HTTP/1.1 connection that sends no request.
const http1Idle = 2 * time.Minute

const (
	textPlain = "text/plain; charset=utf-8"
	appJSON   = "application/json"
)

// answerHTTP answers one HTTP request with a status code, content type and
// body, as net/http's ServeMux and etcd's handlers do.
func (s *Server) answerHTTP(ctx context.Context, method, target string) (int, string, string) {
	u, err := url.ParseRequestURI(target)
	if err != nil {
		return http.StatusBadRequest, textPlain, "400 Bad Request\n"
	}
	var answer func(context.Context, url.Values) (int, string, string)
	switch u.Path {
	case "/health":
		answer = s.health
	case "/version":
		answer = version
	default:
		return http.StatusNotFound, textPlain, "404 page not found\n"
	}
	if method != http.MethodGet && method != http.MethodHead {
		return http.StatusMethodNotAllowed, textPlain, "Method Not Allowed\n"
	}
	return answer(ctx, u.Query())
}

// version answers like etcd: the server, cluster and storage versions.
func version(context.Context, url.Values) (int, string, string) {
	return http.StatusOK, appJSON, `{"etcdserver":"` + core.ServerVersion + `","etcdcluster":"` + core.ClusterVersion + `","storage":"` + core.StorageVersion + `"}`
}

// health answers like etcd: without ?serializable=true it needs a leader,
// then it reads one key, linearizable unless serializable was asked for.
// health is a JSON string, not a boolean. Failures are written as
// http.Error writes them.
func (s *Server) health(ctx context.Context, q url.Values) (int, string, string) {
	serializable := q.Get("serializable") == "true"
	if !serializable && s.n.Leader() == 0 {
		return http.StatusServiceUnavailable, textPlain, `{"health":"false","reason":"RAFT NO LEADER"}` + "\n"
	}
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	if _, err := s.n.Do(ctx, &pb.RangeRequest{Key: []byte("health"), KeysOnly: true, Limit: 1, Serializable: serializable}); err != nil {
		// etcd reports its internal error text, without gRPC's wrapper.
		msg := err.Error()
		if st, ok := status.FromError(err); ok {
			msg = st.Message()
		}
		return http.StatusServiceUnavailable, textPlain, `{"health":"false","reason":"RANGE ERROR:` + msg + `"}` + "\n"
	}
	return http.StatusOK, textPlain, `{"health":"true","reason":""}`
}

// httpHeaders is the HTTP/2 header block of an HTTP response.
func httpHeaders(code int, contentType string, length int) []byte {
	fields := []hpack.HeaderField{
		{Name: ":status", Value: strconv.Itoa(code)},
		{Name: "content-type", Value: contentType},
		{Name: "content-length", Value: strconv.Itoa(length)},
	}
	if code != http.StatusOK {
		fields = append(fields, hpack.HeaderField{Name: "x-content-type-options", Value: "nosniff"})
	}
	return headerBlock(fields...)
}

// http1Conn is a TLS client that did not offer h2. It gets /health and
// /version over HTTP/1.1; gRPC needs HTTP/2.
type http1Conn struct {
	s    *Server
	nc   net.Conn
	raw  net.Conn
	once sync.Once
}

func (h *http1Conn) goAway() { h.close() }

func (h *http1Conn) close() { h.once.Do(func() { _ = h.raw.Close() }) }

func (h *http1Conn) serve() {
	br := bufio.NewReader(h.nc)
	bw := bufio.NewWriter(h.nc)
	for {
		if err := h.nc.SetReadDeadline(time.Now().Add(http1Idle)); err != nil {
			return
		}
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		// The endpoints read no body; a small one is skipped.
		if _, err := io.Copy(io.Discard, io.LimitReader(req.Body, 1<<16)); err != nil {
			return
		}
		ctx, cancel := context.WithCancel(h.s.ctx)
		code, contentType, body := h.s.answerHTTP(ctx, req.Method, req.RequestURI)
		cancel()
		if err := h.nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return
		}
		_, _ = bw.WriteString("HTTP/1.1 " + strconv.Itoa(code) + " " + http.StatusText(code) + "\r\n" +
			"Content-Type: " + contentType + "\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n")
		if code != http.StatusOK {
			_, _ = bw.WriteString("X-Content-Type-Options: nosniff\r\n")
		}
		if req.Close {
			_, _ = bw.WriteString("Connection: close\r\n")
		}
		_, _ = bw.WriteString("\r\n")
		if req.Method != http.MethodHead {
			_, _ = bw.WriteString(body)
		}
		if err := bw.Flush(); err != nil || req.Close {
			return
		}
	}
}
