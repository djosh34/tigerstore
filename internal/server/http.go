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

	"golang.org/x/net/http2/hpack"

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

// httpAnswer is the status code, content type and body of an HTTP
// response, with the headers that some answers add.
type httpAnswer struct {
	code        int
	contentType string
	body        string
	allow       string // the Allow header
	nosniff     bool   // X-Content-Type-Options: nosniff
}

// httpError answers as http.Error writes an error.
func httpError(code int, msg string) httpAnswer {
	return httpAnswer{code: code, contentType: textPlain, body: msg + "\n", nosniff: true}
}

// answerHTTP answers one HTTP request as net/http's ServeMux and etcd's
// handlers do.
func (s *Server) answerHTTP(ctx context.Context, method, target string) httpAnswer {
	u, err := url.ParseRequestURI(target)
	if err != nil {
		return httpError(http.StatusBadRequest, "400 Bad Request")
	}
	var answer func(context.Context, url.Values) httpAnswer
	switch u.Path {
	case "/health":
		answer = s.health
	case "/version":
		answer = version
	case "/metrics":
		answer = s.metricsText
	default:
		p, ok := checkPaths[u.Path]
		if !ok {
			return httpError(http.StatusNotFound, "404 page not found")
		}
		if method != http.MethodGet {
			return methodNotAllowed()
		}
		return s.runChecks(ctx, p, u.Query())
	}
	if method != http.MethodGet && method != http.MethodHead {
		return methodNotAllowed()
	}
	return answer(ctx, u.Query())
}

// methodNotAllowed is etcd's answer to a method other than GET.
func methodNotAllowed() httpAnswer {
	a := httpError(http.StatusMethodNotAllowed, "Method Not Allowed")
	a.allow = http.MethodGet
	return a
}

// version answers like etcd: the server, cluster and storage versions.
func version(context.Context, url.Values) httpAnswer {
	return httpAnswer{code: http.StatusOK, contentType: appJSON, body: `{"etcdserver":"` + core.ServerVersion + `","etcdcluster":"` + core.ClusterVersion + `","storage":"` + core.StorageVersion + `"}`}
}

// health answers like etcd: without ?serializable=true it needs a leader,
// then it reads one key, linearizable unless serializable was asked for.
// health is a JSON string, not a boolean. Failures are written as
// http.Error writes them.
func (s *Server) health(ctx context.Context, q url.Values) httpAnswer {
	serializable := q.Get("serializable") == "true"
	if !serializable && s.n.Leader() == 0 {
		return httpError(http.StatusServiceUnavailable, `{"health":"false","reason":"RAFT NO LEADER"}`)
	}
	if err := s.healthRead(ctx, serializable); err != nil {
		return httpError(http.StatusServiceUnavailable, `{"health":"false","reason":"RANGE ERROR:`+err.Error()+`"}`)
	}
	return httpAnswer{code: http.StatusOK, contentType: textPlain, body: `{"health":"true","reason":""}`}
}

// httpHeaders is the HTTP/2 header block of an HTTP response.
func httpHeaders(a httpAnswer) []byte {
	fields := []hpack.HeaderField{
		{Name: ":status", Value: strconv.Itoa(a.code)},
		{Name: "content-type", Value: a.contentType},
		{Name: "content-length", Value: strconv.Itoa(len(a.body))},
	}
	if a.allow != "" {
		fields = append(fields, hpack.HeaderField{Name: "allow", Value: a.allow})
	}
	if a.nosniff {
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
		a := h.s.answerHTTP(ctx, req.Method, req.RequestURI)
		cancel()
		if err := h.nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return
		}
		_, _ = bw.WriteString("HTTP/1.1 " + strconv.Itoa(a.code) + " " + http.StatusText(a.code) + "\r\n" +
			"Content-Type: " + a.contentType + "\r\nContent-Length: " + strconv.Itoa(len(a.body)) + "\r\n")
		if a.allow != "" {
			_, _ = bw.WriteString("Allow: " + a.allow + "\r\n")
		}
		if a.nosniff {
			_, _ = bw.WriteString("X-Content-Type-Options: nosniff\r\n")
		}
		if req.Close {
			_, _ = bw.WriteString("Connection: close\r\n")
		}
		_, _ = bw.WriteString("\r\n")
		if req.Method != http.MethodHead {
			_, _ = bw.WriteString(a.body)
		}
		if err := bw.Flush(); err != nil || req.Close {
			return
		}
	}
}
