package batchfront

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

const healthTimeout = 7 * time.Second

type httpReply struct {
	code              int
	contentType, body string
}

func versionReply() httpReply {
	return httpReply{http.StatusOK, "application/json", `{"etcdserver":"` + core.ServerVersion + `","etcdcluster":"` + core.ClusterVersion + `","storage":"` + core.StorageVersion + `"}`}
}

func healthReply(err error) httpReply {
	if err == nil {
		return httpReply{http.StatusOK, "text/plain; charset=utf-8", `{"health":"true","reason":""}`}
	}
	msg := err.Error()
	if st, ok := status.FromError(err); ok {
		msg = st.Message()
	}
	return httpReply{http.StatusServiceUnavailable, "text/plain; charset=utf-8", `{"health":"false","reason":"RANGE ERROR:` + msg + "\"}\n"}
}

func noLeaderReply() httpReply {
	return httpReply{http.StatusServiceUnavailable, "text/plain; charset=utf-8", "{\"health\":\"false\",\"reason\":\"RAFT NO LEADER\"}\n"}
}

func healthRead(serializable bool) *pb.RangeRequest {
	return &pb.RangeRequest{Key: []byte("health"), KeysOnly: true, Limit: 1, Serializable: serializable}
}

func (c *conn) dispatchHTTP(id uint32, s *incoming, a *admission) error {
	c.mu.Lock()
	if b := c.budgets[id]; b != nil {
		b.http = true
	}
	c.mu.Unlock()
	u, err := url.ParseRequestURI(s.path)
	if err != nil {
		c.sendHTTP(id, httpReply{http.StatusBadRequest, "text/plain; charset=utf-8", "Bad Request\n"})
		return nil //nolint:nilerr // Bad request is an HTTP response, not a connection error.
	}
	switch u.Path {
	case "/version":
		c.sendHTTP(id, versionReply())
	case "/health":
		serializable := u.Query().Get("serializable") == "true"
		if !serializable && c.s.n.Leader() == 0 {
			c.sendHTTP(id, noLeaderReply())
			return nil
		}
		c.admit(id)
		c.mu.Lock()
		if b := c.budgets[id]; b != nil {
			b.timer = time.AfterFunc(healthTimeout, func() {
				c.Complete([]node.Completion{{Tag: id, Err: context.DeadlineExceeded}})
			})
		}
		c.mu.Unlock()
		// Health is always a real Range, even in the rpc ablation. It shares
		// the existing admission and completion groups, without a waiting reader.
		a.ops = append(a.ops, healthRead(serializable))
		a.legacyTags = append(a.legacyTags, id)
		if len(a.tags)+len(a.legacyTags) >= maxBatchCommands {
			return c.flush(a)
		}
	default:
		c.sendHTTP(id, httpReply{http.StatusNotFound, "text/plain; charset=utf-8", "404 page not found\n"})
	}
	return nil
}

func (c *conn) isHTTP(id uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.budgets[id]
	return b != nil && b.http
}

func (c *conn) encodeHTTP(id uint32, r httpReply) (encoded, bool) {
	c.mu.Lock()
	b := c.budgets[id]
	if b == nil || b.httpDone {
		c.mu.Unlock()
		return encoded{}, false
	}
	b.httpDone = true
	if b.timer != nil {
		b.timer.Stop()
	}
	c.mu.Unlock()
	fields := []hpack.HeaderField{{Name: ":status", Value: strconv.Itoa(r.code)}, {Name: "content-type", Value: r.contentType}, {Name: "content-length", Value: strconv.Itoa(len(r.body))}}
	if r.code >= 400 {
		fields = append(fields, hpack.HeaderField{Name: "x-content-type-options", Value: "nosniff"})
	}
	h, err := headerBlock(fields...)
	if err != nil {
		c.close()
		return encoded{}, false
	}
	n := 18 + len(h) + len(r.body)
	if !c.reserve(id, n) {
		return encoded{}, false
	}
	wire := make([]byte, n)
	frameHeader(wire, len(h), http2.FrameHeaders, byte(http2.FlagHeadersEndHeaders), id)
	copy(wire[9:], h)
	start := 9 + len(h)
	frameHeader(wire[start:], len(r.body), http2.FrameData, byte(http2.FlagDataEndStream), id)
	copy(wire[start+9:], r.body)
	return encoded{id: id, wire: wire, reserved: n}, true
}

func (c *conn) sendHTTP(id uint32, r httpReply) {
	if e, ok := c.encodeHTTP(id, r); ok {
		select {
		case c.out <- []encoded{e}:
			c.s.stats.responseSends.Add(1)
		default:
			c.release(e.reserved)
			c.close()
		}
	}
}

// HTTP/1.1 is only a setup endpoint adapter. gRPC always uses our Framer.
func (c *conn) serveHTTP1() {
	s := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
		WriteTimeout:      healthTimeout + 5*time.Second,
		MaxHeaderBytes:    16 << 10,
		BaseContext:       func(net.Listener) context.Context { return c.ctx },
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				c.close()
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reply httpReply
			if r.Method != http.MethodGet {
				reply = httpReply{http.StatusMethodNotAllowed, "text/plain; charset=utf-8", "Method Not Allowed\n"}
			} else {
				switch r.URL.Path {
				case "/version":
					reply = versionReply()
				case "/health":
					serializable := r.URL.Query().Get("serializable") == "true"
					if !serializable && c.s.n.Leader() == 0 {
						reply = noLeaderReply()
					} else {
						ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
						_, err := c.s.n.Do(ctx, healthRead(serializable))
						cancel()
						reply = healthReply(err)
					}
				default:
					reply = httpReply{http.StatusNotFound, "text/plain; charset=utf-8", "404 page not found\n"}
				}
			}
			w.Header().Set("Content-Type", reply.contentType)
			if reply.code >= 400 {
				w.Header().Set("X-Content-Type-Options", "nosniff")
			}
			w.WriteHeader(reply.code)
			if _, err := io.WriteString(w, reply.body); err != nil {
				c.close()
			}
		}),
	}
	ln := &httpListener{c: c}
	if err := s.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
		c.close()
	}
	_ = s.Close()
}

type httpListener struct {
	c        *conn
	accepted bool
}

func (l *httpListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.c.Conn, nil
	}
	<-l.c.ctx.Done()
	return nil, net.ErrClosed
}
func (l *httpListener) Close() error   { l.c.close(); return nil }
func (l *httpListener) Addr() net.Addr { return l.c.LocalAddr() }
