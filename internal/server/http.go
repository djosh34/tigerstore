package server

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// The HTTP endpoints share the client port with gRPC, as in etcd v3.7.2
// (server/etcdserver/api/etcdhttp). kubeadm's preflight reads /version.
// They are served over HTTP/2 and, for probes that do not offer h2 in
// TLS, over HTTP/1.1.

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
		answer = s.version
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
	if method != http.MethodGet {
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
// The storage version follows the cluster version, which a downgrade
// lowers.
func (s *Server) version(context.Context, url.Values) httpAnswer {
	v := s.n.EtcdVersion()
	return httpAnswer{code: http.StatusOK, contentType: appJSON, body: `{"etcdserver":"` + core.ServerVersion + `","etcdcluster":"` + v + `","storage":"` + v + `"}`}
}

// health answers like etcd: without ?serializable=true it needs a leader,
// then it reads one key, linearizable unless serializable was asked for.
// health is a JSON string, not a boolean. Failures are written as
// http.Error writes them.
func (s *Server) health(ctx context.Context, q url.Values) httpAnswer {
	a := s.checkHealth(ctx, q)
	if a.code == http.StatusOK {
		s.metrics.set.HealthSuccess.Inc()
	} else {
		s.metrics.set.HealthFailures.Inc()
	}
	return a
}

func (s *Server) checkHealth(ctx context.Context, q url.Values) httpAnswer {
	for _, a := range s.n.Alarms() {
		if slices.Contains(q["exclude"], a.Alarm.String()) {
			continue
		}
		reason := "ALARM UNKNOWN"
		switch a.Alarm {
		case pb.AlarmType_NOSPACE:
			reason = "ALARM NOSPACE"
		case pb.AlarmType_CORRUPT:
			reason = "ALARM CORRUPT"
		case pb.AlarmType_NONE:
			// Never active; etcd's answer would be ALARM UNKNOWN.
		}
		return httpError(http.StatusServiceUnavailable, `{"health":"false","reason":"`+reason+`"}`)
	}
	serializable := q.Get("serializable") == "true"
	if !serializable && s.n.Leader() == 0 {
		return httpError(http.StatusServiceUnavailable, `{"health":"false","reason":"RAFT NO LEADER"}`)
	}
	if err := s.healthRead(ctx, serializable); err != nil {
		return httpError(http.StatusServiceUnavailable, `{"health":"false","reason":"RANGE ERROR:`+err.Error()+`"}`)
	}
	return httpAnswer{code: http.StatusOK, contentType: textPlain, body: `{"health":"true","reason":""}`}
}
