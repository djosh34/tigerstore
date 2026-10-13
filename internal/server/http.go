package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// The HTTP endpoints share the client port with gRPC, as in etcd v3.7.2
// (server/etcdserver/api/etcdhttp). kubeadm's preflight reads /version.

// healthTimeout bounds the read that /health makes.
const healthTimeout = 5 * time.Second

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /version", version)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			s.g.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// version answers like etcd: the server, cluster and storage versions.
func version(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"etcdserver":"`+core.ServerVersion+`","etcdcluster":"`+core.ClusterVersion+`","storage":"`+core.StorageVersion+`"}`)
}

// health answers like etcd: without ?serializable=true it needs a leader,
// then it reads one key, linearizable unless serializable was asked for.
// health is a JSON string, not a boolean.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	serializable := r.URL.Query().Get("serializable") == "true"
	if !serializable && s.n.Leader() == 0 {
		http.Error(w, `{"health":"false","reason":"RAFT NO LEADER"}`, http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
	defer cancel()
	if _, err := s.n.Do(ctx, &pb.RangeRequest{Key: []byte("health"), KeysOnly: true, Limit: 1, Serializable: serializable}); err != nil {
		http.Error(w, `{"health":"false","reason":"RANGE ERROR:`+err.Error()+`"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, `{"health":"true","reason":""}`)
}
