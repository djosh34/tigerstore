package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/osfs"
)

const shapeSampleInterval = 100 * time.Millisecond

// listenShapeDebug accepts only literal loopback addresses. An empty
// address disables observation, including the disk wrappers and sampler.
func listenShapeDebug(addr string) (net.Listener, error) {
	if addr == "" {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("shape debug address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("shape debug address must use a literal loopback IP: %q", addr)
	}
	return net.Listen("tcp", addr)
}

type shapeSnapshot struct {
	SchemaVersion    int      `json:"schema_version"`
	CapturedUnixNS   int64    `json:"captured_unix_ns"`
	SampleIntervalNS int64    `json:"sample_interval_ns"`
	WallNSBounds     []uint64 `json:"wall_ns_bounds"`
	*node.ObservationSnapshot
}

func shapeHandler(n *node.Node, logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/shape", func(w http.ResponseWriter, _ *http.Request) {
		o := n.Observation()
		if o == nil {
			http.Error(w, "observation disabled", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(shapeSnapshot{
			SchemaVersion: 1, CapturedUnixNS: time.Now().UnixNano(),
			SampleIntervalNS: int64(shapeSampleInterval), WallNSBounds: osfs.WallNSBounds(),
			ObservationSnapshot: o,
		}); err != nil {
			logger.Printf("shape snapshot: %v", err)
		}
	})
	// Use a private mux, never the client listener or DefaultServeMux.
	// No block/mutex/CPU/trace profiler rate is changed at startup.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func serveShapeDebug(ln net.Listener, n *node.Node, logger *log.Logger) *http.Server {
	s := &http.Server{Handler: shapeHandler(n, logger), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: time.Minute}
	go func() {
		if err := s.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Printf("shape debug HTTP: %v", err)
		}
	}()
	logger.Printf("shape debug HTTP on %s, sample interval %s", ln.Addr(), shapeSampleInterval)
	return s
}
