//go:build pre50frontend

package main

// Experiment-only observation. All tigerstore variants use this exact file.
import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/osfs"
)

func pre50Observe(n *node.Node) error {
	addr := os.Getenv("PRE50_DEBUG")
	if addr == "" {
		return fmt.Errorf("pre50frontend requires PRE50_DEBUG")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/leader", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]uint64{"id": n.ID(), "leader": n.Leader(), "gomaxprocs": uint64(runtime.GOMAXPROCS(0))})
	})
	mux.HandleFunc("/debug/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		// Sample before JSON encoding. Never force GC or enable alloc profiling.
		_ = json.NewEncoder(w).Encode(struct {
			Mallocs    uint64            `json:"mallocs"`
			TotalAlloc uint64            `json:"total_alloc"`
			Disk       map[string]uint64 `json:"disk"`
		}{m.Mallocs, m.TotalAlloc, osfs.Pre50Stats()})
	})
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	go func() { _ = http.Serve(ln, mux) }()
	return nil
}
