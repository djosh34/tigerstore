package main

// Throwaway observability only. No core, storage or transport code changes.
import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/djosh34/tigerstore/internal/node"
)

func pre50Observe(n *node.Node) error {
	addr := os.Getenv("PRE50_DEBUG")
	if addr == "" {
		return nil
	}
	http.HandleFunc("/debug/leader", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]uint64{"id": n.ID(), "leader": n.Leader()}); err != nil {
			return
		}
	})
	http.HandleFunc("/debug/rates", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("on") == "1" {
			runtime.SetMutexProfileFraction(1)
			runtime.SetBlockProfileRate(1000000)
		} else {
			runtime.SetMutexProfileFraction(0)
			runtime.SetBlockProfileRate(0)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			log.Printf("debug HTTP: %v", err)
		}
	}()
	return nil
}
