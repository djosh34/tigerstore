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
	"strconv"

	"github.com/djosh34/tigerstore/internal/expclock"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/osfs"
)

func pre50Observe(n *node.Node) error {
	addr := os.Getenv("PRE50_DEBUG")
	if addr == "" {
		return nil
	}
	http.HandleFunc("/debug/procs", func(w http.ResponseWriter, r *http.Request) {
		value, err := strconv.Atoi(r.URL.Query().Get("n"))
		if err != nil || value < 1 || value > 2 {
			http.Error(w, "expected n=1 or n=2", http.StatusBadRequest)
			return
		}
		previous := runtime.GOMAXPROCS(value)
		if err := json.NewEncoder(w).Encode(map[string]int{"previous": previous, "current": runtime.GOMAXPROCS(0)}); err != nil {
			return
		}
	})
	http.HandleFunc("/debug/trace/start", expclock.Start)
	http.HandleFunc("/debug/trace/stop", expclock.Stop)
	http.HandleFunc("/debug/leader", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(map[string]uint64{"id": n.ID(), "leader": n.Leader()}); err != nil {
			return
		}
	})
	http.HandleFunc("/debug/clock", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(n.ClockStats()); err != nil {
			return
		}
	})
	http.HandleFunc("/debug/disk", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(osfs.Pre50Stats()); err != nil {
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
