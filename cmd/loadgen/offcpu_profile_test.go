package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestOffCPUCollectorBracketsProfilesAndTakesShortTrace(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.RequestURI())
		mu.Unlock()
		_, _ = fmt.Fprint(w, "fixture")
	}))
	defer server.Close()
	dir := t.TempDir()
	start := now() + int64(10*time.Millisecond)
	if err := collectOffCPU(server.Client(), server.URL, dir, start, start+int64(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"/debug/pprof/block", "/debug/pprof/mutex", "/metrics", "/debug/pprof/trace?seconds=2", "/debug/pprof/block", "/debug/pprof/mutex", "/metrics"}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests %v, want %v", requests, want)
	}
	for _, name := range []string{"before.block.pprof", "before.mutex.pprof", "before.metrics", "execution.trace", "after.block.pprof", "after.mutex.pprof", "after.metrics"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(data) != "fixture" {
			t.Fatalf("%s: data %q, error %v", name, data, err)
		}
	}
}

func TestOffCPUCollectorRejectsMissingEndpointAndShortWindow(t *testing.T) {
	t.Setenv("FETCH_PROFILE_OFFCPU_DIR", t.TempDir())
	if err := <-serverOffCPUProfile("http", nil, 0, 0); err == nil {
		t.Fatal("missing endpoint accepted")
	}
	if err := collectOffCPU(http.DefaultClient, "http://unused", t.TempDir(), 0, int64(time.Second)); err == nil {
		t.Fatal("short window accepted")
	}
}

func TestOffCPUProfilingIsOptIn(t *testing.T) {
	t.Setenv("FETCH_PROFILE_OFFCPU_DIR", "")
	t.Setenv("FETCH_PROFILE_LEADER", "")
	if err := <-serverProfile("http", nil, 0, 0); err != nil {
		t.Fatal(err)
	}
}
