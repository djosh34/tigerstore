package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "# HELP go_memstats_heap_inuse_bytes x\ngo_memstats_heap_inuse_bytes 2.097152e+08\ntigerstore_cache_bytes 1048576\ngo_goroutines 12\n")
	}))
	defer srv.Close()
	if got, want := heap(t.Context(), srv.Client(), srv.URL), "heap_inuse 200.0, tigerstore_cache 1.0"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
