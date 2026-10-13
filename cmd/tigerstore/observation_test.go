package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/osfs"
)

type shapeNoPeers struct{}

func (shapeNoPeers) Send(*raftpb.Message) {}
func (shapeNoPeers) Close() error         { return nil }

func shapeTestNode(t *testing.T, enabled bool) *node.Node {
	t.Helper()
	cfg := node.Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: shapeSampleInterval}
	if enabled {
		cfg.ObserveInterval = shapeSampleInterval
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := n.Run(ctx, shapeNoPeers{}); err != nil {
			t.Error(err)
		}
	})
	return n
}

func TestShapeDebugFlagAndLoopbackOnly(t *testing.T) {
	t.Setenv("SHAPE_DEBUG", "127.0.0.1:42001")
	base := []string{"--name=n1", "--initial-cluster=n1=https://127.0.0.1:2380"}
	f, err := parse(base)
	if err != nil || f.shapeDebug != "127.0.0.1:42001" {
		t.Fatalf("environment parse: %+v, %v", f, err)
	}
	f, err = parse(append(base, "--shape-debug="))
	if err != nil || f.shapeDebug != "" {
		t.Fatalf("flag did not disable environment: %+v, %v", f, err)
	}
	t.Setenv("SHAPE_DEBUG", "")
	f, err = parse(base)
	if err != nil || f.shapeDebug != "" {
		t.Fatalf("observation not off by default: %+v, %v", f, err)
	}
	ln, err := listenShapeDebug("")
	if err != nil || ln != nil {
		t.Fatalf("off listener = %v, %v", ln, err)
	}
	for _, addr := range []string{":0", "0.0.0.0:0", "[::]:0", "localhost:0", "192.0.2.1:0", "invalid"} {
		if ln, err := listenShapeDebug(addr); err == nil || ln != nil {
			t.Fatalf("non-loopback accepted: %q", addr)
		}
	}
	ln, err = listenShapeDebug("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShapeSnapshotSchemaAndNoCoreRoundTrip(t *testing.T) {
	n := shapeTestNode(t, true)
	h := shapeHandler(n, log.New(io.Discard, "", 0))
	for range 3 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/shape", nil))
		if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("HTTP snapshot: %d, %v", w.Code, w.Header())
		}
		var s shapeSnapshot
		if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
			t.Fatal(err)
		}
		if s.SchemaVersion != 1 || s.CapturedUnixNS <= 0 || s.SampleIntervalNS != int64(shapeSampleInterval) || !reflect.DeepEqual(s.WallNSBounds, osfs.WallNSBounds()) {
			t.Fatalf("schema: %+v", s)
		}
		if s.ObservationSnapshot == nil || s.Node != nil || s.NodeSampleAgeNS != -1 || s.Peers != nil || s.PeersSampleAgeNS != -1 || len(s.Files.Write.WallNSBuckets) != 12 || len(s.Files.Fsync.WallNSBuckets) != 12 {
			t.Fatalf("initial snapshot: %+v", s.ObservationSnapshot)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/debug/shape", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST accepted: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("pprof index unavailable: %d", w.Code)
	}
}

func TestShapeDisabledSnapshot(t *testing.T) {
	n := shapeTestNode(t, false)
	w := httptest.NewRecorder()
	shapeHandler(n, log.New(io.Discard, "", 0)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/shape", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled observation returned %d", w.Code)
	}
}
