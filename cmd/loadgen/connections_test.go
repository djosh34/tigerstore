package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConnectionControlFlags(t *testing.T) {
	base := []string{"-dispatch", "connections", "-concurrency", "256", "-conns", "16"}
	cfg, err := parseFlags(base)
	if err != nil || !cfg.shaped() || cfg.dispatch != "connections" {
		t.Fatalf("valid control rejected: %+v, %v", cfg, err)
	}
	for _, extra := range [][]string{
		{"-concurrency", "255"},
		{"-concurrency", "0"},
		{"-conns", "18"},
		{"-endpoints", "127.0.0.1:2379,127.0.0.1:2380"},
		{"-arrival", "fixed", "-rate", "100"},
		{"-rate", "100"},
		{"-total", "10"},
		{"-dispatch", "unknown"},
	} {
		args := append(append([]string{}, base...), extra...)
		if _, err := parseFlags(args); err == nil {
			t.Errorf("accepted invalid control flags %v", args)
		}
	}
}

func TestConnectionControlBarriers(t *testing.T) {
	fs := startFake(t, 2*time.Millisecond, time.Second)
	prefix := filepath.Join(t.TempDir(), "control")
	cfg := config{
		endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 24, conns: 2, streams: 256,
		dispatch: "connections", concurrency: 4, duration: 60 * time.Millisecond, warmup: 20 * time.Millisecond,
		drain: time.Second, barrier: prefix, barrierWait: 2 * time.Second, pids: []int{os.Getpid()}, seed: 1,
	}
	type outcome struct {
		res *result
		err error
	}
	finished := make(chan outcome, 1)
	go func() {
		res, err := run(cfg)
		finished <- outcome{res, err}
	}()
	waitMarker := func(suffix string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, err := os.Stat(prefix + "." + suffix); err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("no control marker %s", suffix)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitMarker("ready")
	readyRev := fs.rev.Load()
	time.Sleep(10 * time.Millisecond)
	if fs.rev.Load() != readyRev {
		t.Fatal("control warmup was not drained before ready")
	}
	if err := os.WriteFile(prefix+".go", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitMarker("done")
	doneRev := fs.rev.Load()
	time.Sleep(10 * time.Millisecond)
	if fs.rev.Load() != doneRev {
		t.Fatal("control sent RPCs after done")
	}
	select {
	case <-finished:
		t.Fatal("control returned before finish")
	default:
	}
	if err := os.WriteFile(prefix+".finish", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got := <-finished
	if got.err != nil || got.res == nil || !got.res.Valid {
		t.Fatalf("control result %+v, error %v", got.res, got.err)
	}
	r, shape := got.res, got.res.Shape
	if shape.Dispatch != "static-connections" || shape.Concurrency != 4 || shape.PeakInflight != 4 || r.StreamsPerConn != 2 {
		t.Fatalf("wrong control configuration: %+v, %+v", r, shape)
	}
	if shape.Expected != doneRev-readyRev || r.Cohort != shape.Expected || shape.Success != r.Cohort || r.Latency.Count != uint64(r.Cohort) || shape.ExpectedRevDelta != r.Cohort {
		t.Fatalf("seed or warmup entered cohort: delta %d, %+v, %+v", doneRev-readyRev, r, shape)
	}
	if r.CohortFailed != 0 || r.Unsent != 0 || len(r.Errors) != 0 || shape.CPUThroughDrain == nil || shape.CPUThroughDrain.ElapsedS < cfg.duration.Seconds() {
		t.Fatalf("control lacks validated drain/CPU: %+v", r)
	}
	ep := shape.Endpoints[0]
	if ep.Sent != r.Cohort || ep.Success != r.Cohort || ep.Failed != 0 || ep.Unsent != 0 {
		t.Fatalf("control endpoint counts %+v", ep)
	}
	if shape.QueueAtStop != 0 || shape.QueueMax != 0 || len(shape.Queue) != 3 || shape.Queue[2].Inflight != 0 {
		t.Fatalf("control reported an open-loop backlog: %+v", shape)
	}
}

func TestConnectionControlTimeout(t *testing.T) {
	fs := startFake(t, 30*time.Millisecond, time.Second)
	res, err := run(config{
		endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 8, conns: 2,
		streams: 256, dispatch: "connections", concurrency: 2, duration: 5 * time.Millisecond, drain: 20 * time.Millisecond,
		pids: []int{os.Getpid()}, seed: 1,
	})
	if err == nil || res == nil || res.Valid || res.CohortFailed == 0 {
		t.Fatalf("unfinished control requests disappeared: %+v, %v", res, err)
	}
	if res.Cohort != res.Shape.Expected || res.Shape.Success+res.CohortFailed != res.Shape.Expected || res.Latency.Count != uint64(res.Shape.Success) {
		t.Fatalf("control timeout accounting mismatch: %+v, %+v", res, res.Shape)
	}
}
