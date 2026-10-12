package main

import (
	"bytes"
	"compress/gzip"
	"testing"
	"time"
)

func TestReviewUnaryCardinality(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	for _, path := range []string{"/empty", "/double"} {
		s := fakeRun(t, fs, path, 1024, 100, 100*time.Millisecond)
		t.Logf("%s accepted as success: ops=%d acked=%d errors=%v", path, s.ops, s.acked, s.errs)
		if s.ops == 0 || len(s.errs) != 0 {
			t.Fatalf("unexpected behavior: %+v", s)
		}
	}
}

func TestReviewTailAccounting(t *testing.T) {
	fs := startFake(t, 200*time.Millisecond, time.Second)
	c, err := dial(fs.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	ks := makeKeys(100)
	tpl, err := newTemplate(c.addr, "/ok", putRequest(ks.size, 1024), true)
	if err != nil {
		t.Fatal(err)
	}
	b := base{tpl: tpl, ks: ks, vals: *makeValues(1024, 1), rng: wk.rng, stats: &wk.stats, limit: 64}
	for k := range ks.n {
		b.own = append(b.own, k)
	}
	start := now()
	stop := start + int64(100*time.Millisecond)
	wk.stats.winStart, wk.stats.winStop = start, stop
	err = wk.run(&putLoad{base: b}, phase{rate: 50, start: start, stop: stop, drain: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r := summarize(config{duration: 100 * time.Millisecond}, []*worker{wk}, nil, [2]cpuSample{})
	t.Logf("200ms response delay, 100ms window, rate=50: ops=%d acked=%d unsent=%d latency_count=%d p99=%g errors=%v", r.Ops, wk.stats.acked, r.Unsent, r.Latency.Count, r.Latency.P99, r.Errors)
	if wk.stats.acked == 0 || r.Ops != 0 || r.Unsent != 0 || r.Latency.Count != 0 {
		t.Fatal("unexpected behavior")
	}
}

func TestReviewClosedLoopStartsEarly(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	c, err := dial(fs.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	ks := makeKeys(1)
	tpl, err := newTemplate(c.addr, "/ok", putRequest(ks.size, 64), true)
	if err != nil {
		t.Fatal(err)
	}
	b := base{tpl: tpl, ks: ks, own: []int{0}, vals: *makeValues(64, 1), rng: wk.rng, stats: &wk.stats, limit: 1}
	start := now() + int64(200*time.Millisecond)
	err = wk.run(&putLoad{base: b, once: true}, phase{start: start, stop: start + int64(time.Second), drain: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	early := time.Duration(start - now())
	t.Logf("closed-loop request completed %v before scheduled start; acked=%d", early, wk.stats.acked)
	if early <= 0 || wk.stats.acked != 1 {
		t.Fatal("unexpected behavior")
	}
}

func TestReviewCPUFailureHasEmptyErrorMap(t *testing.T) {
	fs := startFake(t, 0, time.Second)
	cfg := config{endpoints: []string{fs.addr}, workload: "put", valueSize: 1024, keys: 100, conns: 1, streams: 8, rate: 100, warmup: 0, duration: 100 * time.Millisecond, pids: []int{999999999}, seed: 1}
	r, err := run(cfg)
	if r == nil {
		t.Fatalf("missing result: %v", err)
	}
	t.Logf("missing server PID: run_error=%v ops=%d JSON_errors=%v server_cpu_us_per_op=%g", err, r.Ops, r.Errors, r.ServerCPUUsPerOp)
	if err == nil || len(r.Errors) != 0 || r.ServerCPUUsPerOp != 0 {
		t.Fatal("unexpected behavior")
	}
}

func TestReviewPoolOverlap(t *testing.T) {
	const size = 10240
	v := makeValues(size, 1)
	first := bytes.Clone(v.next())
	second := v.next()
	if !bytes.Equal(first[4099:], second[:size-4099]) {
		t.Fatal("expected shared pool overlap")
	}
	compress := func(raw []byte) int {
		var dst bytes.Buffer
		z := gzip.NewWriter(&dst)
		if _, err := z.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
		return dst.Len()
	}
	v.off = 0
	var raw []byte
	for range 256 {
		raw = append(raw, v.next()...)
	}
	ratio := float64(compress(raw)) / float64(len(raw))
	independentRatio := float64(compress(v.pool[:256*size])) / float64(256*size)
	t.Logf("adjacent 10KiB values share %d bytes; gzip batch ratio=%g, non-overlapping random reference ratio=%g", size-4099, ratio, independentRatio)
}

func TestReviewNonfiniteRateAccepted(t *testing.T) {
	for _, v := range []string{"NaN", "+Inf"} {
		cfg, err := parseFlags([]string{"-rate", v})
		t.Logf("-rate %s accepted: %v, error=%v", v, cfg.rate, err)
		if err != nil {
			t.Fatal(err)
		}
	}
}
