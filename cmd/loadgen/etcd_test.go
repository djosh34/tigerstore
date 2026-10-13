package main

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// startEtcd starts a single-node etcd from $LOADGEN_ETCD and returns its client
// endpoint and pid. Without -short the test fails if LOADGEN_ETCD is unset.
func startEtcd(t *testing.T) (string, int) {
	t.Helper()
	if testing.Short() {
		t.Skip("needs etcd")
	}
	bin := os.Getenv("LOADGEN_ETCD")
	if bin == "" {
		t.Fatal("set LOADGEN_ETCD to an etcd v3.7.2 binary, or run with -short")
	}
	client, peer := freePort(t), freePort(t)
	dir := t.TempDir()
	cmd := exec.Command(
		bin,
		"--name", "n1",
		"--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", "http://"+client,
		"--advertise-client-urls", "http://"+client,
		"--listen-peer-urls", "http://"+peer,
		"--initial-advertise-peer-urls", "http://"+peer,
		"--initial-cluster", "n1=http://"+peer,
		"--log-level", "warn",
	)
	log, err := os.Create(filepath.Join(dir, "etcd.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			b, _ := os.ReadFile(log.Name())
			t.Logf("etcd log:\n%s", b)
		}
	})
	for range 100 {
		if c, err := net.Dial("tcp", client); err == nil {
			_ = c.Close()
			return client, cmd.Process.Pid
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("etcd did not listen within 10s")
	return "", 0
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func TestEtcdWorkloads(t *testing.T) {
	ep, pid := startEtcd(t)
	for _, tc := range []struct {
		workload  string
		valueSize int
		rate      float64
	}{
		{"put", 1024, 0},
		{"put", 10240, 300},
		{"txn", 1024, 0},
		{"txn", 10240, 300},
		{"delete", 1024, 3000},
		{"range", 1024, 0},
		{"range", 10240, 300},
		{"list", 1024, 0},
		{"watch", 1024, 2000},
		{"mixed", 1024, 500},
		{"mixed", 1024, 0},
	} {
		endpoint, serverPID := ep, pid
		if tc.workload == "mixed" {
			endpoint, serverPID = startEtcd(t)
		}
		cfg := config{
			endpoints: []string{endpoint},
			workload:  tc.workload,
			valueSize: tc.valueSize,
			keys:      20000,
			conns:     2,
			streams:   32,
			rate:      tc.rate,
			warmup:    200 * time.Millisecond,
			duration:  500 * time.Millisecond,
			watchers:  3,
			page:      100,
			pids:      []int{serverPID},
			seed:      1,
		}
		res, err := run(cfg)
		if err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		// Latency covers the requests that arrived in the window, which
		// all succeeded.
		if res.Ops == 0 || res.Cohort == 0 || res.CohortFailed != 0 || res.Latency.Count != uint64(res.Cohort) || res.ServerCPUUsPerOp <= 0 || !res.Valid {
			t.Fatalf("%+v: %d ops, cohort %d with %d failed, %d latencies, %.1f us server CPU per op, valid %v",
				tc, res.Ops, res.Cohort, res.CohortFailed, res.Latency.Count, res.ServerCPUUsPerOp, res.Valid)
		}
		if tc.workload == "watch" && (res.Watch.Events < res.Ops*3/2 || res.Watch.Latency.Count == 0) {
			t.Fatalf("%+v: %d events for %d puts and 3 watchers", tc, res.Watch.Events, res.Ops)
		}
		t.Log(res.oneLine())
	}
}

// TestEtcdCASDetectsStaleRevision checks that a CAS against a stale mod
// revision is counted as an error, not as a completed update.
func TestEtcdCASDetectsStaleRevision(t *testing.T) {
	ep, _ := startEtcd(t)
	c, err := dial(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	ks := makeKeys(10)
	revs := make([]int64, ks.n)
	mk := func(m *msg) base {
		tpl, err := newTemplate(ep, kvPath("txn"), m, true)
		if err != nil {
			t.Fatal(err)
		}
		b := base{tpl: tpl, ks: ks, vals: *makeValues(64, 1), rng: wk.rng, stats: &wk.stats, limit: 4, writes: newRevisionSet()}
		for k := range ks.n {
			b.own = append(b.own, k)
		}
		return b
	}
	pre := mk(putRequest(ks.size, 64))
	pre.tpl, err = newTemplate(ep, kvPath("put"), putRequest(ks.size, 64), true)
	if err != nil {
		t.Fatal(err)
	}
	start := now()
	if err := wk.run(&putLoad{base: pre, once: true, revs: revs}, phase{start: start, stop: start + int64(5*time.Second), drain: time.Second}); err != nil {
		t.Fatal(err)
	}
	for i := range revs {
		revs[i]--
	}
	start = now()
	wk.stats.winStart, wk.stats.winStop = start, start+int64(time.Second)
	if err := wk.run(newCASLoad(mk(casRequest(ks.size, 64)), revs), phase{start: start, stop: start + int64(200*time.Millisecond), drain: time.Second}); err != nil {
		t.Fatal(err)
	}
	if wk.stats.ops != 0 || wk.stats.errs[errCASFailed.Error()] == 0 {
		t.Fatalf("ops %d, errors %v", wk.stats.ops, wk.stats.errs)
	}
}

// TestEtcdServerError checks that a gRPC error from etcd is counted.
func TestEtcdServerError(t *testing.T) {
	ep, _ := startEtcd(t)
	c, err := dial(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	wk := newWorker(c, 1)
	// A put with an empty key, which etcd rejects.
	tpl, err := newTemplate(ep, kvPath("put"), putRequest(0, 64), true)
	if err != nil {
		t.Fatal(err)
	}
	b := base{tpl: tpl, ks: keys{n: 1}, own: []int{0}, vals: *makeValues(64, 1), rng: wk.rng, stats: &wk.stats, limit: 1, writes: newRevisionSet()}
	start := now()
	if err := wk.run(&putLoad{base: b, once: true}, phase{start: start, stop: start + int64(time.Second), drain: time.Second}); err != nil {
		t.Fatal(err)
	}
	if len(wk.stats.errs) != 1 {
		t.Fatalf("errors %v", wk.stats.errs)
	}
	for e := range wk.stats.errs {
		if !strings.Contains(e, "InvalidArgument") || !strings.Contains(e, "key is not provided") {
			t.Fatalf("error %q", e)
		}
	}
}
