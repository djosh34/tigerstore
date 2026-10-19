package main

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"
)

// TestDiag298WarmupSpansWindow reproduces #298 against real etcd. Holding
// responses fills the closed-loop slots in warmup. All succeed during drain,
// but no request arrives or completes in the 500 ms measurement window.
func TestDiag298WarmupSpansWindow(t *testing.T) {
	ep, pid := startEtcd(t)
	target, err := url.Parse("http://" + ep)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{Protocols: new(http.Protocols)}
	transport.Protocols.SetUnencryptedHTTP2(true)
	proxy.Transport = transport
	var once sync.Once
	release := make(chan struct{})
	proxy.ModifyResponse = func(resp *http.Response) error {
		once.Do(func() {
			time.AfterFunc(2*time.Second, func() { close(release) })
		})
		<-release
		return nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: proxy, Protocols: new(http.Protocols), ReadHeaderTimeout: 5 * time.Second}
	srv.Protocols.SetUnencryptedHTTP2(true)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Error(err)
		}
		if err := <-served; err != http.ErrServerClosed {
			t.Error(err)
		}
		transport.CloseIdleConnections()
	})
	res, err := run(config{
		endpoints: []string{ln.Addr().String()}, workload: "put", valueSize: 1024,
		keys: 20000, conns: 2, streams: 32, rate: 0,
		warmup: 200 * time.Millisecond, duration: 500 * time.Millisecond,
		pids: []int{pid}, seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("phase acknowledgements %d, window ops %d, cohort %d, failed %d, latencies %d, valid %v",
		res.PhaseOps, res.Ops, res.Cohort, res.CohortFailed, res.Latency.Count, res.Valid)
	if res.PhaseOps != 64 || res.Ops != 0 || res.Cohort != 0 || res.CohortFailed != 0 || res.Latency.Count != 0 || !res.Valid {
		t.Fatalf("did not reproduce the timing condition: %+v", res)
	}
	if err := checkWindow(res); err != nil {
		t.Fatal(err)
	}
}
