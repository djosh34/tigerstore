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

// diag271DelayedDeletes forwards to real etcd, but holds the first delete
// responses until 800 ms after the first delete. Warmup plus measurement is
// only 700 ms. Later responses are not delayed, so the backlog can drain.
// This controls the timing condition seen in #271, not etcd's internal cause.
func diag271DelayedDeletes(t *testing.T, endpoint string) string {
	t.Helper()
	target, err := url.Parse("http://" + endpoint)
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
		if resp.Request.URL.Path == kvPath("delete") {
			once.Do(func() {
				time.AfterFunc(800*time.Millisecond, func() { close(release) })
			})
			<-release
		}
		return nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: proxy, Protocols: new(http.Protocols), ReadHeaderTimeout: 5 * time.Second}
	srv.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		transport.CloseIdleConnections()
	})
	return ln.Addr().String()
}
