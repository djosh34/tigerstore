package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPProbesUseSharedHTTP2Port(t *testing.T) {
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"health":"true","reason":""}`
		if r.URL.Path == "/version" {
			body = `{"etcdserver":"3.7.2"}`
		}
		if _, err := fmt.Fprint(w, body); err != nil {
			t.Error(err)
		}
	}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	cfg := s.Client().Transport.(*http.Transport).TLSClientConfig
	p, err := newHTTPProbes(strings.TrimPrefix(s.URL, "https://"), cfg, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer p.transport.CloseIdleConnections()
	start, stop := make(chan struct{}), make(chan struct{})
	p.start(start, stop)
	close(start)
	time.Sleep(30 * time.Millisecond)
	close(stop)
	p.wg.Wait()
	if p.Errors != 0 || p.Requests == 0 {
		t.Fatalf("requests=%d errors=%d", p.Requests, p.Errors)
	}
}
