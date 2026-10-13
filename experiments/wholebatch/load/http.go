package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// One persistent HTTP/2 connection probes the same TLS client endpoint. These
// requests are deliberately inside the Put CPU and elapsed windows.
type httpProbes struct {
	client    *http.Client
	transport *http.Transport
	endpoint  string
	interval  time.Duration
	wg        sync.WaitGroup
	Requests  uint64 `json:"requests"`
	Errors    uint64 `json:"errors"`
}

func newHTTPProbes(endpoint string, cfg *tls.Config, interval time.Duration) (*httpProbes, error) {
	tr := &http.Transport{TLSClientConfig: cfg.Clone(), ForceAttemptHTTP2: true}
	p := &httpProbes{client: &http.Client{Transport: tr}, transport: tr, endpoint: "https://" + endpoint, interval: interval}
	// Warm the connection before any CPU/allocation counters are sampled.
	for _, path := range []string{"/health", "/version"} {
		if err := p.get(path); err != nil {
			tr.CloseIdleConnections()
			return nil, err
		}
	}
	return p, nil
}

func (p *httpProbes) get(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+path, nil)
	if err != nil {
		return err
	}
	r, err := p.client.Do(req)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(io.LimitReader(r.Body, 8192))
	closeErr := r.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if r.StatusCode != http.StatusOK || r.ProtoMajor != 2 {
		return fmt.Errorf("HTTP probe %s: status=%d protocol=%s", path, r.StatusCode, r.Proto)
	}
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		return err
	}
	if path == "/health" && result["health"] != "true" {
		return fmt.Errorf("health probe: %s", body)
	}
	if path == "/version" && result["etcdserver"] != "3.7.2" {
		return fmt.Errorf("version probe: %s", body)
	}
	return nil
}

func (p *httpProbes) start(start, stop <-chan struct{}) {
	p.wg.Go(func() {
		<-start
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				path := "/health"
				if p.Requests%2 != 0 {
					path = "/version"
				}
				p.Requests++
				if p.get(path) != nil {
					p.Errors++
				}
			}
		}
	})
}
