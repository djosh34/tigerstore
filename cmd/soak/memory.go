package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// memoryLines is how many samples of its memory the soak keeps per node:
// two minutes, one a second.
const memoryLines = 120

// heapMetrics are the parts of a node's /metrics that a memory sample
// shows, in MiB.
var heapMetrics = []string{"go_memstats_heap_inuse_bytes", "go_memstats_next_gc_bytes", "tigerstore_cache_bytes", "tigerstore_cache_limit_bytes", "tigerstore_window_bytes", "tigerstore_index_bytes"}

// sampleMemory keeps each node's cgroup memory and the heap its /metrics
// report, once a second with the time, until ctx ends. A run that fails,
// such as on an OOM kill, ends at once, and every run writes the last two
// minutes of it per node (writeMemory), so that a kill's report shows
// what grew before it (#226).
func (s *soak) sampleMemory(ctx context.Context) {
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: s.c.tls}, Timeout: time.Second}
	defer hc.CloseIdleConnections()
	for ctx.Err() == nil {
		for i, n := range s.c.nodes {
			line, err := n.cg.Memory()
			if err != nil {
				line = err.Error()
			}
			line = time.Now().UTC().Format("15:04:05.000") + " " + line + "; " + heap(ctx, hc, "https://"+s.c.endpoints[i]+"/metrics")
			n.mu.Lock()
			if len(n.memory) == memoryLines {
				n.memory = n.memory[1:]
			}
			n.memory = append(n.memory, line)
			n.mu.Unlock()
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// heap reads the heap metrics of a node's /metrics at url.
func heap(ctx context.Context, hc *http.Client, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err.Error()
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "no metrics: " + err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	got := map[string]string{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		for _, m := range heapMetrics {
			if name == m {
				if v, err := strconv.ParseFloat(value, 64); err == nil {
					got[m] = fmt.Sprintf("%s %.1f", strings.TrimSuffix(strings.TrimPrefix(m, "go_memstats_"), "_bytes"), v/(1<<20))
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err.Error()
	}
	var parts []string
	for _, m := range heapMetrics {
		if p, ok := got[m]; ok {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ", ")
}

// writeMemory writes each node's last memory samples to the run's
// directory.
func (c *cluster) writeMemory(dir string) error {
	for i, n := range c.nodes {
		n.mu.Lock()
		body := strings.Join(n.memory, "\n") + "\n"
		n.mu.Unlock()
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("node%d-memory.txt", i+1)), []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}
