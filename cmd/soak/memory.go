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
	"sync"
	"time"
)

// The diagnostic sampler runs independently of /metrics: recovery and a
// stopped node must not hold up cgroup sampling. Keep the entire small time
// series, plus two minutes preceding the highest observed current and each
// change of the kernel high-water mark (which may be between samples).
const memoryLines = 1200

var heapMetrics = []string{"go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "go_memstats_heap_sys_bytes", "go_memstats_heap_idle_bytes", "go_memstats_heap_released_bytes", "go_memstats_sys_bytes", "go_memstats_stack_inuse_bytes", "go_memstats_next_gc_bytes", "go_gc_gomemlimit_bytes", "tigerstore_cache_bytes", "tigerstore_cache_limit_bytes", "tigerstore_window_bytes", "tigerstore_index_bytes"}

func (s *soak) sampleMemory(ctx context.Context) {
	var wg sync.WaitGroup
	for i, n := range s.c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.sampleNodeMemory(ctx, i, n)
		}()
	}
	wg.Wait()
}

func (s *soak) sampleNodeMemory(ctx context.Context, i int, n *node) {
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: s.c.tls}, Timeout: time.Second}
	defer hc.CloseIdleConnections()
	var metricsMu sync.Mutex
	var latest string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			stamp := time.Now().UTC().Format(time.RFC3339Nano)
			value := heap(ctx, hc, "https://"+s.c.endpoints[i]+"/metrics")
			metricsMu.Lock()
			latest = "metrics_at=" + stamp + " " + value
			metricsMu.Unlock()
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
	defer func() { <-done }()
	series, err := os.Create(filepath.Join(s.c.dir, fmt.Sprintf("node%d-memory-series.txt", i+1)))
	if err != nil {
		s.logf("memory evidence: %v", err)
		return
	}
	defer func() { _ = series.Close() }()
	out := bufio.NewWriter(series)
	defer func() { _ = out.Flush() }()
	var highest, kernelPeak int64
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for ctx.Err() == nil {
		read := func(name string) string {
			b, err := os.ReadFile(filepath.Join(n.cg.Path, name))
			if err != nil {
				return err.Error()
			}
			return strings.ReplaceAll(strings.TrimSpace(string(b)), "\n", ",")
		}
		cur, peak := read("memory.current"), read("memory.peak")
		current, _ := strconv.ParseInt(cur, 10, 64)
		pk, _ := strconv.ParseInt(peak, 10, 64)
		n.mu.Lock()
		pid, starts := 0, n.starts
		if n.cmd != nil && n.cmd.Process != nil {
			pid = n.cmd.Process.Pid
		}
		n.mu.Unlock()
		metricsMu.Lock()
		m := latest
		metricsMu.Unlock()
		line := fmt.Sprintf("%s pid=%d incarnation=%d current=%s peak=%s stat={%s} events={%s}; %s", time.Now().UTC().Format(time.RFC3339Nano), pid, starts, cur, peak, read("memory.stat"), read("memory.events"), m)
		_, _ = fmt.Fprintln(out, line)
		n.mu.Lock()
		if len(n.memory) == memoryLines {
			n.memory = n.memory[1:]
		}
		n.memory = append(n.memory, line)
		if current > highest || pk > kernelPeak {
			highest, kernelPeak = max(highest, current), max(kernelPeak, pk)
			body := strings.Join(n.memory, "\n") + "\n"
			_ = os.WriteFile(filepath.Join(s.c.dir, fmt.Sprintf("node%d-memory-peak.txt", i+1)), []byte(body), 0o644)
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
}

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
