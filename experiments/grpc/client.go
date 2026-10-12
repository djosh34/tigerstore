package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func serverCPU() (float64, error) {
	r, e := http.Get("http://" + *control + "/stats")
	if e != nil {
		return 0, e
	}
	defer r.Body.Close()
	var v struct {
		CPU float64 `json:"cpu_seconds"`
	}
	e = json.NewDecoder(r.Body).Decode(&v)
	return v.CPU, e
}

type workerResult struct {
	n         int64
	hist      []int64
	err       error
	latencyNS int64
	maxNS     int64
}

func phase(clients []pb.KVClient, d time.Duration, measure bool) ([]workerResult, time.Duration) {
	results := make([]workerResult, *streams*len(clients))
	var wg sync.WaitGroup
	start := make(chan struct{})
	var deadline time.Time
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res := &results[i]
			if measure {
				res.hist = make([]int64, 20000)
			}
			req := &pb.PutRequest{Key: []byte(fmt.Sprintf("/registry/configmaps/bench/worker-%06d%s", i, strings.Repeat("x", 20))), Value: []byte(strings.Repeat("v", 1024))}
			ctx, cancel := context.WithTimeout(context.Background(), d+30*time.Second)
			defer cancel()
			c := clients[i / *streams]
			<-start
			for time.Now().Before(deadline) {
				t := time.Now()
				resp, e := c.Put(ctx, req)
				if e != nil {
					res.err = e
					return
				}
				if resp.Header == nil || resp.Header.Revision <= 1 {
					res.err = fmt.Errorf("bad Put response")
					return
				}
				res.n++
				if measure {
					ns := time.Since(t).Nanoseconds()
					res.latencyNS += ns
					if ns > res.maxNS {
						res.maxNS = ns
					}
					us := ns / 1000
					bin := us
					if us >= 10000 {
						bin = 10000 + (us-10000)/100
					}
					bin = min(bin, int64(len(res.hist)-1))
					res.hist[bin]++
				}
			}
		}(i)
	}
	t := time.Now()
	deadline = t.Add(d)
	close(start)
	wg.Wait()
	return results, time.Since(t)
}
func load() error {
	if *generator == "lean" {
		return leanLoad()
	}
	conns := make([]*grpc.ClientConn, *connections)
	clients := make([]pb.KVClient, *connections)
	for i := range conns {
		c, e := dial()
		if e != nil {
			return e
		}
		conns[i] = c
		defer c.Close()
		clients[i] = pb.NewKVClient(c)
	}
	warm, _ := phase(clients, *warmup, false)
	for _, r := range warm {
		if r.err != nil {
			return r.err
		}
	}
	s0, e := serverCPU()
	if e != nil {
		return e
	}
	c0 := cpuSeconds()
	results, wall := phase(clients, *duration, true)
	c1 := cpuSeconds()
	s1, e := serverCPU()
	if e != nil {
		return e
	}
	return emitLoadResult(results, wall, s1-s0, c1-c0)
}
func emitLoadResult(results []workerResult, wall time.Duration, serverSeconds, clientSeconds float64) error {
	hist := make([]int64, 20000)
	var n, ns, maxNS int64
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		n += r.n
		ns += r.latencyNS
		maxNS = max(maxNS, r.maxNS)
		for i, v := range r.hist {
			hist[i] += v
		}
	}
	quantile := func(q float64) int {
		target := int64(float64(n) * q)
		var sum int64
		for i, v := range hist {
			sum += v
			if sum >= target {
				if i >= 10000 {
					return 10000 + (i-10000)*100
				}
				return i
			}
		}
		return len(hist) - 1
	}
	out := map[string]any{"kind": *kind, "connections": *connections, "streams_per_connection": *streams, "value_bytes": 1024, "completed": n, "wall_seconds": wall.Seconds(), "throughput": float64(n) / wall.Seconds(), "generator": *generator, "server_cpu_seconds": serverSeconds, "server_cpu_us_op": serverSeconds * 1e6 / float64(n), "client_cpu_us_op": clientSeconds * 1e6 / float64(n), "latency_mean_us": float64(ns) / float64(n) / 1000, "latency_p50_us": quantile(.5), "latency_p90_us": quantile(.9), "latency_p99_us": quantile(.99), "latency_p999_us": quantile(.999), "latency_max_us": float64(maxNS) / 1000}
	return json.NewEncoder(stdout{}).Encode(out)
}

// Keep JSON on stdout and failures on stderr.
type stdout struct{}

func (stdout) Write(p []byte) (int, error) { return fmt.Print(string(p)) }
func smoke() error {
	cfg, e := tlsConfig(false)
	if e != nil {
		return e
	}
	c, e := clientv3.New(clientv3.Config{Endpoints: []string{"https://" + *addr}, TLS: cfg, DialTimeout: 5 * time.Second})
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "smoke-key"
	if _, e := c.Put(ctx, key, "first"); e != nil {
		return e
	}
	r, e := c.Get(ctx, key)
	if e != nil {
		return e
	}
	if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "first" {
		return fmt.Errorf("bad Range: %v", r)
	}
	watchCtx, stop := context.WithCancel(ctx)
	defer stop()
	ch := c.Watch(watchCtx, key, clientv3.WithCreatedNotify(), clientv3.WithPrevKV())
	select {
	case r := <-ch:
		if !r.Created || r.Err() != nil {
			return fmt.Errorf("watch creation: %v", r)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	if _, e := c.Put(ctx, key, "second"); e != nil {
		return e
	}
	select {
	case r := <-ch:
		if r.Err() != nil || len(r.Events) != 1 || string(r.Events[0].Kv.Value) != "second" || r.Events[0].PrevKv == nil || string(r.Events[0].PrevKv.Value) != "first" {
			return fmt.Errorf("bad watch event: %v", r)
		}
		fmt.Printf("clientv3 Put, Range, Watch create and update with prev_kv passed, revision %d\n", r.Header.Revision)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
