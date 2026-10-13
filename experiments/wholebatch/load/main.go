// A closed-loop, direct etcd protobuf client used unchanged for both servers.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type cpu struct {
	PID          int
	Ticks, Start uint64
}

func sample(pid int) (cpu, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return cpu{}, e
	}
	pos := strings.LastIndex(string(b), ")")
	if pos < 0 {
		return cpu{}, errors.New("bad proc stat")
	}
	s := strings.Fields(string(b[pos+1:]))
	u, e := strconv.ParseUint(s[11], 10, 64)
	if e != nil {
		return cpu{}, e
	}
	k, e := strconv.ParseUint(s[12], 10, 64)
	if e != nil {
		return cpu{}, e
	}
	st, e := strconv.ParseUint(s[19], 10, 64)
	return cpu{pid, u + k, st}, e
}

func run() error {
	endpoint := flag.String("endpoint", "127.0.0.1:2379", "leader endpoint")
	certdir := flag.String("certdir", "", "cert directory")
	op := flag.String("op", "put", "put or range")
	total := flag.Uint64("total", 60000, "operations, zero for duration")
	duration := flag.Duration("duration", 0, "arrival duration, drain included in elapsed")
	workers := flag.Int("workers", 256, "closed-loop concurrency")
	conns := flag.Int("conns", 16, "connections")
	pidstr := flag.String("pids", "", "leader,follower1,follower2")
	allocURLs := flag.String("alloc-urls", "", "allocation counter URLs in server role order")
	healthInterval := flag.Duration("health-interval", 0, "alternate health/version GET interval on same TLS port, zero disables")
	hz := flag.Float64("hz", 100, "getconf CLK_TCK")
	out := flag.String("out", "", "json output")
	flag.Parse()
	if (*op != "put" && *op != "range") || *workers < 1 || *conns < 1 || (*total == 0 && *duration <= 0) {
		return errors.New("invalid flags")
	}
	ca, e := os.ReadFile(*certdir + "/ca.pem")
	if e != nil {
		return e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return errors.New("bad CA")
	}
	cert, e := tls.LoadX509KeyPair(*certdir+"/client.pem", *certdir+"/client-key.pem")
	if e != nil {
		return e
	}
	cfg := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}
	clients := make([]pb.KVClient, *conns)
	for i := range clients {
		c, e := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(credentials.NewTLS(cfg.Clone())))
		if e != nil {
			return e
		}
		defer func() {
			if err := c.Close(); err != nil {
				log.Printf("close client: %v", err)
			}
		}()
		clients[i] = pb.NewKVClient(c)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, e = clients[i].Range(ctx, &pb.RangeRequest{Key: []byte("/pre50-probe")})
		cancel()
		if e != nil {
			return e
		}
	}
	const keyspace = 4096
	keys := make([][]byte, keyspace)
	values := make([][]byte, keyspace)
	for i := range keyspace {
		keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%08d", i%64, i))
		values[i] = make([]byte, 1024)
		for j := range 32 {
			s := sha256.Sum256([]byte(fmt.Sprintf("pre50-independent-%d-%d", i, j)))
			copy(values[i][j*32:], s[:])
		}
	}
	probe := func() (*pb.RangeResponse, error) {
		ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		return clients[0].Range(ctx, &pb.RangeRequest{Key: keys[0]})
	}
	before, e := probe()
	if e != nil {
		return e
	}
	var probes *httpProbes
	if *healthInterval > 0 {
		probes, e = newHTTPProbes(*endpoint, cfg, *healthInterval)
		if e != nil {
			return e
		}
		defer probes.transport.CloseIdleConnections()
	}
	pids := []int{}
	if *pidstr != "" {
		for p := range strings.SplitSeq(*pidstr, ",") {
			id, e := strconv.Atoi(p)
			if e != nil {
				return e
			}
			pids = append(pids, id)
		}
	}
	pids = append(pids, os.Getpid())
	pre := make([]cpu, len(pids))
	post := make([]cpu, len(pids))
	var next atomic.Uint64
	var wg sync.WaitGroup
	start := make(chan struct{})
	stopHTTP := make(chan struct{})
	if probes != nil {
		probes.start(start, stopHTTP)
	}
	latencies := make([][]int64, *workers)
	errs := make([]map[string]int, *workers)
	terms := make([]map[uint64]int, *workers)
	var deadline time.Time
	for w := range *workers {
		wg.Go(func() {
			<-start
			local := make([]int64, 0, 1024)
			em := map[string]int{}
			tm := map[uint64]int{}
			for {
				i := next.Add(1) - 1
				if (*total > 0 && i >= *total) || (!deadline.IsZero() && !time.Now().Before(deadline)) {
					break
				}
				k := int((i * 2654435761) % keyspace)
				t := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var err error
				var h *pb.ResponseHeader
				if *op == "put" {
					var r *pb.PutResponse
					r, err = clients[w%*conns].Put(ctx, &pb.PutRequest{Key: keys[k], Value: values[k]})
					if r != nil {
						h = r.Header
					}
				} else {
					var r *pb.RangeResponse
					r, err = clients[w%*conns].Range(ctx, &pb.RangeRequest{Key: keys[k]})
					if r != nil {
						h = r.Header
						if err == nil && (r.Count != 1 || len(r.Kvs) != 1 || !bytes.Equal(r.Kvs[0].Key, keys[k]) || !bytes.Equal(r.Kvs[0].Value, values[k])) {
							err = errors.New("invalid range result")
						}
					}
				}
				cancel()
				dt := time.Since(t).Nanoseconds()
				if err == nil && (h == nil || h.Revision < 1) {
					err = errors.New("invalid header")
				}
				if err != nil {
					em[err.Error()]++
				} else {
					local = append(local, dt)
					tm[h.RaftTerm]++
				}
			}
			latencies[w] = local
			errs[w] = em
			terms[w] = tm
		})
	}
	allocBefore, e := sampleAllocations(*allocURLs)
	if e != nil {
		return e
	}
	for i, p := range pids {
		pre[i], e = sample(p)
		if e != nil {
			return e
		}
	}
	t := time.Now()
	if *duration > 0 {
		deadline = t.Add(*duration)
	}
	close(start)
	wg.Wait()
	close(stopHTTP)
	if probes != nil {
		probes.wg.Wait()
	}
	elapsed := time.Since(t).Seconds()
	for i, p := range pids {
		post[i], e = sample(p)
		if e != nil {
			return e
		}
		if pre[i].Start != post[i].Start {
			return errors.New("process restarted")
		}
	}
	allocAfter, e := sampleAllocations(*allocURLs)
	if e != nil {
		return e
	}
	after, e := probe()
	if e != nil {
		return e
	}
	all := []int64{}
	em := map[string]int{}
	tm := map[uint64]int{}
	for i := range *workers {
		all = append(all, latencies[i]...)
		for k, n := range errs[i] {
			em[k] += n
		}
		for k, n := range terms[i] {
			tm[k] += n
		}
	}
	if len(all) == 0 {
		return errors.New("no successes")
	}
	slices.Sort(all)
	pct := func(p float64) float64 { return float64(all[int(math.Ceil(p*float64(len(all))))-1]) / 1e6 }
	sum := float64(0)
	for _, v := range all {
		sum += float64(v)
	}
	cpus := make([]float64, len(pids))
	occup := make([]float64, len(pids))
	for i := range pids {
		secs := float64(post[i].Ticks-pre[i].Ticks) / *hz
		cpus[i] = secs * 1e6 / float64(len(all))
		occup[i] = secs / elapsed
	}
	delta := after.Header.Revision - before.Header.Revision
	valid := len(em) == 0 && len(tm) == 1 && (*total == 0 || uint64(len(all)) == *total) && ((*op == "put" && delta == int64(len(all))) || (*op == "range" && delta == 0))
	result := map[string]any{"valid": valid, "op": *op, "successes": len(all), "errors": em, "terms": tm, "elapsed_s": elapsed, "ops_s": float64(len(all)) / elapsed, "mean_ms": sum / float64(len(all)) / 1e6, "p50_ms": pct(.5), "p90_ms": pct(.9), "p95_ms": pct(.95), "p99_ms": pct(.99), "p999_ms": pct(.999), "max_ms": float64(all[len(all)-1]) / 1e6, "cpu_us_op": cpus, "alloc_before": allocBefore, "alloc_after": allocAfter, "alloc_urls": *allocURLs, "occupied_cores": occup, "cpu_before": pre, "cpu_after": post, "revision_before": before.Header.Revision, "revision_after": after.Header.Revision, "workers": *workers, "conns": *conns, "keyspace": keyspace, "value_bytes": 1024, "clock_hz": *hz, "start_utc": t.UTC().Format(time.RFC3339Nano)}
	if probes != nil {
		valid = valid && probes.Errors == 0 && probes.Requests > 0
		result["valid"] = valid
		result["http_probes"] = probes
		result["http_probe_interval"] = healthInterval.String()
	}
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(*out, append(b, '\n'), 0o644); e != nil {
		return e
	}
	if !valid {
		return errors.New("invalid measurement; see JSON")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
