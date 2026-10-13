// A closed-loop Put-only client. grpc-go is used only for unmeasured Range probes.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
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

const (
	keyspace         = 4096
	valueBytes       = 1024
	maxSamples       = 4_000_000
	ioTimeout        = 10 * time.Second
	campaignDeadline = "2026-10-13T22:00:00Z"
	sourceSHA        = "d2408301966b99471bfed06d4c2dc1cd1e5af462"
)

type options struct {
	endpoint, certdir, op, pidstr, out, barrierDir string
	total                                          uint64
	duration                                       time.Duration
	workers, conns                                 int
	hz                                             float64
}

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("leanload", flag.ContinueOnError)
	f.StringVar(&o.endpoint, "endpoint", "127.0.0.1:2379", "leader endpoint")
	f.StringVar(&o.certdir, "certdir", "", "cert directory")
	f.StringVar(&o.op, "op", "put", "put only; range is not a measured workload")
	f.Uint64Var(&o.total, "total", 60000, "operations, zero for duration")
	f.DurationVar(&o.duration, "duration", 0, "arrival duration, drain included in elapsed")
	f.IntVar(&o.workers, "workers", 256, "closed-loop concurrency")
	f.IntVar(&o.conns, "conns", 16, "connections")
	f.StringVar(&o.pidstr, "pids", "", "leader,follower1,follower2")
	f.Float64Var(&o.hz, "hz", 100, "getconf CLK_TCK")
	f.StringVar(&o.out, "out", "", "json output")
	f.StringVar(&o.barrierDir, "barrier-dir", "", "controller ready/start and drained/release files")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 || o.op != "put" || o.workers < 1 || o.workers > 4096 || o.conns < 1 || o.conns > 256 || o.conns > o.workers || o.total > maxSamples || o.duration < 0 || (o.total == 0 && o.duration == 0) || o.hz <= 0 || math.IsNaN(o.hz) || math.IsInf(o.hz, 0) || o.out == "" {
		return o, errors.New("invalid flags")
	}
	return o, nil
}

type cpu struct {
	PID          int
	Ticks, Start uint64
}

func parseCPU(pid int, b []byte) (cpu, error) {
	pos := strings.LastIndex(string(b), ")")
	if pos < 0 {
		return cpu{}, errors.New("bad proc stat")
	}
	s := strings.Fields(string(b[pos+1:]))
	if len(s) < 20 {
		return cpu{}, errors.New("short proc stat")
	}
	u, err := strconv.ParseUint(s[11], 10, 64)
	if err != nil {
		return cpu{}, err
	}
	k, err := strconv.ParseUint(s[12], 10, 64)
	if err != nil {
		return cpu{}, err
	}
	st, err := strconv.ParseUint(s[19], 10, 64)
	if err != nil {
		return cpu{}, err
	}
	if u > ^uint64(0)-k {
		return cpu{}, errors.New("CPU tick overflow")
	}
	return cpu{pid, u + k, st}, nil
}

func sample(pid int) (cpu, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return cpu{}, err
	}
	return parseCPU(pid, b)
}

// These are the same controller fences as experiments/pre50next/load.
func barrier(dir, signal, release string) error {
	if dir == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(dir, signal), nil, 0o644); err != nil {
		return err
	}
	end := time.Now().Add(30 * time.Second)
	for time.Now().Before(end) {
		if _, err := os.Stat(filepath.Join(dir, release)); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("barrier %s timed out", release)
}

func clientTLS(dir string) (*tls.Config, error) {
	ca, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("bad CA")
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "client.pem"), filepath.Join(dir, "client-key.pem"))
	if err != nil {
		return nil, err
	}
	return &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}, nil
}

func workload() ([][]byte, [][]byte) {
	keys, values := make([][]byte, keyspace), make([][]byte, keyspace)
	for i := range keyspace {
		keys[i] = []byte(fmt.Sprintf("/registry/pods/ns-%03d/pod-%08d", i%64, i))
		values[i] = make([]byte, valueBytes)
		for j := range 32 {
			s := sha256.Sum256([]byte(fmt.Sprintf("pre50-independent-%d-%d", i, j)))
			copy(values[i][j*32:], s[:])
		}
	}
	return keys, values
}

// Close the probe transport before the measured window. grpc-go may finish
// asynchronous cleanup afterward; all Put traffic uses the lean transport.
func probe(endpoint string, cfg *tls.Config, key []byte) (*pb.RangeResponse, error) {
	c, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(cfg.Clone())))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	defer cancel()
	r, err := pb.NewKVClient(c).Range(ctx, &pb.RangeRequest{Key: key})
	closeErr := c.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if r == nil || r.Header == nil || r.Header.Revision < 1 || r.Header.RaftTerm == 0 {
		return nil, errors.New("invalid Range probe header")
	}
	return r, nil
}

type measurement struct {
	latencyNS, revision int64
	term                uint64
}

type connectionResult struct {
	samples []measurement
	err     error
}

// Owners share an exact ticket limit. A ticket is taken only when a free stream
// can be admitted, not for an idle worker's attempt to find work.
type budget struct {
	next     atomic.Uint64
	total    uint64
	deadline time.Time
}

func (b *budget) available(now time.Time) bool {
	return (b.deadline.IsZero() || now.Before(b.deadline)) && b.next.Load() < b.limit()
}
func (b *budget) limit() uint64 {
	if b.total != 0 {
		return b.total
	}
	return maxSamples
}
func (b *budget) claim() (uint64, bool) {
	for {
		if !b.available(time.Now()) {
			return 0, false
		}
		n := b.next.Load()
		if n >= b.limit() {
			return 0, false
		}
		if b.next.CompareAndSwap(n, n+1) {
			return n, true
		}
	}
}

func processIDs(text string) ([]int, error) {
	var pids []int
	seen := map[int]bool{os.Getpid(): true}
	if text != "" {
		for p := range strings.SplitSeq(text, ",") {
			id, err := strconv.Atoi(p)
			if err != nil || id <= 0 || seen[id] {
				return nil, errors.New("invalid or duplicate server PID")
			}
			seen[id] = true
			pids = append(pids, id)
		}
		if len(pids) != 3 {
			return nil, errors.New("pids must name all three servers in role order")
		}
	}
	return append(pids, os.Getpid()), nil
}

func validateCohort(all []measurement, admitted, total uint64, before, after *pb.ResponseHeader) error {
	if len(all) == 0 {
		return errors.New("no successes")
	}
	if uint64(len(all)) != admitted || (total != 0 && admitted != total) {
		return errors.New("success/admission/total count mismatch")
	}
	if before == nil || after == nil || before.RaftTerm == 0 || before.RaftTerm != after.RaftTerm || before.ClusterId != after.ClusterId || before.MemberId != after.MemberId {
		return errors.New("probe identity or term changed")
	}
	if after.Revision-before.Revision != int64(len(all)) {
		return errors.New("revision delta differs from successes")
	}
	// Sorting happens after the CPU window. A delta alone would miss two replies
	// that claim the same revision and omit another revision.
	slices.SortFunc(all, func(a, b measurement) int { return cmpInt64(a.revision, b.revision) })
	for i, s := range all {
		if s.term != before.RaftTerm || s.revision != before.Revision+int64(i)+1 {
			return errors.New("duplicate/missing revision or response term changed")
		}
	}
	return nil
}
func cmpInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func run(args []string) error {
	o, err := parseOptions(args)
	if err != nil {
		return err
	}
	cutoff, err := time.Parse(time.RFC3339, campaignDeadline)
	if err != nil {
		return err
	}
	if !time.Now().Before(cutoff) {
		return errors.New("campaign deadline reached; start nothing")
	}
	cfg, err := clientTLS(o.certdir)
	if err != nil {
		return err
	}
	keys, values := workload()
	requests, err := encodeRequests(keys, values)
	if err != nil {
		return err
	}
	pids, err := processIDs(o.pidstr)
	if err != nil {
		return err
	}
	clients := make([]*leanClient, o.conns)
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.close()
			}
		}
		for _, c := range clients {
			if c != nil {
				<-c.readDone
			}
		}
	}()
	for i := range clients {
		if !time.Now().Before(cutoff) {
			return errors.New("campaign deadline reached")
		}
		slots := o.workers / o.conns
		if i < o.workers%o.conns {
			slots++
		}
		clients[i], err = newLeanClient(o.endpoint, cfg, requests, slots)
		if err != nil {
			return err
		}
	}
	before, err := probe(o.endpoint, cfg, keys[0])
	if err != nil {
		return err
	}
	pre, post := make([]cpu, len(pids)), make([]cpu, len(pids))
	b := &budget{total: o.total}
	results := make([]connectionResult, len(clients))
	start := make(chan struct{})
	abort := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			<-start
			results[i] = c.run(b, abort)
			if results[i].err != nil {
				once.Do(func() { close(abort) })
			}
		})
	}
	// Always release parked owners if setup or a barrier fails.
	defer func() {
		once.Do(func() { close(abort) })
		select {
		case <-start:
		default:
			close(start)
		}
		wg.Wait()
	}()
	if err = barrier(o.barrierDir, "ready", "start"); err != nil {
		return err
	}
	if !time.Now().Before(cutoff) {
		return errors.New("campaign deadline reached before window")
	}
	for i, pid := range pids {
		pre[i], err = sample(pid)
		if err != nil {
			return err
		}
	}
	t := time.Now()
	if o.duration > 0 {
		b.deadline = t.Add(o.duration)
	}
	// Do not admit requests past the campaign deadline even in a long invocation.
	if b.deadline.IsZero() || b.deadline.After(cutoff) {
		b.deadline = cutoff
	}
	close(start)
	wg.Wait()
	elapsed := time.Since(t).Seconds()
	for i, pid := range pids {
		post[i], err = sample(pid)
		if err != nil {
			return err
		}
		if pre[i].Start != post[i].Start || pre[i].Ticks > post[i].Ticks {
			return errors.New("process restarted or CPU ticks regressed")
		}
	}
	for _, c := range clients {
		c.close()
	}
	for _, c := range clients {
		<-c.readDone
	}
	if err = barrier(o.barrierDir, "drained", "release"); err != nil {
		return err
	}
	after, err := probe(o.endpoint, cfg, keys[0])
	if err != nil {
		return err
	}
	all := make([]measurement, 0)
	em, tm := map[string]int{}, map[uint64]int{}
	for _, r := range results {
		all = append(all, r.samples...)
		if r.err != nil {
			em[r.err.Error()]++
		}
		for _, s := range r.samples {
			tm[s.term]++
		}
	}
	if o.total == 0 && b.next.Load() == maxSamples {
		em["sample budget exhausted"]++
	}
	if err = validateCohort(all, b.next.Load(), o.total, before.Header, after.Header); err != nil {
		em[err.Error()]++
	}
	valid := len(em) == 0
	slices.SortFunc(all, func(a, b measurement) int { return cmpInt64(a.latencyNS, b.latencyNS) })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(math.Ceil(p*float64(len(all))))-1].latencyNS) / 1e6
	}
	var sum float64
	for _, s := range all {
		sum += float64(s.latencyNS)
	}
	cpus, occup := make([]float64, len(pids)), make([]float64, len(pids))
	var serverCPU float64
	for i := range pids {
		secs := float64(post[i].Ticks-pre[i].Ticks) / o.hz
		if len(all) != 0 {
			cpus[i] = secs * 1e6 / float64(len(all))
		}
		occup[i] = secs / elapsed
		if i < len(pids)-1 {
			serverCPU += cpus[i]
		}
	}
	var mean float64
	if len(all) != 0 {
		mean = sum / float64(len(all)) / 1e6
	}
	result := map[string]any{
		"client_kind":         "lean HTTP/2 Framer/HPACK Put; grpc-go Range probes outside CPU window",
		"controller_barriers": o.barrierDir != "", "valid": valid, "op": o.op,
		"successes": len(all), "errors": em, "terms": tm, "elapsed_s": elapsed,
		"ops_s": float64(len(all)) / elapsed, "mean_ms": mean, "p50_ms": pct(.5),
		"p90_ms": pct(.9), "p99_ms": pct(.99), "p999_ms": pct(.999), "max_ms": pct(1),
		"cpu_us_op": cpus, "occupied_cores": occup, "cpu_before": pre, "cpu_after": post,
		"revision_before": before.Header.Revision, "revision_after": after.Header.Revision,
		"workers": o.workers, "conns": o.conns, "keyspace": keyspace, "value_bytes": valueBytes,
		"clock_hz": o.hz, "start_utc": t.UTC().Format(time.RFC3339Nano),
		"generator_cpu_us_op": cpus[len(cpus)-1], "all_servers_cpu_us_op": serverCPU,
		"admitted": b.next.Load(), "total": o.total, "duration_s": o.duration.Seconds(),
		"lean_source_sha": sourceSHA, "per_put_context": false, "grpc_timeout_metadata": false,
		"timeout_s": ioTimeout.Seconds(), "completion_fence": "one HTTP/2 PING round trip per connection, inside CPU/elapsed",
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(o.out, append(data, '\n'), 0o644); err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid measurement; see JSON")
	}
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
