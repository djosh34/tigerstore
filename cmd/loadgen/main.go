// Command loadgen drives an etcd v3 API cluster with one workload and
// reports throughput, latency percentiles and server CPU per operation.
//
// It speaks gRPC over raw HTTP/2: many streams per connection, requests
// stamped out from prebuilt frames, and open-loop Poisson arrivals whose
// latency counts from the time each request was meant to be sent. The
// workloads use Kubernetes-shaped keys and the requests kube-apiserver sends.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	endpoints []string
	workload  string
	mix       string
	valueSize int
	keys      int
	conns     int
	streams   int
	rate      float64
	warmup    time.Duration
	duration  time.Duration
	watchers  int
	page      int
	pids      []int
	label     string
	seed      uint64
	out       string
	cacert    string
	cert, key string
}

var workloads = []string{"put", "txn", "delete", "range", "list", "watch", "mixed"}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	endpoints := fs.String("endpoints", "127.0.0.1:2379", "comma-separated host:port of the cluster's client endpoints; connections go round robin")
	fs.StringVar(&cfg.workload, "workload", "put", "one of "+strings.Join(workloads, ", "))
	fs.StringVar(&cfg.mix, "mix", defaultMix, "mixed percentages: get,list,update,create,delete; watches on every resource prefix")
	fs.IntVar(&cfg.valueSize, "value-size", 1024, "value size in bytes")
	fs.IntVar(&cfg.keys, "keys", 10000, "number of keys; delete deletes each once, so it needs at least as many as it sends")
	fs.IntVar(&cfg.conns, "conns", 6, "HTTP/2 connections for requests")
	fs.IntVar(&cfg.streams, "streams", 256, "requests in flight per connection at most")
	fs.Float64Var(&cfg.rate, "rate", 0, "open-loop arrivals per second over all connections; 0 keeps every stream busy")
	fs.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "load before the measurement window")
	fs.DurationVar(&cfg.duration, "duration", 10*time.Second, "measurement window")
	fs.IntVar(&cfg.watchers, "watchers", 4, "watch workload: watch streams on the key prefix")
	fs.IntVar(&cfg.page, "page", 500, "list workload: the limit of each LIST page; keys spread over 100 namespaces")
	pids := fs.String("pids", "", "comma-separated pids of the server processes, for server CPU per operation")
	fs.StringVar(&cfg.label, "label", "", "name of the store under test, copied into the result")
	fs.Uint64Var(&cfg.seed, "seed", 1, "random seed")
	fs.StringVar(&cfg.out, "out", "", "write the JSON result to this file instead of stdout")
	fs.StringVar(&cfg.cacert, "cacert", "", "CA certificate of the servers; with it, connections use TLS 1.3")
	fs.StringVar(&cfg.cert, "cert", "", "client certificate for TLS")
	fs.StringVar(&cfg.key, "key", "", "client key for TLS")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.endpoints = strings.Split(*endpoints, ",")
	if *pids != "" {
		for p := range strings.SplitSeq(*pids, ",") {
			n, err := strconv.Atoi(p)
			if err != nil {
				return cfg, fmt.Errorf("-pids: %w", err)
			}
			cfg.pids = append(cfg.pids, n)
		}
	}
	switch {
	case !slices.Contains(workloads, cfg.workload):
		return cfg, fmt.Errorf("unknown workload %q", cfg.workload)
	case cfg.conns < 1 || cfg.streams < 1 || cfg.keys < cfg.conns:
		return cfg, errors.New("need -conns >= 1, -streams >= 1 and -keys >= -conns")
	case cfg.valueSize < 8:
		return cfg, errors.New("-value-size must be at least 8, to hold the watch timestamp")
	case cfg.workload == "watch" && cfg.watchers < 1:
		return cfg, errors.New("watch needs -watchers >= 1")
	case cfg.rate < 0 || cfg.duration <= 0 || cfg.warmup < 0:
		return cfg, errors.New("need -rate >= 0, -duration > 0 and -warmup >= 0")
	}
	if _, err := parseMix(cfg.mix); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// result is the JSON output of one run. Latencies are in microseconds.
type result struct {
	// Valid is false if anything went wrong: a request error, a phase
	// that did not finish, or a CPU sample that failed. Reports must
	// reject invalid results.
	Valid          bool     `json:"valid"`
	Label          string   `json:"label"`
	Workload       string   `json:"workload"`
	ValueSize      int      `json:"value_size"`
	Keys           int      `json:"keys"`
	LiveKeys       int      `json:"live_keys,omitempty"`
	SizeSource     string   `json:"size_source,omitempty"`
	Mix            string   `json:"mix,omitempty"`
	Endpoints      []string `json:"endpoints"`
	Conns          int      `json:"conns"`
	StreamsPerConn int      `json:"streams_per_conn"`
	// OfferedRate is the open-loop arrival rate; 0 means every stream
	// was kept busy, so latency counts from the actual send.
	OfferedRate float64 `json:"offered_rate"`
	WarmupS     float64 `json:"warmup_s"`
	DurationS   float64 `json:"duration_s"`
	Ops         int64   `json:"ops"`
	Throughput  float64 `json:"throughput"`
	// Latency counts from the intended send time, ServiceLatency from the
	// actual send. They differ when the generator or a full window held a
	// request back.
	Latency        latency `json:"latency_us"`
	ServiceLatency latency `json:"service_latency_us"`
	// Cohort counts the requests that arrived in the window, which the
	// latencies describe, and CohortFailed those of them that failed.
	Cohort       int64 `json:"cohort"`
	CohortFailed int64 `json:"cohort_failed"`
	// Unsent counts open-loop arrivals never sent by the end of the drain;
	// any makes the result invalid.
	Unsent int64        `json:"unsent"`
	Watch  *watchResult `json:"watch,omitempty"`
	// CPU over the measurement window.
	ServerCPUUsPerOp   float64          `json:"server_cpu_us_per_op"`
	ServerCPUUsPerNode []float64        `json:"server_cpu_us_per_op_by_node"`
	ServerCores        []float64        `json:"server_cores_by_node"`
	ClientCPUUsPerOp   float64          `json:"client_cpu_us_per_op"`
	ClientCores        float64          `json:"client_cores"`
	Errors             map[string]int64 `json:"errors"`
}

type watchResult struct {
	Watchers            int     `json:"watchers"`
	Events              int64   `json:"events"`
	EventsPerS          float64 `json:"events_per_s"`
	Latency             latency `json:"latency_us"`
	ServerCPUUsPerEvent float64 `json:"server_cpu_us_per_event"`
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
	res, err := run(cfg)
	if res != nil {
		if werr := writeResult(cfg.out, res); werr != nil {
			err = errors.Join(err, werr)
		}
		fmt.Fprintln(os.Stderr, res.oneLine())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func writeResult(path string, res *result) error {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if path == "" {
		_, err = os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func (r *result) oneLine() string {
	s := fmt.Sprintf("%s %s %dB: %.0f ops/s, p50 %.0fus p99 %.0fus p99.9 %.0fus, server %.1f us/op, client %.1f us/op",
		r.Label, r.Workload, r.ValueSize, r.Throughput, r.Latency.P50, r.Latency.P99, r.Latency.P999,
		r.ServerCPUUsPerOp, r.ClientCPUUsPerOp)
	if r.Watch != nil {
		s += fmt.Sprintf(", watch %.0f events/s p99 %.0fus", r.Watch.EventsPerS, r.Watch.Latency.P99)
	}
	if n := len(r.Errors); n > 0 {
		s += fmt.Sprintf(", %d kinds of errors", n)
	}
	return s
}

const (
	preloadTimeout = 10 * time.Minute
	drainTimeout   = 10 * time.Second
)

func run(cfg config) (*result, error) {
	if cfg.cacert != "" {
		c, err := loadTLS(cfg.cacert, cfg.cert, cfg.key)
		if err != nil {
			return nil, err
		}
		clientTLS = c
	}
	if cfg.workload == "mixed" {
		return runMixed(cfg)
	}
	ks := makeKeys(cfg.keys)
	vals := makeValues(cfg.valueSize, cfg.seed)
	writes := newRevisionSet()

	var workers []*worker
	defer func() {
		for _, wk := range workers {
			wk.c.close()
		}
	}()
	for i := range cfg.conns {
		c, err := dial(cfg.endpoints[i%len(cfg.endpoints)])
		if err != nil {
			return nil, err
		}
		workers = append(workers, newWorker(c, cfg.seed+uint64(i)))
	}
	// base returns the per-connection parts of a load on worker i.
	base := func(i int, path string, m *msg) (base, error) {
		wk := workers[i]
		t, err := newTemplate(wk.c.addr, path, m, true)
		if err != nil {
			return base{}, err
		}
		v := *vals
		v.off = i * (valuePoolSize / cfg.conns) / cfg.valueSize * cfg.valueSize
		b := base{tpl: t, ks: ks, vals: v, rng: wk.rng, stats: &wk.stats, limit: cfg.streams, writes: writes}
		for k := i; k < cfg.keys; k += cfg.conns {
			b.own = append(b.own, k)
		}
		return b, nil
	}

	var revs []int64
	var putVals [][]byte
	if cfg.workload == "txn" || cfg.workload == "range" || cfg.workload == "list" || cfg.workload == "delete" {
		revs = make([]int64, cfg.keys)
		putVals = make([][]byte, cfg.keys)
		loads := make([]load, len(workers))
		for i := range workers {
			b, err := base(i, "/etcdserverpb.KV/Put", putRequest(ks.size, cfg.valueSize))
			if err != nil {
				return nil, err
			}
			loads[i] = &putLoad{base: b, once: true, revs: revs, vals: putVals}
		}
		start := now()
		if err := runPhase(workers, loads, phase{start: start, stop: start + int64(preloadTimeout), drain: drainTimeout}); err != nil {
			return nil, fmt.Errorf("preload: %w", err)
		}
		for _, l := range loads {
			if !l.exhausted() {
				return nil, fmt.Errorf("preload of %d keys did not finish in %v", cfg.keys, preloadTimeout)
			}
		}
		if err := phaseErrors(workers); err != nil {
			return nil, fmt.Errorf("preload: %w", err)
		}
	}

	loads := make([]load, len(workers))
	for i := range workers {
		var m *msg
		switch cfg.workload {
		case "put", "watch":
			m = putRequest(ks.size, cfg.valueSize)
		case "txn":
			m = casRequest(ks.size, cfg.valueSize)
		case "delete":
			m = deleteRequest(ks.size)
		case "range":
			m = rangeRequest(ks.size)
		case "list":
			m = listRequest(len(listPrefix(0)), cfg.page)
		}
		b, err := base(i, kvPath(cfg.workload), m)
		if err != nil {
			return nil, err
		}
		switch cfg.workload {
		case "put":
			loads[i] = &putLoad{base: b}
		case "watch":
			loads[i] = &putLoad{base: b, stamp: true}
		case "txn":
			loads[i] = newCASLoad(b, revs)
		case "delete":
			loads[i] = &deleteLoad{base: b}
		case "range":
			loads[i] = &rangeLoad{base: b, valueSize: cfg.valueSize, want: putVals}
		case "list":
			loads[i] = newListLoad(b, cfg.page, cfg.valueSize)
		}
	}

	var watches []*watchLoad
	var watchConns []*conn
	stopControl := make(chan struct{})
	var control sync.WaitGroup
	closeWatches := func() {
		close(stopControl)
		control.Wait()
		for _, c := range watchConns {
			c.close()
		}
		watchConns = nil
	}
	defer func() {
		if watchConns != nil {
			closeWatches()
		}
	}()
	if cfg.workload == "watch" {
		var err error
		watchConns, watches, err = startWatches(cfg, &control, stopControl)
		if err != nil {
			return nil, err
		}
	}

	t0 := now() + int64(50*time.Millisecond)
	t1 := t0 + int64(cfg.warmup)
	t2 := t1 + int64(cfg.duration)
	for _, wk := range workers {
		wk.stats.winStart, wk.stats.winStop = t1, t2
		wk.unsent = 0
	}
	for _, w := range watches {
		w.stats.winStart, w.stats.winStop = t1, t2
	}

	var samples [2]cpuSample
	var sampleErr error
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for i, at := range []int64{t1, t2} {
			time.Sleep(time.Duration(at - now()))
			s, err := sampleCPU(cfg.pids)
			if err != nil {
				sampleErr = err
				return
			}
			samples[i] = s
		}
	})
	perConn := cfg.rate / float64(cfg.conns)
	phaseErr := runPhase(workers, loads, phase{rate: perConn, start: t0, stop: t2, drain: drainTimeout})
	sampler.Wait()

	var errs []error
	if phaseErr != nil {
		errs = append(errs, phaseErr)
	}
	if sampleErr != nil {
		errs = append(errs, fmt.Errorf("server CPU: %w", sampleErr))
	}
	if cfg.workload == "delete" {
		for _, l := range loads {
			if l.exhausted() {
				errs = append(errs, fmt.Errorf("delete ran out of its %d keys; raise -keys", cfg.keys))
				break
			}
		}
	}
	if len(watches) > 0 {
		if err := awaitWatches(workers, watchConns, watches); err != nil {
			errs = append(errs, err)
		}
		// Closed connections stop calling the watch handlers, so the
		// summary can read them.
		closeWatches()
	}

	res := summarize(cfg, workers, watches, samples)
	if len(res.Errors) > 0 {
		errs = append(errs, fmt.Errorf("%d kinds of request errors, see the result", len(res.Errors)))
	}
	if len(cfg.pids) == 0 {
		errs = append(errs, errors.New("no -pids, so no server CPU"))
	}
	res.Valid = len(errs) == 0
	return res, errors.Join(errs...)
}

func kvPath(workload string) string {
	switch workload {
	case "txn":
		return "/etcdserverpb.KV/Txn"
	case "delete":
		return "/etcdserverpb.KV/DeleteRange"
	case "range", "list":
		return "/etcdserverpb.KV/Range"
	default:
		return "/etcdserverpb.KV/Put"
	}
}

// runPhase runs loads[i] on workers[i] in parallel and returns the first
// error.
func runPhase(workers []*worker, loads []load, p phase) error {
	errs := make([]error, len(workers))
	var wg sync.WaitGroup
	for i, wk := range workers {
		wg.Go(func() { errs[i] = wk.run(loads[i], p) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// phaseErrors returns the request errors counted so far, if any.
func phaseErrors(workers []*worker) error {
	for _, wk := range workers {
		wk.c.mu.Lock()
		n := len(wk.stats.errs)
		var first string
		for e := range wk.stats.errs {
			first = e
			break
		}
		wk.c.mu.Unlock()
		if n > 0 {
			return fmt.Errorf("%d kinds of request errors, such as %q", n, first)
		}
	}
	return nil
}

// startWatches opens the watch streams round robin over the endpoints, one
// connection per endpoint at most, and waits until every watch is created.
func startWatches(cfg config, control *sync.WaitGroup, stop <-chan struct{}) ([]*conn, []*watchLoad, error) {
	n := min(cfg.watchers, len(cfg.endpoints))
	var conns []*conn
	var watches []*watchLoad
	for i := range n {
		c, err := dial(cfg.endpoints[i])
		if err != nil {
			return conns, nil, err
		}
		conns = append(conns, c)
		t, err := newTemplate(c.addr, "/etcdserverpb.Watch/Watch", watchRequest([]byte(keyPrefix)), false)
		if err != nil {
			return conns, nil, err
		}
		count := cfg.watchers / n
		if i < cfg.watchers%n {
			count++
		}
		w := &watchLoad{tpl: t, streams: make([]watchStream, count), stats: &stats{errs: make(map[string]int64)}}
		watches = append(watches, w)
		c.mu.Lock()
		for s := range count {
			if !c.canStart(t, count) {
				c.mu.Unlock()
				return conns, nil, fmt.Errorf("%s: cannot open watch stream %d", c.addr, s)
			}
			st := c.newStream(w)
			st.slot = s
			c.startStream(st, t)
		}
		c.mu.Unlock()
		if err := c.flush(); err != nil {
			return conns, nil, err
		}
		control.Go(func() { c.serveControl(stop) })
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready := true
		for i, c := range conns {
			c.mu.Lock()
			for _, ws := range watches[i].streams {
				ready = ready && ws.created
			}
			err := c.err
			c.mu.Unlock()
			if err != nil {
				return conns, nil, err
			}
		}
		if ready {
			return conns, watches, nil
		}
		if time.Now().After(deadline) {
			return conns, nil, errors.New("watches not created within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// serveControl sends the control frames of a connection that has no request
// writer, such as window updates for watch data, until stop is closed.
func (c *conn) serveControl(stop <-chan struct{}) {
	for {
		c.mu.Lock()
		c.appendControl()
		failed := c.err != nil
		c.mu.Unlock()
		if failed || c.flush() != nil {
			// The reader records the failure; watch checks report it.
			return
		}
		select {
		case <-c.wake:
		case <-stop:
			return
		}
	}
}

// watchCatchUp is how long watches may take to deliver every put after the
// last one. etcd delivers events to a watcher that fell behind in catch-up
// batches, which takes several seconds after a max-throughput run.
const watchCatchUp = 30 * time.Second

// awaitWatches waits until every watch stream that has not failed has seen
// an event for every acknowledged put.
func awaitWatches(workers []*worker, conns []*conn, watches []*watchLoad) error {
	var acked int64
	for _, wk := range workers {
		wk.c.mu.Lock()
		acked += wk.stats.acked
		wk.c.mu.Unlock()
	}
	deadline := time.Now().Add(watchCatchUp)
	for {
		behind := 0
		worst := acked
		for i, c := range conns {
			c.mu.Lock()
			for _, ws := range watches[i].streams {
				if !ws.failed && ws.events < acked {
					behind++
					worst = min(worst, ws.events)
				}
			}
			c.mu.Unlock()
		}
		if behind == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d watch streams missed events %v after the last put: as few as %d of %d", behind, watchCatchUp, worst, acked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func summarize(cfg config, workers []*worker, watches []*watchLoad, samples [2]cpuSample) *result {
	res := &result{
		Label:          cfg.label,
		Workload:       cfg.workload,
		ValueSize:      cfg.valueSize,
		Keys:           cfg.keys,
		Endpoints:      cfg.endpoints,
		Conns:          cfg.conns,
		StreamsPerConn: cfg.streams,
		OfferedRate:    cfg.rate,
		WarmupS:        cfg.warmup.Seconds(),
		DurationS:      cfg.duration.Seconds(),
		Errors:         make(map[string]int64),
	}
	var resp, service hist
	for _, wk := range workers {
		wk.c.mu.Lock()
		res.Ops += wk.stats.ops
		res.Cohort += wk.stats.cohort
		res.CohortFailed += wk.stats.cohortFailed
		res.Unsent += wk.unsent
		resp.merge(&wk.stats.resp)
		service.merge(&wk.stats.service)
		for e, n := range wk.stats.errs {
			res.Errors[e] += n
		}
		wk.c.mu.Unlock()
	}
	res.Throughput = float64(res.Ops) / cfg.duration.Seconds()
	res.Latency = resp.summary()
	res.ServiceLatency = service.summary()

	if len(watches) > 0 {
		var lat hist
		wr := &watchResult{Watchers: cfg.watchers}
		for _, w := range watches {
			wr.Events += w.events
			lat.merge(&w.lat)
			for e, n := range w.stats.errs {
				res.Errors[e] += n
			}
		}
		wr.EventsPerS = float64(wr.Events) / cfg.duration.Seconds()
		wr.Latency = lat.summary()
		res.Watch = wr
	}

	elapsed := time.Duration(samples[1].at - samples[0].at)
	if elapsed <= 0 {
		return res
	}
	perOp := func(d time.Duration, n int64) float64 {
		if n == 0 {
			return 0
		}
		return float64(d.Microseconds()) / float64(n)
	}
	var total time.Duration
	for i := range samples[0].servers {
		d := samples[1].servers[i] - samples[0].servers[i]
		total += d
		res.ServerCPUUsPerNode = append(res.ServerCPUUsPerNode, perOp(d, res.Ops))
		res.ServerCores = append(res.ServerCores, d.Seconds()/elapsed.Seconds())
	}
	res.ServerCPUUsPerOp = perOp(total, res.Ops)
	self := samples[1].self - samples[0].self
	res.ClientCPUUsPerOp = perOp(self, res.Ops)
	res.ClientCores = self.Seconds() / elapsed.Seconds()
	if res.Watch != nil {
		res.Watch.ServerCPUUsPerEvent = perOp(total, res.Watch.Events)
	}
	return res
}

// loadTLS builds the client TLS configuration: TLS 1.3, h2, the servers'
// CA, and a client certificate if one is given.
func loadTLS(cacert, cert, key string) (*tls.Config, error) {
	pem, err := os.ReadFile(cacert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no certificates", cacert)
	}
	c := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	if cert != "" {
		kp, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		c.Certificates = []tls.Certificate{kp}
	}
	return c, nil
}
