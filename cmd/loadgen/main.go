// Command loadgen drives an etcd v3 API cluster with one workload and
// reports throughput, latency percentiles and server CPU per operation.
//
// It speaks gRPC over raw HTTP/2: many streams per connection, requests
// stamped out from prebuilt frames, and open-loop Poisson arrivals whose
// latency counts from the time each request was meant to be sent. The
// workloads use Kubernetes-shaped keys and the requests kube-apiserver sends.
//
// It measures the store, so it does as little as it can on its one core:
// it never checks an answer (the simulator checks correctness), it reads
// only what the next request needs, a write's revision and outcome, and the
// sizes of the key-values a read returned, from their lengths. With -ramp,
// the requests in flight double after each measured step until throughput
// stops climbing, and the best step is the result.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/internal/k8strace"
)

type config struct {
	endpoints   []string
	workload    string
	valueSize   int
	keys        int
	conns       int
	streams     int
	rate        float64
	preloadRate float64
	warmup      time.Duration
	duration    time.Duration
	watchers    int
	page        int
	ramp        bool
	pids        []int
	metrics     []string
	disk        string
	label       string
	seed        uint64
	out         string
	cpuprofile  string
	cacert      string
	cert, key   string
	traces      string
	copies      int
}

var workloads = []string{"put", "txn", "delete", "range", "list", "watch", "k8s"}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	endpoints := fs.String("endpoints", "127.0.0.1:2379", "comma-separated host:port of the cluster's client endpoints; connections go round robin")
	fs.StringVar(&cfg.workload, "workload", "put", "one of "+strings.Join(workloads, ", "))
	fs.IntVar(&cfg.valueSize, "value-size", 1024, "value size in bytes")
	fs.IntVar(&cfg.keys, "keys", 10000, "number of keys; delete deletes each once, so it needs at least as many as it sends")
	fs.IntVar(&cfg.conns, "conns", 6, "HTTP/2 connections for requests")
	fs.IntVar(&cfg.streams, "streams", 256, "requests in flight per connection at most")
	fs.Float64Var(&cfg.preloadRate, "preload-rate", 300, "k8s preload Txns per second across all connections; 0 keeps the preload slots busy")
	fs.Float64Var(&cfg.rate, "rate", 0, "open-loop arrivals per second over all connections; 0 keeps every stream busy")
	fs.DurationVar(&cfg.warmup, "warmup", 2*time.Second, "load before the measurement window")
	fs.DurationVar(&cfg.duration, "duration", 10*time.Second, "measurement window")
	fs.IntVar(&cfg.watchers, "watchers", 4, "watch workload: watch streams on the key prefix")
	fs.IntVar(&cfg.page, "page", 500, "list workload: the limit of each LIST page; keys spread over 100 namespaces")
	fs.BoolVar(&cfg.ramp, "ramp", false, "double -streams after each step until throughput climbs less than 5%, at most 5 times; the best step is the result")
	pids := fs.String("pids", "", "comma-separated pids of the server processes, for server CPU per operation")
	metrics := fs.String("metrics", "", "comma-separated host:port of every node's client endpoint, whose /metrics give each node's peer bytes per operation")
	fs.StringVar(&cfg.disk, "disk", "", "device or partition in /proc/diskstats, such as sda1, whose bytes written in the window are reported per operation")
	fs.StringVar(&cfg.label, "label", "", "name of the store under test, copied into the result")
	fs.Uint64Var(&cfg.seed, "seed", 1, "random seed")
	fs.StringVar(&cfg.out, "out", "", "write the JSON result to this file instead of stdout")
	fs.StringVar(&cfg.cpuprofile, "cpuprofile", "", "write a CPU profile of the measurement window to this file")
	fs.StringVar(&cfg.cacert, "cacert", "", "CA certificate of the servers; with it, connections use TLS 1.3")
	fs.StringVar(&cfg.cert, "cert", "", "client certificate for TLS")
	fs.StringVar(&cfg.key, "key", "", "client key for TLS")
	fs.StringVar(&cfg.traces, "traces", "bench/recordings", "k8s: the directory of recorded traces, one of which the seed draws")
	fs.IntVar(&cfg.copies, "copies", 64, "k8s: copies of the recorded cluster that share the store")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.endpoints = strings.Split(*endpoints, ",")
	if *metrics != "" {
		cfg.metrics = strings.Split(*metrics, ",")
	}
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
	case cfg.workload == "k8s" && cfg.copies < cfg.conns:
		return cfg, errors.New("k8s needs -copies >= -conns")
	case cfg.workload == "watch" && cfg.watchers < 1:
		return cfg, errors.New("watch needs -watchers >= 1")
	case cfg.ramp && (cfg.rate > 0 || cfg.workload == "delete" || cfg.workload == "watch"):
		return cfg, errors.New("-ramp needs max load (-rate 0) and a workload that does not run out of keys or watch")
	case cfg.rate < 0 || cfg.preloadRate < 0 || math.IsNaN(cfg.rate) || math.IsInf(cfg.rate, 0) || math.IsNaN(cfg.preloadRate) || math.IsInf(cfg.preloadRate, 0) || cfg.duration <= 0 || cfg.warmup < 0:
		return cfg, errors.New("need finite non-negative -rate and -preload-rate, -duration > 0 and -warmup >= 0")
	}
	if cfg.workload == "k8s" {
		return cfg, traceDir(cfg.traces)
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
	SizeSource     string   `json:"size_source,omitempty"`
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
	// Acked counts the successful calls of the whole measured phase, its
	// warmup and drain too, not the preload.
	Acked int64 `json:"acked"`
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
	// CPU over the measurement window. The first of -pids is the leader,
	// so LeaderCPUUsPerOp is the leader's alone; ServerCPUUsPerOp sums
	// every node.
	LeaderCPUUsPerOp   float64   `json:"leader_cpu_us_per_op"`
	ServerCPUUsPerOp   float64   `json:"server_cpu_us_per_op"`
	ServerCPUUsPerNode []float64 `json:"server_cpu_us_per_op_by_node"`
	ServerCores        []float64 `json:"server_cores_by_node"`
	// PeerSentBytesPerOp is each node's bytes sent to its peers in the
	// window per operation, from -metrics: the node that led at the end
	// first, then the others.
	PeerSentBytesPerOp []float64 `json:"peer_sent_bytes_per_op_by_node,omitempty"`
	ClientCPUUsPerOp   float64   `json:"client_cpu_us_per_op"`
	ClientCores        float64   `json:"client_cores"`
	// DiskBytesPerOp counts every byte -disk wrote in the window, from
	// all servers on it and anything else, per operation.
	DiskBytesPerOp float64 `json:"disk_bytes_per_op,omitempty"`
	// MinWrittenBytesPerOp is the fewest bytes an operation must make
	// durable on each node (tally), a mean over the operations of the
	// window.
	MinWrittenBytesPerOp float64 `json:"min_written_bytes_per_op"`
	// PerOp is what the window's calls carried, per operation, for the
	// least CPU an operation needs (cmd/benchreport).
	PerOp perOp `json:"per_op"`
	// Ramp has every step of -ramp, the chosen one included. RampClimbing
	// is set when the last step still climbed: the requests in flight may
	// still have limited the store.
	Ramp         []rampStep       `json:"ramp,omitempty"`
	RampClimbing bool             `json:"ramp_climbing,omitempty"`
	Errors       map[string]int64 `json:"errors"`
	// K8s describes a k8s run.
	K8s *k8sResult `json:"k8s,omitempty"`
}

// perOp is counts over the operations of a window, as counts has them.
type perOp struct {
	OutBytes float64 `json:"out_bytes"`
	InBytes  float64 `json:"in_bytes"`
	KVs      float64 `json:"kvs"`
	PrevKVs  float64 `json:"prev_kvs"`
	Events   float64 `json:"event_kvs"`
	Writes   float64 `json:"writes"`
	Creates  float64 `json:"creates"`
	Deletes  float64 `json:"deletes"`
	Entries  float64 `json:"entries"`
	Checks   float64 `json:"checks"`
	Leases   float64 `json:"leases"`
}

// rampStep is one step of -ramp.
type rampStep struct {
	StreamsPerConn int     `json:"streams_per_conn"`
	Throughput     float64 `json:"throughput"`
	ClientCores    float64 `json:"client_cores"`
	LeaderCores    float64 `json:"leader_cores"`
}

type watchResult struct {
	Watchers            int     `json:"watchers"`
	Events              int64   `json:"events"`
	EventsPerS          float64 `json:"events_per_s"`
	Latency             latency `json:"latency_us"`
	ServerCPUUsPerEvent float64 `json:"server_cpu_us_per_event,omitempty"`
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

// profile is the file of the CPU profile being taken.
var profile *os.File

// profileCPU starts a CPU profile into path, or stops it. Each measured
// step of a ramp writes the file anew.
func profileCPU(path string, start bool) error {
	if !start {
		pprof.StopCPUProfile()
		return profile.Close()
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		return errors.Join(err, f.Close())
	}
	profile = f
	return nil
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
	s := fmt.Sprintf("%s %s %dB, %d in flight: %.0f ops/s, p50 %.0fus p99 %.0fus p99.9 %.0fus, server %.1f us/op, client %.1f us/op",
		r.Label, r.Workload, r.ValueSize, r.Conns*r.StreamsPerConn, r.Throughput, r.Latency.P50, r.Latency.P99, r.Latency.P999,
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

// rampGain is the least gain in throughput from doubling the requests in
// flight that -ramp takes for still climbing, and maxRampSteps the most
// doublings it tries.
const (
	rampGain     = 1.05
	maxRampSteps = 5
)

func run(cfg config) (*result, error) {
	if cfg.cacert != "" {
		c, err := loadTLS(cfg.cacert, cfg.cert, cfg.key)
		if err != nil {
			return nil, err
		}
		clientTLS = c
	}
	s := &session{cfg: cfg, stop: make(chan struct{})}
	defer s.close()
	setup := s.setupKV
	if cfg.workload == "k8s" {
		setup = s.setupK8s
	}
	if err := setup(); err != nil {
		return nil, err
	}
	var best *result
	var bestErr error
	var ramp []rampStep
	for step, streams := 0, cfg.streams; ; step, streams = step+1, streams*2 {
		res, err := s.measure(streams)
		leader := 0.0
		if len(res.ServerCores) > 0 {
			leader = res.ServerCores[0]
		}
		ramp = append(ramp, rampStep{StreamsPerConn: streams, Throughput: res.Throughput, ClientCores: res.ClientCores, LeaderCores: leader})
		climbing := best == nil || res.Throughput >= best.Throughput*rampGain
		if best == nil || err != nil || res.Throughput > best.Throughput {
			best, bestErr = res, err
		}
		if err != nil || !cfg.ramp || !climbing || step == maxRampSteps {
			if cfg.ramp {
				best.Ramp = ramp
				best.RampClimbing = err == nil && climbing
			}
			break
		}
	}
	return best, bestErr
}

// session is the connections and loads of one run, which every step of
// -ramp measures again.
type session struct {
	cfg     config
	workers []*worker
	loads   []load
	// The watch workload's watches.
	watches    []*watchLoad
	watchConns []*conn
	// k8s's recording and the loads of its copies.
	trace      *k8strace.Trace
	k8s        []*k8sLoad
	k8sWatches []*k8sWatch
	// stop ends the goroutines in control, which send the watch
	// connections' control frames.
	stop    chan struct{}
	control sync.WaitGroup
}

func (s *session) close() {
	close(s.stop)
	s.control.Wait()
	for _, c := range s.watchConns {
		c.close()
	}
	for _, wk := range s.workers {
		wk.c.close()
	}
}

// dial opens the request connections, round robin over the endpoints.
func (s *session) dial() error {
	for i := range s.cfg.conns {
		c, err := dial(s.cfg.endpoints[i%len(s.cfg.endpoints)])
		if err != nil {
			return err
		}
		s.workers = append(s.workers, newWorker(c, s.cfg.seed+uint64(i)))
	}
	return nil
}

// preload runs loads to the end, before any measurement.
func (s *session) preload(loads []load, rate float64) error {
	start := now()
	err := runPhase(s.workers, loads, phase{rate: rate, start: start, stop: start + int64(preloadTimeout), drain: drainTimeout, limit: s.cfg.streams})
	if err != nil {
		return fmt.Errorf("preload: %w", err)
	}
	for _, l := range loads {
		if !l.exhausted() {
			return fmt.Errorf("preload did not finish in %v", preloadTimeout)
		}
	}
	if err := phaseErrors(s.workers); err != nil {
		return fmt.Errorf("preload: %w", err)
	}
	return nil
}

func (s *session) setupKV() error {
	cfg := s.cfg
	if err := s.dial(); err != nil {
		return err
	}
	ks := makeKeys(cfg.keys)
	vals := makeValues(cfg.valueSize, cfg.seed)
	// base returns the per-connection parts of a load on worker i.
	base := func(i int, path string, m *msg) (base, error) {
		wk := s.workers[i]
		t, err := newTemplate(path, m, true)
		if err != nil {
			return base{}, err
		}
		v := *vals
		v.off = i * (valuePoolSize / cfg.conns) / cfg.valueSize * cfg.valueSize
		b := base{tpl: t, ks: ks, vals: v, rng: wk.rng, stats: &wk.stats}
		for k := i; k < cfg.keys; k += cfg.conns {
			b.own = append(b.own, k)
		}
		return b, nil
	}

	var revs []int64
	if cfg.workload == "txn" || cfg.workload == "range" || cfg.workload == "list" || cfg.workload == "delete" {
		revs = make([]int64, cfg.keys)
		loads := make([]load, len(s.workers))
		for i := range s.workers {
			b, err := base(i, "/etcdserverpb.KV/Put", putRequest(ks.size, cfg.valueSize))
			if err != nil {
				return err
			}
			loads[i] = &putLoad{base: b, once: true, revs: revs}
		}
		if err := s.preload(loads, 0); err != nil {
			return err
		}
	}

	for i := range s.workers {
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
			return err
		}
		var l load
		switch cfg.workload {
		case "put":
			l = &putLoad{base: b}
		case "watch":
			l = &putLoad{base: b, stamp: true}
		case "txn":
			l = newCASLoad(b, revs)
		case "delete":
			l = &deleteLoad{base: b}
		case "range":
			l = &rangeLoad{base: b}
		case "list":
			l = newListLoad(b)
		}
		s.loads = append(s.loads, l)
	}
	if cfg.workload == "watch" {
		var err error
		s.watchConns, s.watches, err = startWatches(cfg, &s.control, s.stop)
		return err
	}
	return nil
}

// measure runs one measured phase with at most streams requests in flight
// per connection.
func (s *session) measure(streams int) (*result, error) {
	cfg := s.cfg
	t0 := now() + int64(50*time.Millisecond)
	t1 := t0 + int64(cfg.warmup)
	t2 := t1 + int64(cfg.duration)
	for i, wk := range s.workers {
		wk.c.mu.Lock()
		// Every step, and every store, gets the same arrival schedule for
		// the seed, however the preload or an earlier step went.
		wk.arrivals = newArrivalRNG(cfg.seed + uint64(i))
		wk.stats = stats{winStart: t1, winStop: t2, errs: make(map[string]int64)}
		wk.unsent = 0
		wk.c.mu.Unlock()
	}
	for i, w := range s.watches {
		s.watchConns[i].mu.Lock()
		w.stats.winStart, w.stats.winStop = t1, t2
		s.watchConns[i].mu.Unlock()
	}
	for _, ws := range s.k8sWatches {
		ws.c.mu.Lock()
		ws.stats = stats{winStart: t1, winStop: t2, errs: make(map[string]int64)}
		ws.c.mu.Unlock()
	}

	var samples [2]cpuSample
	var sampleErr error
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for i, at := range []int64{t1, t2} {
			time.Sleep(time.Duration(at - now()))
			s, err := sampleCPU(cfg.pids, cfg.disk)
			if err != nil {
				sampleErr = err
				return
			}
			if len(cfg.metrics) > 0 {
				if s.nodes, err = sampleMetrics(cfg.metrics); err != nil {
					sampleErr = err
					return
				}
			}
			samples[i] = s
			if cfg.cpuprofile != "" {
				if err := profileCPU(cfg.cpuprofile, i == 0); err != nil {
					sampleErr = err
					return
				}
			}
		}
	})
	phaseErr := runPhase(s.workers, s.loads, phase{rate: cfg.rate / float64(cfg.conns), start: t0, stop: t2, drain: drainTimeout, limit: streams})
	sampler.Wait()

	var errs []error
	if phaseErr != nil {
		errs = append(errs, phaseErr)
	}
	if sampleErr != nil {
		errs = append(errs, fmt.Errorf("server CPU: %w", sampleErr))
	}
	if cfg.workload == "delete" {
		for _, l := range s.loads {
			if l.exhausted() {
				errs = append(errs, fmt.Errorf("delete ran out of its %d keys; raise -keys", cfg.keys))
				break
			}
		}
	}
	if len(s.watches) > 0 {
		if err := awaitWatches(s.workers, s.watchConns, s.watches); err != nil {
			errs = append(errs, err)
		}
	}
	if s.trace != nil {
		if err := s.k8sErrors(); err != nil {
			errs = append(errs, err)
		}
	}
	res := s.summarize(streams, samples)
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
		t, err := newTemplate("/etcdserverpb.Watch/Watch", watchRequest([]byte(keyPrefix)), false)
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
			if !c.canStart(t.dataLen, count) {
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

func (s *session) summarize(streams int, samples [2]cpuSample) *result {
	cfg := s.cfg
	res := &result{
		Label:          cfg.label,
		Workload:       cfg.workload,
		ValueSize:      cfg.valueSize,
		Keys:           cfg.keys,
		Endpoints:      cfg.endpoints,
		Conns:          cfg.conns,
		StreamsPerConn: streams,
		OfferedRate:    cfg.rate,
		WarmupS:        cfg.warmup.Seconds(),
		DurationS:      cfg.duration.Seconds(),
		Errors:         make(map[string]int64),
	}
	var resp, service hist
	var written int64
	var cnt counts
	add := func(st *stats) {
		cnt.add(&st.c)
		res.Ops += st.ops
		res.Acked += st.acked
		res.Cohort += st.cohort
		res.CohortFailed += st.cohortFailed
		written += st.written
		resp.merge(&st.resp)
		service.merge(&st.service)
		for e, n := range st.errs {
			res.Errors[e] += n
		}
	}
	for _, wk := range s.workers {
		wk.c.mu.Lock()
		add(&wk.stats)
		res.Unsent += wk.unsent
		wk.c.mu.Unlock()
	}
	// The k8s watches answer progress requests and carry events.
	for _, ws := range s.k8sWatches {
		ws.c.mu.Lock()
		add(&ws.stats)
		ws.c.mu.Unlock()
	}
	res.Throughput = float64(res.Ops) / cfg.duration.Seconds()
	res.Latency = resp.summary()
	res.ServiceLatency = service.summary()

	if len(s.watches) > 0 {
		var lat hist
		wr := &watchResult{Watchers: cfg.watchers}
		for i, w := range s.watches {
			s.watchConns[i].mu.Lock()
			wr.Events += w.events
			lat.merge(&w.lat)
			for e, n := range w.stats.errs {
				res.Errors[e] += n
			}
			s.watchConns[i].mu.Unlock()
		}
		wr.EventsPerS = float64(wr.Events) / cfg.duration.Seconds()
		wr.Latency = lat.summary()
		res.Watch = wr
	}
	if s.trace != nil {
		s.summarizeK8s(res)
	}
	if res.Ops > 0 {
		res.MinWrittenBytesPerOp = float64(written) / float64(res.Ops)
		n := float64(res.Ops)
		res.PerOp = perOp{
			OutBytes: float64(cnt.Out) / n, InBytes: float64(cnt.In) / n,
			KVs: float64(cnt.KVs) / n, PrevKVs: float64(cnt.PrevKVs) / n, Events: float64(cnt.Events) / n,
			Writes: float64(cnt.Writes) / n, Creates: float64(cnt.Creates) / n, Deletes: float64(cnt.Deletes) / n,
			Entries: float64(cnt.Entries) / n, Checks: float64(cnt.Checks) / n, Leases: float64(cnt.Leases) / n,
		}
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
	res.PeerSentBytesPerOp = peerBytesPerOp(samples[0].nodes, samples[1].nodes, res.Ops)
	if len(res.ServerCPUUsPerNode) > 0 {
		res.LeaderCPUUsPerOp = res.ServerCPUUsPerNode[0]
	}
	res.ServerCPUUsPerOp = perOp(total, res.Ops)
	self := samples[1].self - samples[0].self
	res.ClientCPUUsPerOp = perOp(self, res.Ops)
	res.ClientCores = self.Seconds() / elapsed.Seconds()
	if cfg.disk != "" && res.Ops > 0 {
		written := samples[1].diskBytes - samples[0].diskBytes
		res.DiskBytesPerOp = float64(written) / float64(res.Ops)
	}
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
