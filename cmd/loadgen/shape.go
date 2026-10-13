package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"time"
)

func (cfg config) shaped() bool {
	return cfg.concurrency > 0 || cfg.balance != "" || cfg.arrival == "fixed" || cfg.total > 0 || cfg.barrier != "" || cfg.workload == "list" || cfg.dispatch == "connections"
}

type endpointCounts struct {
	Endpoint string `json:"endpoint"`
	Sent     int64  `json:"sent"`
	Success  int64  `json:"success"`
	Failed   int64  `json:"failed"`
	Unsent   int64  `json:"unsent"`
}

type queueSample struct {
	ElapsedS  float64 `json:"elapsed_s"`
	Scheduled int64   `json:"scheduled"`
	Sent      int64   `json:"sent"`
	Queued    int64   `json:"queued"`
	Inflight  int64   `json:"inflight"`
}

type shapeResult struct {
	Concurrency      int              `json:"concurrency"`
	PeakInflight     int64            `json:"peak_inflight"`
	Arrival          string           `json:"arrival"`
	Dispatch         string           `json:"dispatch"`
	Expected         int64            `json:"expected"`
	Success          int64            `json:"success"`
	ExpectedRevDelta int64            `json:"expected_revision_delta"`
	Endpoints        []endpointCounts `json:"endpoint_counts"`
	Queue            []queueSample    `json:"queue_samples"`
	QueueMax         int64            `json:"queue_max_observed"`
	QueueMean        float64          `json:"queue_mean_sampled"`
	QueueAtStop      int64            `json:"queue_at_arrival_stop"`
	InflightAtStop   int64            `json:"inflight_at_arrival_stop"`
	ArrivalWindowS   float64          `json:"arrival_window_s"`
	ElapsedS         float64          `json:"through_drain_s"`
	DrainS           float64          `json:"drain_s"`
	CohortThroughput float64          `json:"cohort_throughput"`
	CPUThroughDrain  *cpuResult       `json:"cpu_through_drain"`
	StartUnixNS      int64            `json:"start_unix_ns"`
	StopUnixNS       int64            `json:"arrival_stop_unix_ns"`
	DoneUnixNS       int64            `json:"done_unix_ns"`
	ResponseBuffers  responseBuffers  `json:"response_buffers"`
}

type cpuResult struct {
	ElapsedS float64   `json:"elapsed_s"`
	Server   []float64 `json:"server_us_per_op_by_node"`
	Client   float64   `json:"client_us_per_op"`
}

type responseBuffers struct {
	LiveBytes         int64 `json:"live_bytes_at_done"`
	PeakLiveBytes     int64 `json:"peak_live_bytes"`
	RetainedBytes     int64 `json:"retained_capacity_bytes_at_done"`
	PeakRetainedBytes int64 `json:"peak_retained_capacity_bytes"`
}

type byteCounter struct {
	current atomic.Int64
	peak    atomic.Int64
}

func (c *byteCounter) add(delta int64) {
	n := c.current.Add(delta)
	for old := c.peak.Load(); n > old; old = c.peak.Load() {
		if c.peak.CompareAndSwap(old, n) {
			break
		}
	}
}

type reassemblyCounter struct {
	live, capacity byteCounter
}

func (c *reassemblyCounter) snapshot() responseBuffers {
	return responseBuffers{LiveBytes: c.live.current.Load(), PeakLiveBytes: c.live.peak.Load(), RetainedBytes: c.capacity.current.Load(), PeakRetainedBytes: c.capacity.peak.Load()}
}

type inflightCounter struct {
	current atomic.Int64
	peak    atomic.Int64
}

func (m *inflightCounter) started() {
	n := m.current.Add(1)
	for old := m.peak.Load(); n > old; old = m.peak.Load() {
		if m.peak.CompareAndSwap(old, n) {
			break
		}
	}
}

// fixedSchedule computes timestamps from the global request number, never
// from the previous send or completion. No backlog allocation is needed.
type fixedSchedule struct {
	start, stop int64
	rate        float64
}

func (s fixedSchedule) at(n int64) int64 {
	return s.start + int64(float64(n)*1e9/s.rate)
}

func (s fixedSchedule) count() int64 {
	n := int64(math.Ceil(float64(s.stop-s.start) * s.rate / 1e9))
	// Correct floating-point rounding at the exclusive stop boundary.
	for n > 0 && s.at(n-1) >= s.stop {
		n--
	}
	for s.at(n) < s.stop {
		n++
	}
	return n
}

func (s fixedSchedule) due(at int64) int64 {
	if at < s.start {
		return 0
	}
	if at >= s.stop {
		return s.count()
	}
	n := int64(float64(at-s.start)*s.rate/1e9) + 1
	for n > 0 && s.at(n-1) > at {
		n--
	}
	for s.at(n) <= at && s.at(n) < s.stop {
		n++
	}
	return n
}

// onceLoad probes each connection without letting fast ones hide an unused
// connection. It wraps an ordinary validator and sends exactly one RPC.
type onceLoad struct {
	load
	sent bool
}

func (l *onceLoad) start(c *conn, st *stream) bool {
	if l.sent || !l.load.start(c, st) {
		return false
	}
	l.sent = true
	return true
}

func (l *onceLoad) exhausted() bool { return l.sent }

func runShaped(cfg config) (*result, error) {
	if cfg.dispatch == "connections" {
		if err := cfg.validateConnectionControl(); err != nil {
			return nil, err
		}
		cfg.streams = cfg.concurrency / cfg.conns
	}
	if cfg.rate > 0 && cfg.arrival != "fixed" {
		return nil, errors.New("experiment open-loop runs need -arrival fixed; the default dispatcher retains Poisson arrivals")
	}
	if cfg.workload == "list" && (cfg.keys != 1000 || cfg.valueSize != 1024) {
		return nil, errors.New("list needs exactly 1000 keys with 1024-byte values")
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = cfg.conns * cfg.streams
	}
	if cfg.drain == 0 {
		cfg.drain = 30 * time.Second
	}
	if cfg.barrierWait == 0 {
		cfg.barrierWait = 5 * time.Minute
	}
	if len(cfg.endpoints) == 0 || cfg.conns < len(cfg.endpoints) {
		return nil, errors.New("need at least one connection per endpoint")
	}
	if cfg.barrier != "" {
		for _, suffix := range []string{"ready", "go", "done", "finish"} {
			if _, err := os.Stat(cfg.barrier + "." + suffix); err == nil {
				return nil, fmt.Errorf("barrier %s.%s already exists", cfg.barrier, suffix)
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("barrier %s.%s: %w", cfg.barrier, suffix, err)
			}
		}
	}
	ks := makeKeys(cfg.keys)
	vals := makeValues(cfg.valueSize, cfg.seed)
	writes := newRevisionSet()
	wake := make(chan struct{}, 1)
	var buffers reassemblyCounter
	var workers []*worker
	defer func() {
		for _, wk := range workers {
			wk.c.close()
		}
	}()
	for i := range cfg.conns {
		sharedWake := wake
		if cfg.dispatch == "connections" {
			sharedWake = nil
		}
		c, err := dialShared(cfg.endpoints[i%len(cfg.endpoints)], sharedWake, &buffers, cfg.maxResponse)
		if err != nil {
			return nil, err
		}
		workers = append(workers, newWorker(c, cfg.seed+uint64(i)))
	}
	mkBase := func(i int, m *msg, path string) (base, error) {
		t, err := newTemplate(workers[i].c.addr, path, m, true)
		if err != nil {
			return base{}, err
		}
		v := *vals
		v.off = i * (valuePoolSize / cfg.conns) / cfg.valueSize * cfg.valueSize
		b := base{tpl: t, ks: ks, vals: v, rng: workers[i].rng, stats: &workers[i].stats, limit: cfg.concurrency, writes: writes}
		for k := i; k < cfg.keys; k += cfg.conns {
			b.own = append(b.own, k)
		}
		return b, nil
	}
	// Seed every shape, including Put, before warming connections. The
	// caller fences replicas after ready and before releasing go.
	putVals := make([][]byte, cfg.keys)
	seedLoads := make([]load, cfg.conns)
	for i := range workers {
		b, err := mkBase(i, putRequest(ks.size, cfg.valueSize), kvPath("put"))
		if err != nil {
			return nil, err
		}
		seedLoads[i] = &putLoad{base: b, once: true, vals: putVals}
	}
	start := now()
	if err := runPhase(workers, seedLoads, phase{start: start, stop: start + int64(preloadTimeout), drain: cfg.drain}); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	for _, l := range seedLoads {
		if !l.exhausted() {
			return nil, errors.New("seed did not finish")
		}
	}
	if err := phaseErrors(workers); err != nil {
		return nil, fmt.Errorf("seed: %w", err)
	}
	loads := make([]load, cfg.conns)
	probes := make([]load, cfg.conns)
	for i := range workers {
		m := rangeRequest(ks.size)
		switch cfg.workload {
		case "put":
			m = putRequest(ks.size, cfg.valueSize)
		case "list":
			m = listRequest()
		}
		b, err := mkBase(i, m, kvPath(cfg.workload))
		if err != nil {
			return nil, err
		}
		if cfg.dispatch == "connections" {
			b.limit = cfg.concurrency / cfg.conns
		}
		switch cfg.workload {
		case "put":
			loads[i] = &putLoad{base: b}
		case "range":
			loads[i] = &rangeLoad{base: b, valueSize: cfg.valueSize, want: putVals}
		case "list":
			loads[i] = newListLoad(b, putVals)
		default:
			return nil, fmt.Errorf("unsupported experiment workload %q", cfg.workload)
		}
		probes[i] = &onceLoad{load: loads[i]}
	}
	start = now()
	if err := runPhase(workers, probes, phase{start: start, stop: start + int64(cfg.drain), drain: cfg.drain}); err != nil {
		return nil, fmt.Errorf("connection probes: %w", err)
	}
	if err := phaseErrors(workers); err != nil {
		return nil, fmt.Errorf("connection probes: %w", err)
	}
	runDispatch := func(duration time.Duration, total int64) (*shapeResult, [2]cpuSample, error) {
		if cfg.dispatch == "connections" {
			return dispatchConnections(cfg, workers, loads, duration)
		}
		return dispatch(cfg, workers, loads, wake, duration, total)
	}
	if cfg.warmup > 0 {
		if _, _, err := runDispatch(cfg.warmup, 0); err != nil {
			return nil, fmt.Errorf("warmup: %w", err)
		}
		if err := phaseErrors(workers); err != nil {
			return nil, fmt.Errorf("warmup: %w", err)
		}
	}
	// All seed/probe/warmup RPCs have ended. Keep stats at the same address
	// because validators retain pointers to them.
	for _, wk := range workers {
		wk.stats = stats{errs: make(map[string]int64)}
		wk.unsent = 0
	}
	if err := barrierPublish(cfg.barrier, "ready", map[string]any{"keys": cfg.keys, "connections": cfg.conns, "workload": cfg.workload}); err != nil {
		return nil, err
	}
	if err := barrierWait(cfg, "go", workers); err != nil {
		return nil, err
	}
	// Keep warmup capacity in the baseline but reset measurement peaks.
	buffers.live.peak.Store(buffers.live.current.Load())
	buffers.capacity.peak.Store(buffers.capacity.current.Load())
	shape, samples, phaseErr := runDispatch(cfg.duration, cfg.total)
	shape.ResponseBuffers = buffers.snapshot()
	phaseErr = errors.Join(phaseErr, phaseErrors(workers))
	// From here to finish there are no RPCs. Only HTTP/2 controls may be
	// sent while the caller fences replicas and captures external counters.
	doneErr := barrierPublish(cfg.barrier, "done", map[string]any{"expected": shape.Expected, "success": shape.Success, "error": errorString(phaseErr)})
	finishErr := barrierWait(cfg, "finish", workers)
	if cfg.total > 0 {
		cfg.duration = time.Duration(shape.ElapsedS * 1e9)
	}
	res := summarize(cfg, workers, nil, samples)
	res.Shape = shape
	errs := []error{phaseErr, doneErr, finishErr}
	if len(res.Errors) > 0 {
		errs = append(errs, errors.New("request errors, see result"))
	}
	if len(cfg.pids) == 0 {
		errs = append(errs, errors.New("no -pids, so no server CPU"))
	}
	if res.Cohort == 0 || shape.Success+res.CohortFailed != shape.Expected || res.Latency.Count != uint64(shape.Success) {
		errs = append(errs, errors.New("cohort outcome/latency accounting mismatch or empty cohort"))
	}
	res.Valid = errors.Join(errs...) == nil
	return res, errors.Join(errs...)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func barrierPublish(prefix, suffix string, value any) error {
	if prefix == "" {
		return nil
	}
	// Rename makes publication
	// atomic for the sampler, which may react immediately to the marker.
	path := prefix + "." + suffix
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", b, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func barrierWait(cfg config, suffix string, workers []*worker) error {
	if cfg.barrier == "" {
		return nil
	}
	deadline := time.Now().Add(cfg.barrierWait)
	for {
		if _, err := os.Stat(cfg.barrier + "." + suffix); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s.%s", cfg.barrier, suffix)
		}
		for _, wk := range workers {
			c := wk.c
			c.mu.Lock()
			c.appendControl()
			err := c.err
			c.mu.Unlock()
			if err != nil {
				return err
			}
			if err := c.flush(); err != nil {
				return err
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}
