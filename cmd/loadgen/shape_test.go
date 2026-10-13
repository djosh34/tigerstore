package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShapeFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-rate", "NaN"},
		{"-rate", "+Inf"},
		{"-rate", "-1"},
		{"-arrival", "fixed"},
		{"-concurrency", "-1"},
		{"-total", "1", "-rate", "10"},
		{"-balance", "random"},
		{"-concurrency", "10", "-workload", "txn"},
		{"-workload", "list", "-keys", "999"},
		{"-workload", "list", "-value-size", "10240"},
		{"-balance", "round-robin", "-rate", "100"},
		{"-drain", "0s"},
	} {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("accepted invalid flags %v", args)
		}
	}
	cfg, err := parseFlags([]string{"-workload", "list", "-concurrency", "256", "-conns", "18"})
	if err != nil || cfg.keys != 1000 || !cfg.shaped() {
		t.Fatalf("list defaults: %+v, %v", cfg, err)
	}
	cfg, err = parseFlags(nil)
	if err != nil || cfg.shaped() {
		t.Fatalf("default path changed: %+v, %v", cfg, err)
	}
}

func TestFixedSchedule(t *testing.T) {
	for _, rate := range []float64{0.1, 1, 3, 1000, 12345.67, 1e9} {
		s := fixedSchedule{start: 1234567890, stop: 1234567890 + int64(100*time.Millisecond), rate: rate}
		if s.due(s.start-1) != 0 || s.due(s.stop) != s.count() || s.at(s.count()-1) >= s.stop || s.at(s.count()) < s.stop {
			t.Fatalf("bad schedule at rate %v", rate)
		}
		for _, at := range []int64{s.start, s.start + 1, s.start + 1234567, s.stop - 1} {
			n := s.due(at)
			if n > 0 && s.at(n-1) > at || s.at(n) <= at {
				t.Fatalf("rate %v, at %d, due %d", rate, at, n)
			}
		}
	}
	for n := range int64(100) {
		total := int64(0)
		for ep := range 3 {
			total += ticketsForEndpoint(n, ep, 3)
		}
		if total != n {
			t.Fatalf("endpoint tickets sum to %d, want %d", total, n)
		}
	}
}

func TestShapedClosedLoopBalance(t *testing.T) {
	var eps []string
	for i := range 3 {
		fs := startFake(t, 2*time.Millisecond, time.Second)
		fs.rev.Store(int64(i) * 1000000)
		eps = append(eps, fs.addr)
	}
	cfg := config{
		endpoints: eps, workload: "put", valueSize: 64, keys: 24, conns: 6, streams: 256,
		concurrency: 7, total: 73, duration: time.Second, warmup: 20 * time.Millisecond, drain: time.Second,
		pids: []int{os.Getpid()}, seed: 1,
	}
	res, err := run(cfg)
	if err != nil || !res.Valid {
		t.Fatalf("result %+v, error %v", res, err)
	}
	if res.Cohort != 73 || res.Ops != 73 || res.Latency.Count != 73 || res.Shape.ExpectedRevDelta != 73 || res.Shape.PeakInflight != 7 {
		t.Fatalf("wrong exact-cohort/concurrency accounting: %+v, %+v", res, res.Shape)
	}
	for i, ep := range res.Shape.Endpoints {
		want := ticketsForEndpoint(73, i, 3)
		if ep.Sent != want || ep.Success != want || ep.Failed != 0 || ep.Unsent != 0 {
			t.Fatalf("endpoint %d: %+v, want %d", i, ep, want)
		}
	}
}

func TestShapedFixedRateThroughDrain(t *testing.T) {
	fs := startFake(t, 30*time.Millisecond, time.Second)
	cfg := config{
		endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 8, conns: 1, streams: 256,
		concurrency: 2, arrival: "fixed", rate: 200, duration: 100 * time.Millisecond, drain: 2 * time.Second,
		pids: []int{os.Getpid()}, seed: 1,
	}
	res, err := run(cfg)
	if err != nil || !res.Valid {
		t.Fatalf("result %+v, error %v", res, err)
	}
	if res.Cohort != 20 || res.Latency.Count != 20 || res.Unsent != 0 || res.Shape.PeakInflight > 2 || res.Shape.DrainS <= 0 || res.Shape.QueueMax == 0 || res.Shape.QueueAtStop == 0 {
		t.Fatalf("missing scheduled queue or drain outcomes: %+v, %+v", res, res.Shape)
	}
	if res.Latency.Mean <= res.ServiceLatency.Mean || res.Shape.CPUThroughDrain == nil {
		t.Fatalf("missing queued latency or CPU: %+v", res)
	}
	last := res.Shape.Queue[len(res.Shape.Queue)-1]
	if last.Scheduled != 20 || last.Sent != 20 || last.Queued != 0 || last.Inflight != 0 {
		t.Fatalf("final queue sample %+v", last)
	}
}

func TestShapedDrainTimeoutAccountsCohort(t *testing.T) {
	fs := startFake(t, 30*time.Millisecond, time.Second)
	res, err := run(config{
		endpoints: []string{fs.addr}, workload: "put", valueSize: 64, keys: 8, conns: 1, streams: 256,
		concurrency: 1, arrival: "fixed", rate: 1000, duration: 20 * time.Millisecond, drain: 20 * time.Millisecond,
		pids: []int{os.Getpid()}, seed: 1,
	})
	if err == nil || res == nil || res.Valid || res.Cohort != 20 || res.CohortFailed == 0 || res.Unsent == 0 {
		t.Fatalf("timeout disappeared: %+v, %v", res, err)
	}
	if uint64(res.Cohort-res.CohortFailed) != res.Latency.Count {
		t.Fatalf("censored outcomes missing: %+v", res)
	}
}

func TestListValidation(t *testing.T) {
	ks := makeKeys(1000)
	want := make([][]byte, ks.n)
	values := makeValues(1024, 1)
	for i := range want {
		want[i] = values.next()
	}
	l := newListLoad(base{ks: ks}, want)
	response := func(indices []int, count uint64, more bool, corrupt bool) []byte {
		h := newMsg()
		h.varint(headerRevision, 1001)
		m := newMsg()
		m.sub(respHeader, h)
		for j, k := range indices {
			kv := newMsg()
			kv.bytes(kvKey, ks.key(k))
			value := want[k]
			if corrupt && j == len(indices)/2 {
				value = bytes.Clone(value)
				value[999] ^= 1
			}
			kv.bytes(kvValue, value)
			m.sub(rangeKVs, kv)
		}
		m.varint(rangeCount, count)
		if more {
			m.varint(3, 1)
		}
		return m.b
	}
	if err := l.message(nil, response(l.order, 1000, false, false)); err != nil {
		t.Fatal(err)
	}
	duplicate := slices.Clone(l.order)
	duplicate[500] = duplicate[499]
	for _, bad := range [][]byte{
		response(l.order[:999], 1000, false, false), response(l.order, 999, false, false),
		response(l.order, 1000, true, false), response(l.order, 1000, false, true),
		response(duplicate, 1000, false, false), response(append(slices.Clone(l.order), l.order[0]), 1001, false, false),
	} {
		if err := l.message(nil, bad); err == nil {
			t.Fatal("accepted incomplete, duplicate, corrupt or extra list data")
		}
	}
	request := listRequest()
	serializable, err := varintField(request.b, 7)
	if err != nil || serializable != 0 {
		t.Fatal("list is not linearizable")
	}
}

// memoryKV exercises fragmented megabyte LIST responses without running a
// database, consensus, disk, benchmarks or external processes locally.
type memoryKV struct {
	mu     sync.Mutex
	values map[string][]byte
	rev    atomic.Int64
	rpcs   atomic.Int64
}

func (s *memoryKV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.rpcs.Add(1)
	body, err := io.ReadAll(r.Body)
	if err != nil || len(body) < grpcPrefixLen {
		return
	}
	request := body[grpcPrefixLen:]
	key, err := bytesField(request, rangeKey)
	if err != nil {
		return
	}
	resp := newMsg()
	h := newMsg()
	s.mu.Lock()
	switch r.URL.Path {
	case "/etcdserverpb.KV/Put":
		value, err := bytesField(request, putValue)
		if err != nil {
			s.mu.Unlock()
			return
		}
		s.values[string(key)] = bytes.Clone(value)
		s.rev.Add(1)
	case "/etcdserverpb.KV/Range":
		end, err := bytesField(request, 2)
		if err != nil {
			s.mu.Unlock()
			return
		}
		var keys []string
		for k := range s.values {
			if k == string(key) || len(end) > 0 && k >= string(key) && k < string(end) {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			kv := newMsg()
			kv.bytes(kvKey, []byte(k))
			kv.bytes(kvValue, s.values[k])
			resp.sub(rangeKVs, kv)
		}
		resp.varint(rangeCount, uint64(len(keys)))
	}
	h.varint(headerRevision, uint64(s.rev.Load()))
	resp.sub(respHeader, h)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Trailer", "Grpc-Status")
	b := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(resp.b)))
	b = append(b, resp.b...)
	for len(b) > 0 {
		n := min(len(b), 16384)
		if _, err := w.Write(b[:n]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		b = b[n:]
	}
	w.Header().Set("Grpc-Status", "0")
}

func TestListBarriersAndReassembly(t *testing.T) {
	s := &memoryKV{values: make(map[string][]byte)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: s, Protocols: new(http.Protocols), ReadHeaderTimeout: time.Second}
	srv.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	prefix := filepath.Join(t.TempDir(), "stage")
	cfg := config{
		endpoints: []string{ln.Addr().String()}, workload: "list", valueSize: 1024, keys: 1000, conns: 1, streams: 256,
		concurrency: 2, total: 5, duration: time.Second, drain: time.Second, barrier: prefix, barrierWait: 3 * time.Second,
		maxResponse: 2 << 20, pids: []int{os.Getpid()}, seed: 1,
	}
	type outcome struct {
		res *result
		err error
	}
	finished := make(chan outcome, 1)
	go func() {
		res, err := run(cfg)
		finished <- outcome{res, err}
	}()
	waitMarker := func(suffix string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, err := os.Stat(prefix + "." + suffix); err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("no marker %s", suffix)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitMarker("ready")
	readyRev := s.rev.Load()
	readyRPCs := s.rpcs.Load()
	time.Sleep(10 * time.Millisecond)
	if s.rpcs.Load() != readyRPCs || readyRev != 1000 {
		t.Fatal("warmup or seed not drained before ready")
	}
	if err := os.WriteFile(prefix+".go", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitMarker("done")
	doneRPCs := s.rpcs.Load()
	time.Sleep(10 * time.Millisecond)
	if s.rpcs.Load() != doneRPCs || doneRPCs-readyRPCs != 5 || s.rev.Load() != readyRev {
		t.Fatal("RPCs after done, wrong cohort, or LIST modified data")
	}
	select {
	case <-finished:
		t.Fatal("result before finish")
	default:
	}
	if err := os.WriteFile(prefix+".finish", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got := <-finished
	if got.err != nil || !got.res.Valid || got.res.Latency.Count != 5 || got.res.Shape.ExpectedRevDelta != 0 {
		t.Fatalf("%+v, %v", got.res, got.err)
	}
	buffers := got.res.Shape.ResponseBuffers
	if buffers.LiveBytes != 0 || buffers.PeakLiveBytes < 1<<20 || buffers.RetainedBytes < 1<<20 || buffers.PeakRetainedBytes < buffers.RetainedBytes {
		t.Fatalf("buffer counters: %+v", buffers)
	}
}

func TestFiniteRateFlags(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := parseFlags([]string{"-rate", strconv.FormatFloat(value, 'g', -1, 64)}); err == nil {
			t.Fatal("accepted nonfinite rate")
		}
	}
}
