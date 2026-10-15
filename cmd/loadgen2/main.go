// Command loadgen2 is a throwaway prototype for issue #188: a shared-nothing
// load generator for the Kubernetes-like etcd mix. Each worker goroutine
// owns one TLS connection, its keys, expected state, RNG, histogram and
// logs, and does its own sends and receives. Watches run on their own
// connections and goroutines. Results are joined after the run.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var epoch = time.Now()

func now() int64 { return int64(time.Since(epoch)) }

var (
	resources    []string
	resPrefix    [][]byte
	sizeTable    [][]int32 // per resource, pre-drawn value sizes from the k8s table
	resForNew    []uint8   // pre-drawn resources of new keys, by object count
	workersTotal int
)

func initTables(seed uint64) {
	r := rand.New(rand.NewPCG(seed, 99))
	resources = k8sResources()
	for _, res := range resources {
		resPrefix = append(resPrefix, []byte("/registry/"+res+"/"))
		t := make([]int32, 8192)
		for i := range t {
			t[i] = int32(k8sSizes[drawBin(r, res)].draw(r))
		}
		sizeTable = append(sizeTable, t)
	}
	resForNew = make([]uint8, 8192)
	for i := range resForNew {
		res := k8sSizes[drawBin(r, "")].resource
		resForNew[i] = uint8(slices.Index(resources, res))
	}
}

type result struct {
	Label        string             `json:"label"`
	Valid        bool               `json:"valid"`
	GOMAXPROCS   int                `json:"gomaxprocs"`
	Workers      int                `json:"workers"`
	Streams      int                `json:"streams_per_worker"`
	WatchConns   int                `json:"watch_conns"`
	Keys         int                `json:"keys"`
	LiveKeys     int                `json:"live_keys"`
	PreloadS     float64            `json:"preload_s"`
	Ops          int64              `json:"ops"`
	OpsPerS      float64            `json:"ops_per_s"`
	ByOp         map[string]int64   `json:"by_op"`
	GenCores     float64            `json:"gen_cores"`
	GenUsPerOp   float64            `json:"gen_us_per_op"`
	FakeCores    float64            `json:"fake_cores"`
	FakeUsPerOp  float64            `json:"fake_us_per_op"`
	Latency      latency            `json:"latency_us"`
	WatchEvents  int64              `json:"watch_events_window"`
	WatchEPS     float64            `json:"watch_events_per_s"`
	Writes       int                `json:"writes_total"`
	WatchSeen    int                `json:"watch_events_total"`
	Checks       map[string]int     `json:"check_failures"`
	CheckedItems map[string]int     `json:"checked"`
	Load1        string             `json:"load1"`
	GCs          uint32             `json:"gc_cycles_window"`
	Rate          float64            `json:"target_rate"`
	ForeignEvents int64              `json:"foreign_watch_events"`
	Behind       int64              `json:"behind_schedule"`
	Extra        map[string]float64 `json:"extra,omitempty"`
}

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:42000", "server")
	cacert := flag.String("cacert", "", "CA")
	cert := flag.String("cert", "", "client cert")
	key := flag.String("key", "", "client key")
	nw := flag.Int("workers", 0, "workers (0: 16 x GOMAXPROCS; fakeetcd serves each connection on one goroutine)")
	S := flag.Int("streams", 64, "requests in flight per worker")
	nkeys := flag.Int("keys", 300000, "keys preloaded")
	watchConns := flag.Int("watch-conns", 3, "connections carrying the 27 watches")
	warmup := flag.Duration("warmup", 2*time.Second, "warmup")
	dur := flag.Duration("duration", 8*time.Second, "measured window")
	seed := flag.Uint64("seed", 1, "seed")
	pid := flag.Int("pid", 0, "server pid for CPU")
	out := flag.String("out", "", "JSON result file")
	cpuprofile := flag.String("cpuprofile", "", "CPU profile of the measured window")
	instance := flag.Int("instance", 0, "instance number; key owners are disjoint across instances")
	mixS := flag.String("mix", "25,10,40,15,10", "get,currev,update,create,delete")
	label := flag.String("label", "", "label")
	watchNow := flag.Bool("watch-now", false, "start watches at the current revision, for several instances on one server")
	rate := flag.Float64("rate", 0, "fixed total rate in ops/s (0: closed loop at max)")
	flag.IntVar(&maxBatch, "max-batch", 64<<10, "flush once this many request bytes are pending even if more answers are readable (0: flush after every read)")
	flag.Parse()

	var mix [5]int
	sum := 0
	for i, p := range strings.Split(*mixS, ",") {
		mix[i], _ = strconv.Atoi(p)
		sum += mix[i]
	}
	if sum != 100 {
		log.Fatal("mix must sum to 100")
	}
	procs := runtime.GOMAXPROCS(0)
	if *nw == 0 {
		*nw = 16 * procs
	}
	workersTotal = *nw
	initTables(*seed)
	pem, err := os.ReadFile(*cacert)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	kp, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		log.Fatal(err)
	}
	clientTLS = &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{kp}, MinVersion: tls.VersionTLS13,
		NextProtos: []string{"h2"}, DynamicRecordSizingDisabled: true}

	// Workers and preload.
	workers := make([]*worker, *nw)
	perWorker := *nkeys / *nw
	for i := range workers {
		c, err := dialH2(*endpoint)
		if err != nil {
			log.Fatal(err)
		}
		workers[i] = newWorker(i, *instance**nw+i, c, *seed+uint64(*instance)*1000, perWorker, *S, mix)
	}
	pStart := time.Now()
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Go(func() {
			if err := w.preload(); err != nil {
				w.fatal = err
			}
		})
	}
	wg.Wait()
	preloadS := time.Since(pStart).Seconds()
	var preRev int64
	for _, w := range workers {
		if w.fatal != nil {
			log.Fatalf("preload: %v", w.fatal)
		}
		preRev = max(preRev, w.lastAck)
	}
	fmt.Fprintf(os.Stderr, "preload %d keys in %.1fs, revision %d\n", perWorker**nw, preloadS, preRev)

	// Watches from the revision after preload. With several instances
	// the others may have written since; use -instance 0 first.
	watchers := make([]*watcher, *watchConns)
	for i := range watchers {
		c, err := dialH2(*endpoint)
		if err != nil {
			log.Fatal(err)
		}
		watchers[i] = newWatcher(c)
		watchers[i].ownLo, watchers[i].ownHi = *instance**nw, (*instance+1)**nw
	}
	from := preRev + 1
	if *watchNow {
		from = 0 // other instances may have written since our preload
	}
	for r := range resources {
		watchers[r%len(watchers)].add(r, from)
	}
	for _, w := range watchers {
		if err := w.waitCreated(); err != nil {
			log.Fatalf("watch create: %v", err)
		}
	}

	t0 := now() + int64(10*time.Millisecond)
	t1 := t0 + int64(*warmup)
	t2 := t1 + int64(*dur)
	for _, w := range workers {
		w.t0, w.t1, w.t2 = t0, t1, t2
		w.rate = *rate / float64(*nw) / 1e9
	}
	var wwg sync.WaitGroup
	for _, w := range watchers {
		w.t1, w.t2 = t1, t2
		wwg.Go(w.run)
	}
	start := make(chan struct{})
	for _, w := range workers {
		wg.Go(func() {
			if err := w.run(start); err != nil {
				w.fatal = err
			}
		})
	}
	time.Sleep(time.Duration(t0 - now()))
	close(start)

	var samples [2]cpuSample
	var gc [2]runtime.MemStats
	var sampler sync.WaitGroup
	sampler.Go(func() {
		time.Sleep(time.Duration(t1 - now()))
		pids := []int{}
		if *pid != 0 {
			pids = append(pids, *pid)
		}
		samples[0], _ = sampleCPU(pids)
		runtime.ReadMemStats(&gc[0])
		var pf *os.File
		if *cpuprofile != "" {
			pf, _ = os.Create(*cpuprofile)
			pprof.StartCPUProfile(pf)
		}
		time.Sleep(time.Duration(t2 - now()))
		samples[1], _ = sampleCPU(pids)
		if pf != nil {
			pprof.StopCPUProfile()
			pf.Close()
		}
		runtime.ReadMemStats(&gc[1])
	})
	wg.Wait()
	sampler.Wait()

	// Join.
	res := result{Label: *label, GOMAXPROCS: procs, Workers: *nw, Streams: *S, WatchConns: *watchConns, Keys: perWorker * *nw,
		PreloadS: preloadS, ByOp: map[string]int64{}, Checks: map[string]int{}, CheckedItems: map[string]int{}}
	var fatal []error
	expect := make([]int64, len(resources))
	var lat hist
	var wlog []rec
	var revs []int64
	for _, w := range workers {
		if w.fatal != nil {
			fatal = append(fatal, fmt.Errorf("worker %d: %w", w.id, w.fatal))
		}
		for k, v := range w.errs {
			res.Checks[k] += v
		}
		for i, n := range w.opsWin {
			res.Ops += n
			res.ByOp[opNames[i]] += n
		}
		lat.merge(&w.lat)
		for i, n := range w.resWrites {
			expect[i] += n
		}
		wlog = append(wlog, w.wlog...)
		revs = append(revs, w.plog...)
		res.LiveKeys += len(w.live)
		res.Behind += w.behind
	}
	for _, w := range watchers {
		w.final <- expect
		w.c.tc.SetReadDeadline(time.Now())
	}
	wwg.Wait()
	var wseen []rec
	for _, w := range watchers {
		if w.fatal != nil {
			fatal = append(fatal, fmt.Errorf("watcher: %w", w.fatal))
		}
		for k, v := range w.errs {
			res.Checks[k] += v
		}
		for _, s := range w.streams {
			wseen = append(wseen, s.log...)
			res.ForeignEvents += s.foreign
			res.WatchEvents += s.winEvents
		}
	}
	for _, r := range wlog {
		revs = append(revs, r.rev)
	}
	slices.Sort(revs)
	for i := 1; i < len(revs); i++ {
		if revs[i] == revs[i-1] {
			res.Checks["duplicate write revision"]++
		}
	}
	cmp := func(a, b rec) int {
		if a.rev != b.rev {
			return int(a.rev - b.rev)
		}
		return int(int64(a.fp>>1) - int64(b.fp>>1))
	}
	slices.SortFunc(wlog, cmp)
	slices.SortFunc(wseen, cmp)
	i, j := 0, 0
	for i < len(wlog) || j < len(wseen) {
		switch {
		case j == len(wseen) || i < len(wlog) && wlog[i].rev < wseen[j].rev:
			res.Checks["write missing from watches"]++
			i++
		case i == len(wlog) || wseen[j].rev < wlog[i].rev:
			res.Checks["watch event without a write"]++
			j++
		default:
			if wlog[i].fp != wseen[j].fp {
				res.Checks["watch event differs from the write"]++
			}
			i++
			j++
		}
	}
	res.Rate = *rate
	res.Writes = len(wlog)
	res.WatchSeen = len(wseen)
	res.CheckedItems["writes_joined"] = len(wlog)
	res.CheckedItems["revisions_unique"] = len(revs)

	el := float64(samples[1].at-samples[0].at) / 1e9
	self := (samples[1].self - samples[0].self).Seconds()
	res.OpsPerS = float64(res.Ops) / dur.Seconds()
	res.GenCores = self / el
	if res.Ops > 0 {
		res.GenUsPerOp = self * 1e6 / float64(res.Ops) * (dur.Seconds() / el)
	}
	if len(samples[0].servers) > 0 {
		fs := (samples[1].servers[0] - samples[0].servers[0]).Seconds()
		res.FakeCores = fs / el
		if res.Ops > 0 {
			res.FakeUsPerOp = fs * 1e6 / float64(res.Ops) * (dur.Seconds() / el)
		}
	}
	res.Latency = lat.summary()
	res.WatchEPS = float64(res.WatchEvents) / dur.Seconds()
	res.GCs = gc[1].NumGC - gc[0].NumGC
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		res.Load1 = strings.Fields(string(b))[0]
	}
	for _, e := range fatal {
		res.Checks["fatal: "+e.Error()]++
	}
	res.Valid = len(res.Checks) == 0 && res.Ops > 0
	j2, _ := json.Marshal(res)
	fmt.Println(string(j2))
	if *out != "" {
		os.WriteFile(*out, j2, 0o644)
	}
	if !res.Valid {
		os.Exit(1)
	}
	_ = errors.New
}
