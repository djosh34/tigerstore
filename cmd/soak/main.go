// Command soak runs a 3-node tigerstore cluster for hours under a memory
// limit and checks it (#148). Workers write Kubernetes-shaped objects at
// a steady rate, as an apiserver would: creates and updates in Txns that
// compare the create or mod revision, deletes, reads and paginated
// lists, events on leases that expire, a watch on every resource prefix
// and a compaction every few minutes. Each worker owns its objects, so it
// knows what every answer must hold, and every watch must send exactly
// the changes the workers made at the revisions it covers.
//
// Meanwhile a node is killed every few minutes and started again, and
// now and then a follower is stopped while a burst of writes moves the
// leader past what it keeps for followers, so it must install a snapshot
// when it comes back. The values take more than the memory limit, so
// reads go to the disk. After each fault the cluster must commit writes
// within the liveness bound and its nodes must agree on the hash of
// their keys. No node may exit on its own or be killed by the memory
// limit, and no alarm may go off.
//
// Usage:
//
//	soak -tigerstore ./tigerstore -duration 5h -out soak-out
//
// It needs sudo -n for the cgroups. The run writes report.json and the
// node logs to -out and exits 1 on a finding.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type config struct {
	tigerstore, out string
	duration        time.Duration
	seed            uint64
	objects         int
	memory          int64
	diskBudget      int64
	workers         int
	rate            float64
	killEvery       time.Duration
	holdEvery       time.Duration
	compactEvery    time.Duration
	burst           int64
	liveness        time.Duration
	requestTimeout  time.Duration
	nodeArgs        []string
}

func main() {
	var c config
	flag.StringVar(&c.tigerstore, "tigerstore", "", "tigerstore binary (required)")
	flag.StringVar(&c.out, "out", "soak-out", "directory for the data, the logs and report.json")
	flag.DurationVar(&c.duration, "duration", 5*time.Hour, "how long the churn and the faults run")
	flag.Uint64Var(&c.seed, "seed", 1, "seed of the workers and the faults")
	flag.IntVar(&c.objects, "objects", 1_000_000, "objects the workers create first and keep churning")
	flag.Int64Var(&c.memory, "memory", 1<<30, "memory limit of each node's cgroup, in bytes; also its --memory-budget-bytes")
	flag.Int64Var(&c.diskBudget, "disk-budget", 0, "--disk-budget-bytes of each node; 0 leaves the default")
	flag.IntVar(&c.workers, "workers", 16, "sequential clients")
	flag.Float64Var(&c.rate, "rate", 400, "calls per second over all workers")
	flag.DurationVar(&c.killEvery, "kill-every", 4*time.Minute, "mean time between node kills")
	flag.DurationVar(&c.holdEvery, "hold-every", 30*time.Minute, "mean time between follower holds")
	flag.DurationVar(&c.compactEvery, "compact-every", 5*time.Minute, "compaction interval; each compacts to the revision of the one before, as the apiserver does")
	flag.Int64Var(&c.burst, "burst", 256<<20, "bytes a follower hold writes while the follower is stopped")
	// etcd's request timeout: 5 s and two election timeouts.
	flag.DurationVar(&c.liveness, "liveness", 7*time.Second, "time within which the cluster must commit a write after a fault")
	flag.DurationVar(&c.requestTimeout, "request-timeout", 7*time.Second, "deadline of one call")
	flag.Func("node-arg", "one more argument of every node; repeat for more", func(s string) error {
		c.nodeArgs = append(c.nodeArgs, s)
		return nil
	})
	flag.Parse()
	if c.tigerstore == "" || flag.NArg() != 0 || c.workers < 1 || c.rate <= 0 {
		flag.Usage()
		os.Exit(2)
	}
	rep, err := run(c)
	if rep != nil {
		if b, jerr := json.MarshalIndent(rep, "", "  "); jerr == nil {
			_ = os.WriteFile(filepath.Join(c.out, "report.json"), b, 0o644)
		}
	}
	if err != nil {
		if rep != nil && rep.NodesAtFailure != "" {
			fmt.Fprintf(os.Stderr, "the nodes when the run failed:\n%s", rep.NodesAtFailure)
		}
		fmt.Fprintf(os.Stderr, "FAIL: %v\nevidence in %s\nreplay (needs sudo -n): %s\n", err, c.out, replayCommand(c))
		os.Exit(1)
	}
	_, _ = fmt.Fprintf(os.Stdout, "PASS: %s\n", rep.summary())
}

// report is what the run did and saw.
type report struct {
	Config        map[string]any `json:"config"`
	Started       time.Time      `json:"started"`
	Seconds       float64        `json:"seconds"`
	Preload       float64        `json:"preload_seconds"`
	Kills         int            `json:"kills"`
	Holds         int            `json:"holds"`
	Snapshots     int            `json:"snapshot_fetches"`
	Compactions   int            `json:"compactions"`
	CompactErrors int            `json:"compact_errors"`
	Hashes        int            `json:"hash_checks"`
	// Recovery is the longest time from a kill to a write the others
	// committed, CatchUp from a node's start or resume to a write committed
	// through it, and Restart from a node's start to its first answer.
	Recovery   float64     `json:"max_recovery_seconds"`
	CatchUp    float64     `json:"max_catch_up_seconds"`
	Restart    float64     `json:"max_restart_seconds"`
	Revision   int64       `json:"final_revision"`
	Load       *stats      `json:"load"`
	Watches    []*watched  `json:"watches"`
	Nodes      [3]nodeStat `json:"nodes"`
	Unexpected []string    `json:"unexpected_exits,omitempty"`
	Findings   []string    `json:"findings,omitempty"`
	// NodesAtFailure is how each node stood when the run failed.
	NodesAtFailure string `json:"nodes_at_failure,omitempty"`
}

// nodeStat is the most a node took.
type nodeStat struct {
	RSS      int64 `json:"max_rss_bytes"`
	Peak     int64 `json:"cgroup_peak_bytes"`
	OOMKills int64 `json:"oom_kills"`
	Disk     int64 `json:"max_data_dir_bytes"`
	Starts   int   `json:"starts"`
	// Cleaned and the cache's hits and misses are the node's counters at
	// the end, since its last start.
	Cleaned     int64 `json:"cleaned_bytes"`
	CacheHits   int64 `json:"cache_hits"`
	CacheMisses int64 `json:"cache_misses"`
}

func (r *report) summary() string {
	var calls, cleaned int64
	for _, n := range r.Load.Calls {
		calls += n
	}
	for _, n := range r.Nodes {
		cleaned += n.Cleaned
	}
	return fmt.Sprintf("%.0f s, %d calls, %d kills, %d holds with %d snapshot fetches, %d compactions, %d hash checks, final revision %d, max recovery %.1f s, max catch-up %.1f s, %d MiB cleaned",
		r.Seconds, calls, r.Kills, r.Holds, r.Snapshots, r.Compactions, r.Hashes, r.Revision, r.Recovery, r.CatchUp, cleaned>>20)
}

// soak is one run.
type soak struct {
	cfg  config
	c    *cluster
	l    *load
	ws   *watches
	rng  *rand.Rand
	rep  *report
	mu   sync.Mutex
	errs []error
	// stop ends the run on the first finding.
	stop context.CancelFunc
}

func (s *soak) fail(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
	s.stop()
}

func (s *soak) failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.errs...)
}

func run(cfg config) (*report, error) {
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return nil, err
	}
	rep := &report{Started: time.Now().UTC(), Config: map[string]any{
		"duration": cfg.duration.String(), "seed": cfg.seed, "objects": cfg.objects, "memory": cfg.memory,
		"disk_budget": cfg.diskBudget, "workers": cfg.workers, "rate": cfg.rate, "kill_every": cfg.killEvery.String(),
		"hold_every": cfg.holdEvery.String(), "compact_every": cfg.compactEvery.String(), "burst": cfg.burst,
		"liveness": cfg.liveness.String(),
	}}
	c, err := newCluster(cfg)
	if c != nil {
		defer c.close()
	}
	if err != nil {
		return rep, err
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	s := &soak{cfg: cfg, c: c, rng: rand.New(rand.NewPCG(cfg.seed, 0x50a4)), rep: rep, stop: stop}
	// The exit is reported with the others at the end; it ends the run now.
	c.exited = func(string) { stop() }
	err = s.run(ctx)
	if err != nil || s.failed() != nil {
		rep.NodesAtFailure = c.report(context.Background())
	}
	c.stop()
	for i, n := range c.nodes {
		rep.Nodes[i].Starts = n.starts
		st, serr := n.cg.Stats()
		if serr != nil {
			err = errors.Join(err, fmt.Errorf("memory of node%d: %w", i+1, serr))
			continue
		}
		rep.Nodes[i].Peak, rep.Nodes[i].OOMKills = st.PeakBytes, st.OOMKills
		if st.OOMKills > 0 {
			err = errors.Join(err, fmt.Errorf("the memory limit killed node%d %d times", i+1, st.OOMKills))
		}
	}
	if s.l != nil {
		rep.Load = &s.l.stats
	}
	if s.ws != nil {
		rep.Watches = s.ws.per
	}
	rep.Unexpected = c.unexpected()
	if len(rep.Unexpected) > 0 {
		err = errors.Join(err, fmt.Errorf("unexpected exits: %s", strings.Join(rep.Unexpected, "; ")))
	}
	err = errors.Join(err, s.failed())
	if err != nil {
		rep.Findings = strings.Split(err.Error(), "\n")
	}
	rep.Seconds = time.Since(rep.Started).Seconds()
	return rep, err
}

func (s *soak) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stdout, "%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func (s *soak) run(ctx context.Context) error {
	c := s.c
	for _, n := range c.nodes {
		if err := c.start(n); err != nil {
			return err
		}
		var err error
		if n.one, err = c.dial(c.endpoints[n.i]); err != nil {
			return err
		}
	}
	if err := c.ready(ctx, 60*time.Second); err != nil {
		return err
	}
	cl, err := c.dial(c.endpoints[:]...)
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }()
	wcl, err := c.dial(c.endpoints[:]...)
	if err != nil {
		return err
	}
	defer func() { _ = wcl.Close() }()
	s.l = newLoad(s.cfg, cl, s.fail)
	s.logf("preloading %d objects", s.cfg.objects)
	start := time.Now()
	var wg sync.WaitGroup
	for _, w := range s.l.workers {
		wg.Go(func() {
			if err := w.preload(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.fail(fmt.Errorf("preload: %w", err))
			}
		})
	}
	wg.Wait()
	if err := s.failed(); err != nil {
		return err
	}
	s.rep.Preload = time.Since(start).Seconds()
	s.logf("preloaded in %.0f s at revision %d; data dirs %d MiB", s.rep.Preload, s.l.acked.Load(), dirSize(c.dir)>>20)

	s.ws = newWatches(s.l, wcl)
	watchCtx, stopWatches := context.WithCancel(ctx)
	defer stopWatches()
	var watching sync.WaitGroup
	watching.Go(func() { s.ws.run(watchCtx, s.l.acked.Load()+1) })
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	var bg sync.WaitGroup
	for _, w := range s.l.workers {
		bg.Go(func() {
			if err := w.run(loadCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.fail(err)
			}
		})
	}
	bg.Go(func() { s.sample(loadCtx) })
	bg.Go(func() { s.compact(loadCtx) })
	bg.Go(func() {
		for {
			select {
			case <-loadCtx.Done():
				return
			case <-time.After(time.Minute):
			}
			if err := s.ws.check(false); err != nil {
				s.fail(err)
			}
		}
	})

	s.faults(loadCtx, time.Now().Add(s.cfg.duration))
	stopLoad()
	bg.Wait()
	if err := s.failed(); err != nil {
		return err
	}
	err = s.finish(ctx)
	stopWatches()
	watching.Wait()
	if err != nil {
		return err
	}
	if err := s.ws.check(true); err != nil {
		return err
	}
	if s.rep.Compactions == 0 && s.cfg.duration > 2*s.cfg.compactEvery {
		return fmt.Errorf("no compaction succeeded, %d failed", s.rep.CompactErrors)
	}
	if err := s.hashes(ctx); err != nil {
		return err
	}
	return s.counters(ctx)
}

// faults kills a node every few minutes and holds a follower now and
// then, until the end, checking the cluster after each.
func (s *soak) faults(ctx context.Context, end time.Time) {
	exp := func(mean time.Duration) time.Duration {
		return time.Duration(s.rng.ExpFloat64() * float64(mean))
	}
	nextKill := time.Now().Add(exp(s.cfg.killEvery))
	nextHold := time.Now().Add(exp(s.cfg.holdEvery))
	for ctx.Err() == nil && time.Now().Before(end) {
		next := end
		for _, t := range []time.Time{nextKill, nextHold} {
			if t.Before(next) {
				next = t
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		var err error
		switch {
		case !time.Now().Before(end):
			return
		case !nextHold.After(nextKill):
			err = s.hold(ctx)
			nextHold = time.Now().Add(exp(s.cfg.holdEvery))
		default:
			err = s.kill(ctx)
			nextKill = time.Now().Add(exp(s.cfg.killEvery))
		}
		if err == nil {
			err = s.hashes(ctx)
		}
		if err != nil {
			if ctx.Err() == nil {
				s.fail(err)
			}
			return
		}
	}
}

// kill kills a node, which the cluster must survive, and starts it again
// a few seconds later.
func (s *soak) kill(ctx context.Context) error {
	n := s.c.nodes[s.rng.IntN(3)]
	if lead, err := s.c.leader(ctx); err == nil && s.rng.IntN(2) == 0 {
		n = s.c.nodes[lead]
	}
	s.rep.Kills++
	at := time.Now()
	s.logf("kill node%d", n.i+1)
	if err := n.signal(syscall.SIGKILL); err != nil {
		return err
	}
	if err := s.commit(ctx, "the kill", at, others(n.i), s.cfg.liveness, &s.rep.Recovery); err != nil {
		return fmt.Errorf("after node%d was killed: %w", n.i+1, err)
	}
	pause(ctx, time.Duration(1+s.rng.IntN(10))*time.Second)
	return s.restart(ctx, n)
}

// restart starts n and waits for its first linearizable answer.
func (s *soak) restart(ctx context.Context, n *node) error {
	at := time.Now()
	if err := s.c.start(n); err != nil {
		return err
	}
	for {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := n.one.Get(cctx, "/soak/probe")
		cancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(at) > catchUp {
			return fmt.Errorf("node%d did not answer within %v of its start: %w", n.i+1, catchUp, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	d := time.Since(at).Seconds()
	s.rep.Restart = max(s.rep.Restart, d)
	s.logf("node%d answers %.1f s after its start", n.i+1, d)
	return s.commit(ctx, "its start", at, []int{n.i}, catchUp, &s.rep.CatchUp)
}

// catchUp bounds the time from a node's start or resume, which recovers
// its log or installs a snapshot, to a write committed through it.
const catchUp = 2 * time.Minute

// commit writes a probe through each of the nodes until one commits it,
// within bound from at.
func (s *soak) commit(ctx context.Context, what string, at time.Time, through []int, bound time.Duration, worst *float64) error {
	var last error
	for {
		for _, i := range through {
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			_, err := s.c.nodes[i].one.Put(cctx, "/soak/probe", at.String())
			cancel()
			last = err
			if err == nil {
				d := time.Since(at)
				*worst = max(*worst, d.Seconds())
				s.logf("a write through node%d committed %.1f s after %s", i+1, d.Seconds(), what)
				if d > bound {
					return fmt.Errorf("the first write through node%d committed %.1f s after %s, past the bound of %v", i+1, d.Seconds(), what, bound)
				}
				return nil
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(at) > bound {
			return fmt.Errorf("no write committed within %v of %s; the last try through node%d got: %w", bound, what, through[len(through)-1]+1, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// hold stops a follower, writes a burst that moves the leader past what
// it keeps for followers, and resumes it: it must fetch a snapshot.
func (s *soak) hold(ctx context.Context) error {
	lead, err := s.c.leader(ctx)
	if err != nil {
		return err
	}
	f := s.c.nodes[others(lead)[s.rng.IntN(2)]]
	s.rep.Holds++
	s.logf("hold node%d while %d MiB go in", f.i+1, s.cfg.burst>>20)
	from := logSize(f)
	// A call waiting in a stopped node's socket applies when the node
	// resumes, after its worker gave up and read the object, and the
	// model would take it for a write no one made. So the workers' client
	// leaves the node out while it is held, from when the calls it has
	// already sent there have ended.
	var rest []string
	for _, i := range others(f.i) {
		rest = append(rest, s.c.endpoints[i])
	}
	s.l.cl.SetEndpoints(rest...)
	defer s.l.cl.SetEndpoints(s.c.endpoints[:]...)
	pause(ctx, s.cfg.requestTimeout)
	if err := f.signal(syscall.SIGSTOP); err != nil {
		return err
	}
	cl := s.c.nodes[lead].one
	value := strings.Repeat("b", 32<<10)
	var wrote int64
	deadline := time.Now().Add(5 * time.Minute)
	for k := 0; wrote < s.cfg.burst; k++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("with node%d held, the burst wrote only %d MiB in 5 minutes", f.i+1, wrote>>20)
		}
		cctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout)
		_, err := cl.Put(cctx, fmt.Sprintf("/soak/burst/%06d", k%4096), value)
		cancel()
		if err == nil {
			wrote += int64(len(value))
		}
	}
	cctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout)
	_, _ = cl.Delete(cctx, "/soak/burst/", clientv3.WithPrefix())
	cancel()
	at := time.Now()
	if err := f.signal(syscall.SIGCONT); err != nil {
		return err
	}
	if !waitUntil(ctx, 2*time.Minute, func() bool { return strings.Contains(logSince(f, from), "fetching the snapshot") }) {
		return fmt.Errorf("node%d, held while %d MiB went in, did not fetch a snapshot within 2 minutes of its resume; its last log line: %q", f.i+1, wrote>>20, lastLines(f.log.Name(), 1))
	}
	s.rep.Snapshots++
	s.logf("node%d fetches a snapshot", f.i+1)
	return s.commit(ctx, "its resume", at, []int{f.i}, catchUp, &s.rep.CatchUp)
}

// hashes checks that the nodes agree on the hash of their keys at the
// current revision. A compaction that lands between two of the answers
// moves the revision the hash starts from, so then it asks all again.
func (s *soak) hashes(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout)
	resp, err := s.l.cl.Get(cctx, "/soak/probe")
	cancel()
	if err != nil {
		return fmt.Errorf("hash check: %w", err)
	}
	rev := resp.Header.Revision
	for try := 0; ; try++ {
		var hs [3]*clientv3.HashKVResponse
		for _, n := range s.c.nodes {
			deadline := time.Now().Add(time.Minute)
			for {
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				hs[n.i], err = n.one.HashKV(cctx, n.one.Endpoints()[0], rev)
				cancel()
				if err == nil || ctx.Err() != nil || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Second)
			}
			if err != nil {
				return fmt.Errorf("hash of node%d at revision %d: %w", n.i+1, rev, err)
			}
		}
		if hs[0].CompactRevision != hs[1].CompactRevision || hs[0].CompactRevision != hs[2].CompactRevision {
			if try == 9 {
				return fmt.Errorf("at revision %d the nodes hash after compactions %d, %d and %d, ten times in a row", rev, hs[0].CompactRevision, hs[1].CompactRevision, hs[2].CompactRevision)
			}
			time.Sleep(time.Second)
			continue
		}
		for i := 1; i < 3; i++ {
			if hs[i].Hash != hs[0].Hash {
				return &violation{fmt.Sprintf("at revision %d after compaction %d node1 hashes its keys to %08x, node%d to %08x", rev, hs[0].CompactRevision, hs[0].Hash, i+1, hs[i].Hash)}
			}
		}
		s.rep.Hashes++
		return nil
	}
}

// counters records each node's cleaning and cache counters from its
// metrics.
func (s *soak) counters(ctx context.Context) error {
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: s.c.tls}, Timeout: 10 * time.Second}
	for i, n := range s.c.nodes {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+s.c.endpoints[i]+"/metrics", nil)
		if err != nil {
			return err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return fmt.Errorf("metrics of node%d: %w", n.i+1, err)
		}
		b, err := io.ReadAll(resp.Body)
		err = errors.Join(err, resp.Body.Close())
		if err != nil {
			return fmt.Errorf("metrics of node%d: %w", n.i+1, err)
		}
		st := &s.rep.Nodes[i]
		for line := range strings.SplitSeq(string(b), "\n") {
			name, value, ok := strings.Cut(line, " ")
			if !ok {
				continue
			}
			var dst *int64
			switch name {
			case "tigerstore_cleaned_bytes_total":
				dst = &st.Cleaned
			case "tigerstore_cache_hits_total":
				dst = &st.CacheHits
			case "tigerstore_cache_misses_total":
				dst = &st.CacheMisses
			default:
				continue
			}
			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return fmt.Errorf("metrics of node%d: %s: %w", n.i+1, line, err)
			}
			*dst = int64(f)
		}
	}
	return nil
}

// compact compacts every interval to the revision of the interval before.
func (s *soak) compact(ctx context.Context) {
	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.compactEvery):
		}
		cur := s.l.acked.Load()
		if prev > 0 {
			cctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout)
			_, err := s.l.cl.Compact(cctx, prev)
			cancel()
			if err != nil {
				s.rep.CompactErrors++
				s.logf("compaction to revision %d: %v", prev, err)
			} else {
				s.rep.Compactions++
			}
		}
		prev = cur
	}
}

// sample records the nodes' memory and disk, and fails on an alarm.
func (s *soak) sample(ctx context.Context) {
	for {
		for i, n := range s.c.nodes {
			n.mu.Lock()
			pid := n.cmd.Process.Pid
			n.mu.Unlock()
			st := &s.rep.Nodes[i]
			st.RSS = max(st.RSS, rss(pid))
			st.Disk = max(st.Disk, dirSize(n.data))
			if s.cfg.diskBudget > 0 && st.Disk > s.cfg.diskBudget+(256<<20) {
				s.fail(&violation{fmt.Sprintf("node%d's data takes %d MiB, past its disk budget of %d MiB and the 256 MiB in flight", i+1, st.Disk>>20, s.cfg.diskBudget>>20)})
			}
		}
		cctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout)
		alarms, err := s.l.cl.AlarmList(cctx)
		cancel()
		if err == nil && len(alarms.Alarms) > 0 {
			s.fail(&violation{fmt.Sprintf("alarms went off: %v", alarms.Alarms)})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}

// finish settles the models, reads every object, and waits for the
// watches to reach the last revision, so they can be checked to the end.
func (s *soak) finish(ctx context.Context) error {
	s.logf("settling")
	var wg sync.WaitGroup
	for _, w := range s.l.workers {
		wg.Go(func() {
			if err := w.settle(ctx); err != nil {
				s.fail(fmt.Errorf("settle: %w", err))
			}
		})
	}
	wg.Wait()
	if err := s.failed(); err != nil {
		return err
	}
	rev := s.l.acked.Load()
	s.rep.Revision = rev
	if !waitUntil(ctx, time.Minute, func() bool { return s.ws.caughtUp(rev) }) {
		return fmt.Errorf("the watches did not reach revision %d within a minute: %s", rev, s.ws.reached())
	}
	return nil
}

func others(i int) []int {
	return slices.DeleteFunc([]int{0, 1, 2}, func(j int) bool { return j == i })
}

func pause(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func waitUntil(ctx context.Context, timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
	return true
}

// logSize is the size of n's log file now.
func logSize(n *node) int64 {
	st, err := n.log.Stat()
	if err != nil {
		return 0
	}
	return st.Size()
}

// logSince is what n's log file got from offset from on.
func logSince(n *node, from int64) string {
	f, err := os.Open(n.log.Name())
	if err != nil {
		return ""
	}
	var b []byte
	if _, err = f.Seek(from, io.SeekStart); err == nil {
		b, err = io.ReadAll(f)
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return ""
	}
	return string(b)
}
