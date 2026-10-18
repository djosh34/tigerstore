package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/chaos/env"
	"github.com/djosh34/tigerstore/chaos/proxy"
	"github.com/djosh34/tigerstore/history"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// The faults that need the environment of -env: each node on a disk of
// its own under LazyFS, in a cgroup of its own, and netem on the links.
const (
	netemLoss      = "netem-loss"
	netemReorder   = "netem-reorder"
	netemCorrupt   = "netem-corrupt"
	cpuGray        = "cpu-gray"
	diskSlow       = "disk-slow"
	diskStall      = "disk-stall"
	fsyncFail      = "fsync-fail"
	diskFull       = "disk-full"
	powerLoss      = "power-loss"
	tornWrite      = "torn-write"
	recoveryCrash  = "crash-in-recovery"
	installCrash   = "crash-in-install"
	slowWatcher    = "barely-alive-client"
	severalAtOnce  = "several-at-once"
	replace        = "replace-member"
	electionMillis = 1000
)

var envFaults = []string{
	netemLoss, netemReorder, netemCorrupt, cpuGray, diskSlow, diskStall, fsyncFail, diskFull,
	powerLoss, tornWrite, recoveryCrash, installCrash, slowWatcher, severalAtOnce, replace,
}

// LazyFS answers an fsync with success even when the disk under it
// failed the write or is full, so a node on it would be told that data is
// durable that is not. Its fsync also only writes to the disk under it
// and never syncs it, so a slow or stalled disk does not hold the node's
// fsync. Those faults run only without it, and the faults that need it
// only with it.
var (
	withoutLazyFS = []string{diskSlow, diskStall, fsyncFail, diskFull}
	withLazyFS    = []string{powerLoss, tornWrite}
)

// envFaultsFor are the faults of the environment a run can have.
func envFaultsFor(lazyfs bool) []string {
	var out []string
	for _, f := range envFaults {
		if lazyfs && slices.Contains(withoutLazyFS, f) || !lazyfs && slices.Contains(withLazyFS, f) {
			continue
		}
		out = append(out, f)
	}
	return out
}

// grayFaults leave two nodes healthy, so writes through them must keep
// committing within 10 election timeouts.
var grayFaults = []string{netemLoss, netemReorder, netemCorrupt, cpuGray, diskSlow, diskStall, fsyncFail, diskFull, powerLoss, tornWrite, recoveryCrash, slowWatcher}

// livenessBound is how long a write through a healthy node may take while
// one node is gray: 10 election timeouts.
const livenessBound = 10 * electionMillis * time.Millisecond

// nodeArgs are the arguments every node gets under -env: sizes small
// enough that a follower stopped for a few seconds falls behind the
// window and needs a snapshot, and checkpoints and segment removal come
// often.
var nodeArgs = []string{"--unsafe-window-bytes", "16384", "--unsafe-checkpoint-bytes", "65536", "--unsafe-segment-bytes", "32768"}

// environment is what -env adds to a run.
type environment struct {
	disks   [3]*env.Disk
	groups  [3]*env.Cgroup
	netem   *env.Netem
	latency map[string]time.Duration
	tag     string
}

// newEnvironment makes a disk under dir for each node, with LazyFS when
// lazyfs names its binary.
func newEnvironment(dir string, seed uint64, lazyfs string, size int64) (*environment, error) {
	e := &environment{tag: fmt.Sprintf("chaos-%d-%d", seed, os.Getpid())}
	for i := range 3 {
		d, err := env.NewDisk(filepath.Join(dir, "disks"), fmt.Sprintf("%s-n%d", e.tag, i+1), size, lazyfs)
		if err != nil {
			return nil, errors.Join(err, e.close())
		}
		e.disks[i] = d
		g, err := env.NewCgroup(fmt.Sprintf("%s-n%d", e.tag, i+1))
		if err != nil {
			return nil, errors.Join(err, e.close())
		}
		e.groups[i] = g
	}
	return e, nil
}

func (e *environment) data() [3]string {
	var out [3]string
	for i, d := range e.disks {
		out[i] = d.Data
	}
	return out
}

// network puts every link behind netem: peer links with a seeded delay
// of 1 to 10 ms each way, client links with none.
func (e *environment) network(c *cluster, seed uint64) error {
	rng := rand.New(rand.NewPCG(seed, 10))
	base := map[int]time.Duration{}
	e.latency = map[string]time.Duration{}
	for _, l := range c.links {
		port, err := linkPort(l)
		if err != nil {
			return err
		}
		d := time.Duration(0)
		if l.From >= 0 {
			d = time.Duration(1+rng.IntN(10)) * time.Millisecond
		}
		base[port] = d
		e.latency[l.Name] = d
	}
	n, err := env.NewNetem(base)
	if err != nil {
		return err
	}
	e.netem = n
	return nil
}

func linkPort(l *link) (int, error) {
	_, p, err := net.SplitHostPort(l.Proxy.Addr())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(p)
}

// close takes the environment apart; the nodes must be stopped.
func (e *environment) close() error {
	var errs []error
	if e.netem != nil {
		errs = append(errs, e.netem.Close())
	}
	for _, g := range e.groups {
		if g != nil {
			errs = append(errs, g.Close())
		}
	}
	for _, d := range e.disks {
		if d != nil {
			errs = append(errs, d.Close())
		}
	}
	return errors.Join(errs...)
}

// keep copies each node's files out of its disk into dir/data, as the
// evidence a run keeps.
func (e *environment) keep(dir string) error {
	var errs []error
	for i, d := range e.disks {
		to := filepath.Join(dir, "data", fmt.Sprintf("node%d", i+1))
		if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
			return err
		}
		if out, err := exec.Command("cp", "-a", d.Data, to).CombinedOutput(); err != nil {
			errs = append(errs, fmt.Errorf("copy %s: %w: %s", d.Data, err, out))
		}
	}
	return errors.Join(errs...)
}

// envRecord is the evidence of a fault of the environment.
type envRecord struct {
	Node      string           `json:"node,omitempty"`
	Link      string           `json:"link,omitempty"`
	Netem     *env.NetemStats  `json:"netem,omitempty"`
	CPU       *env.CgroupStats `json:"cpu,omitempty"`
	Disk      string           `json:"disk,omitempty"`
	FreeBytes int64            `json:"free_bytes,omitempty"`
	Filled    int64            `json:"filled_bytes,omitempty"`
	LazyFS    string           `json:"lazyfs,omitempty"`
	Exited    string           `json:"exited,omitempty"`
	LeaderNow string           `json:"leader_after,omitempty"`
	// Crashes are the kills during recovery, each with the time after
	// the start and whether it came inside the recovery of the log.
	Crashes []recoveryKill `json:"crashes,omitempty"`
	// Fetching is the node's log line of the fetch the crash cut.
	Fetching string `json:"fetching,omitempty"`
	// Probes are the writes through healthy nodes during the fault.
	Probes []probe `json:"probes,omitempty"`
	// Parts are the faults of a step with several at once.
	Parts []string `json:"parts,omitempty"`
}

type recoveryKill struct {
	After      time.Duration `json:"after"`
	InRecovery bool          `json:"in_recovery"`
}

// probe is one write through a healthy node during a gray fault.
type probe struct {
	Node    string        `json:"node"`
	Took    time.Duration `json:"took"`
	Error   string        `json:"error,omitempty"`
	StartAt time.Duration `json:"start_at"`
}

// probeClient is the history client of the prober on node i.
func probeClient(i int) int { return 40 + i }

// probes writes through the nodes in healthy, one every 500 ms, until
// stop. A client retries: a probe is one write, sent again after an
// error, as a new call, until one commits or livenessBound passed since
// the first.
func (r *runner) probes(healthy []int, stop <-chan struct{}, count *atomic.Int64) []probe {
	var out []probe
	n := 0
	for {
		select {
		case <-stop:
			return out
		default:
		}
		i := healthy[n%len(healthy)]
		n++
		start := r.since()
		q := history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sprobe%d", prefix, i), Value: fmt.Sprintf("probe-%d-%d", i, n)}}
		p := probe{Node: fmt.Sprintf("node%d", i+1), StartAt: start}
		for {
			left := livenessBound - (r.since() - start)
			if left <= 0 {
				break
			}
			op := r.t.r.call(r.t.cs[i], probeClient(i), q, left)
			p.Took, p.Error = r.since()-start, ""
			if op.Err == nil {
				break
			}
			p.Error = op.Err.Message
			time.Sleep(50 * time.Millisecond)
		}
		out = append(out, p)
		count.Add(1)
		select {
		case <-stop:
			return out
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// live checks the probes of a gray fault: at least three, every one
// answered within livenessBound.
func live(ps []probe) error {
	if len(ps) < 3 {
		return fmt.Errorf("only %d writes through healthy nodes during the fault", len(ps))
	}
	for _, p := range ps {
		if p.Error != "" || p.Took > livenessBound {
			return fmt.Errorf("a write through %s at %v took %v: %s", p.Node, p.StartAt, p.Took, p.Error)
		}
	}
	return nil
}

// withProbes runs inject while writes through the healthy nodes probe
// liveness, and checks them.
func (r *runner) withProbes(healthy []int, rec *record, inject func() error) error {
	stop := make(chan struct{})
	done := make(chan []probe, 1)
	var count atomic.Int64
	start := time.Now()
	go func() { done <- r.probes(healthy, stop, &count) }()
	err := inject()
	// Probe for 3 s and three probes at least, also after a short fault.
	waitUntil(context.Background(), 3*livenessBound, func() bool { return time.Since(start) >= 3*time.Second && count.Load() >= 3 })
	close(stop)
	ps := <-done
	if rec.Env == nil {
		rec.Env = &envRecord{}
	}
	rec.Env.Probes = ps
	return errors.Join(err, live(ps))
}

func others(i int) []int { return []int{(i + 1) % 3, (i + 2) % 3} }

// injectEnv runs a fault of the environment on node target, with lead
// the leader.
func (r *runner) injectEnv(ctx context.Context, s step, lead, target int, rec *record) error {
	e := r.e
	if e == nil {
		return fmt.Errorf("fault %s needs -env", s.Fault)
	}
	rec.Env = &envRecord{Node: fmt.Sprintf("node%d", target+1)}
	switch s.Fault {
	case netemLoss, netemReorder, netemCorrupt:
		l := r.c.peers[lead][target]
		if target == lead {
			l = r.c.peers[lead][others(lead)[0]]
		}
		return r.withProbes(others(l.To), rec, func() error { return r.netemFault(ctx, s, l, rec) })
	case cpuGray:
		return r.withProbes(others(target), rec, func() error { return r.cpuFault(ctx, s, target, rec) })
	case diskSlow, diskStall:
		return r.withProbes(others(target), rec, func() error { return r.diskFault(ctx, s, target, rec) })
	case fsyncFail, diskFull:
		return r.withProbes(others(target), rec, func() error { return r.diskError(ctx, s, target, rec) })
	case powerLoss:
		return r.withProbes(others(target), rec, func() error { return r.powerLoss(ctx, s, target, rec) })
	case tornWrite:
		return r.withProbes(others(target), rec, func() error { return r.tornWrite(ctx, s, target, rec) })
	case recoveryCrash:
		return r.withProbes(others(target), rec, func() error { return r.crashInRecovery(ctx, s, target, rec) })
	case installCrash:
		return r.crashInInstall(ctx, lead, target, rec)
	case slowWatcher:
		return r.withProbes([]int{0, 1, 2}, rec, func() error { return r.slowWatch(ctx, s, target, rec) })
	case severalAtOnce:
		return r.several(ctx, s, lead, target, rec)
	case replace:
		return r.replaceMember(ctx, s, target, rec)
	}
	return fmt.Errorf("unknown fault %q", s.Fault)
}

func (r *runner) netemFault(ctx context.Context, s step, l *link, rec *record) error {
	port, err := linkPort(l)
	if err != nil {
		return err
	}
	rec.Env.Link = l.Name
	impairment := map[string]string{netemLoss: "loss 20%", netemReorder: "reorder 50% 50%", netemCorrupt: "corrupt 5%"}[s.Fault]
	before, err := r.e.netem.Stats(port)
	if err != nil {
		return err
	}
	if err := r.e.netem.Impair(port, impairment); err != nil {
		return err
	}
	rec.Installed = true
	holdErr := pause(ctx, s.Hold)
	after, statErr := r.e.netem.Stats(port)
	healErr := r.e.netem.Heal(port)
	rec.Healed, rec.HealedAt = healErr == nil, r.since()
	rec.Env.Netem = &after
	rec.Effect = statErr == nil && after.Packets > before.Packets && (s.Fault != netemLoss || after.Dropped > before.Dropped)
	return errors.Join(holdErr, statErr, healErr)
}

func (r *runner) cpuFault(ctx context.Context, s step, target int, rec *record) error {
	g := r.e.groups[target]
	before, err := g.Stats()
	if err != nil {
		return err
	}
	if err := g.LimitCPU(s.Rate); err != nil {
		return err
	}
	rec.Installed = true
	// Reads through the node keep it busy, so that a follower with little
	// to do still uses up its share. They are serializable, so that each
	// is work of the node alone, not a wait for the leader across slow
	// links, which left a node at 4% unthrottled (#252). Their answers do
	// not matter.
	bctx, stop := context.WithCancel(ctx)
	busy := make(chan struct{})
	go func() {
		defer close(busy)
		for bctx.Err() == nil {
			rctx, cancel := context.WithTimeout(bctx, time.Second)
			_, _ = r.t.cs[target].Get(rctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithSerializable())
			cancel()
		}
	}()
	holdErr := pause(ctx, s.Hold)
	stop()
	<-busy
	after, statErr := g.Stats()
	healErr := g.LimitCPU(0)
	rec.Healed, rec.HealedAt = healErr == nil, r.since()
	rec.Env.CPU = &after
	rec.Effect = statErr == nil && after.Throttled > before.Throttled
	return errors.Join(holdErr, statErr, healErr)
}

// diskFault slows or stalls the disk of target. A stall of the leader's
// disk must move leadership away within the hold.
func (r *runner) diskFault(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	logBefore := logSize(n)
	var err error
	if s.Fault == diskSlow {
		err = d.Delay(s.Delay/4, s.Delay)
	} else {
		err = d.Stall()
	}
	if err != nil {
		return err
	}
	rec.Installed = true
	var statErr error
	if rec.Env.Disk, _, statErr = d.Status(); statErr != nil {
		return errors.Join(statErr, d.Healthy())
	}
	holdErr := pause(ctx, s.Hold)
	if s.Fault == diskStall {
		// Whether a write was held: the node logs a slow write once the
		// stall ends, or, as leader, hands over.
		if lead, err := r.c.leader(ctx, 2*time.Second); err == nil {
			rec.Env.LeaderNow = fmt.Sprintf("node%d", lead+1)
		}
	}
	healErr := d.Healthy()
	rec.Healed, rec.HealedAt = healErr == nil, r.since()
	if s.Fault == diskStall {
		// The held writes finish once it heals.
		time.Sleep(time.Second)
	}
	rec.Effect = strings.Contains(logSince(n, logBefore), "slow write to the log")
	return errors.Join(holdErr, healErr)
}

// diskError fails the writes of target's disk, or fills it, and expects
// the node to stop or refuse; it heals the disk and starts the node again
// if it stopped.
func (r *runner) diskError(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	n.mu.Lock()
	n.expecting = true
	n.mu.Unlock()
	logBefore := logSize(n)
	var err error
	if s.Fault == fsyncFail {
		err = d.FailWrites()
	} else {
		rec.Env.Filled, err = d.Fill()
	}
	if err != nil {
		return err
	}
	rec.Installed = true
	var statErr error
	rec.Env.Disk, rec.Env.FreeBytes, statErr = d.Status()
	holdErr := errors.Join(statErr, pause(ctx, s.Hold))
	var healErr error
	if s.Fault == fsyncFail {
		healErr = d.Healthy()
	} else {
		healErr = d.Unfill()
	}
	n.mu.Lock()
	exited, ps := n.exitedNow, n.cmd.ProcessState
	n.mu.Unlock()
	if exited {
		rec.Env.Exited = ps.String()
	} else {
		// A node that kept running stops now: it may hold a failed
		// write in memory that it could not sync.
		_, _, err := n.signal(syscall.SIGKILL)
		healErr = errors.Join(healErr, err)
	}
	if s.Fault == fsyncFail {
		healErr = errors.Join(healErr, d.Repair())
	}
	if err := r.c.start(n); err != nil {
		healErr = errors.Join(healErr, err)
	}
	rec.Healed, rec.HealedAt = healErr == nil, r.since()
	text := logSince(n, logBefore)
	rec.Effect = strings.Contains(text, "input/output error") || strings.Contains(text, "read-only file system") ||
		strings.Contains(text, "no space left on device") || strings.Contains(text, "database space exceeded")
	return errors.Join(holdErr, healErr)
}

func logSize(n *node) int64 {
	st, err := os.Stat(n.log.Name())
	if err != nil {
		return 0
	}
	return st.Size()
}

func logSince(n *node, from int64) string {
	b, err := os.ReadFile(n.log.Name())
	if err != nil || int64(len(b)) < from {
		return ""
	}
	return string(b[from:])
}

// powerLoss cuts target's power: SIGKILL and every write no fsync
// covered gone, then a start after the hold.
func (r *runner) powerLoss(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	ps, _, err := n.signal(syscall.SIGKILL)
	if err != nil {
		return err
	}
	rec.Installed = true
	rec.Env.Exited = ps.String()
	report, err := d.PowerLoss()
	rec.Env.LazyFS = strings.TrimSpace(report)
	rec.Effect = err == nil && strings.Contains(report, "cache is cleared")
	holdErr := pause(ctx, s.Hold)
	startErr := r.c.start(n)
	rec.Healed, rec.HealedAt = startErr == nil, r.since()
	return errors.Join(err, holdErr, startErr)
}

// tornWrite tears the next write to target's newest log segment: LazyFS
// keeps its first third and crashes, as the power would cut mid-write.
// The node is killed, the disk mounted again, and the node started.
func (r *runner) tornWrite(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	seg, err := newestSegment(d.Data)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.expecting = true
	n.mu.Unlock()
	if err := d.TornWrite(seg); err != nil {
		return err
	}
	rec.Installed = true
	crashed := waitUntil(ctx, s.Hold, d.Crashed)
	rec.Effect = crashed
	rec.Env.Disk = filepath.Base(seg)
	if _, _, err := n.signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	healErr := d.Remount()
	if healErr == nil {
		healErr = r.c.start(n)
	}
	rec.Healed, rec.HealedAt = healErr == nil, r.since()
	return healErr
}

// newestSegment is the log segment a node writes to.
func newestSegment(data string) (string, error) {
	segs, err := segments(data)
	if err != nil {
		return "", err
	}
	return segs[len(segs)-1], nil
}

// segments are the log segments under data, oldest first.
func segments(data string) ([]string, error) {
	var segs []string
	err := filepath.WalkDir(data, func(path string, de os.DirEntry, err error) error {
		if err == nil && !de.IsDir() && strings.HasSuffix(path, ".wal") {
			segs = append(segs, path)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("no log segment under %s", data)
	}
	slices.Sort(segs)
	return segs, nil
}

func waitUntil(ctx context.Context, d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// crashInRecovery kills target, then five times drops its caches,
// starts it and kills it again 5 to 100 ms after it logged that it is
// recovering its log, from a disk slowed to 50 ms a read, and starts it
// for good on a healthy disk. At least one kill must come before the node
// logged that it recovered the log.
func (r *runner) crashInRecovery(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	if _, _, err := n.signal(syscall.SIGKILL); err != nil {
		return err
	}
	if err := d.Delay(50*time.Millisecond, 20*time.Millisecond); err != nil {
		return err
	}
	rec.Installed = true
	rng := rand.New(rand.NewPCG(s.Pick, 11))
	for range 5 {
		if err := d.DropCaches(); err != nil {
			return errors.Join(err, d.Healthy())
		}
		from := logSize(n)
		if err := r.c.start(n); err != nil {
			return errors.Join(err, d.Healthy())
		}
		after := time.Duration(5+rng.IntN(95)) * time.Millisecond
		waitUntil(ctx, 10*time.Second, func() bool { return strings.Contains(logSince(n, from), "recovering the log") })
		time.Sleep(after)
		if _, _, err := n.signal(syscall.SIGKILL); err != nil {
			return err
		}
		text := logSince(n, from)
		in := strings.Contains(text, "recovering the log") && !strings.Contains(text, "recovered the log")
		rec.Env.Crashes = append(rec.Env.Crashes, recoveryKill{After: after, InRecovery: in})
		rec.Effect = rec.Effect || in
	}
	if err := d.Healthy(); err != nil {
		return err
	}
	holdErr := pause(ctx, s.Gap)
	err := r.c.start(n)
	rec.Healed, rec.HealedAt = err == nil, r.since()
	return errors.Join(holdErr, err)
}

// crashInInstall stops follower target, writes until the leader's log
// moved past it, resumes it so that it fetches a snapshot, and kills it
// once the fetch started; it then starts it again, which must recover.
// The fetch is a connection of the follower's own, so the replies on its
// links to the others come at 64 KiB/s from before it resumes until the
// kill, which comes once 16 KiB of the stream arrived: a small snapshot
// would otherwise be fetched and installed before the kill (#223). The
// writes go to keys of their own, all live, so the snapshot holds their
// 300 KB. Its Raft messages go out on the same links at full speed, and
// those it gets come on the others' links. A follower that caught up from
// the leader's log instead goes through it again, up to three times
// (#246).
func (r *runner) crashInInstall(ctx context.Context, lead, target int, rec *record) error {
	if target == lead {
		target = others(lead)[0]
	}
	rec.Env.Node = fmt.Sprintf("node%d", target+1)
	n := r.c.nodes[target]
	var held []*link
	for _, j := range others(target) {
		held = append(held, r.c.peers[target][j])
	}
	slow := proxy.Fault{Kind: proxy.Rate, Dir: proxy.Backward, Rate: 64 << 10}
	// release heals the links slowed so far, which rec.Links records.
	release := func() error {
		var errs []error
		for i := range rec.Links {
			l := held[i]
			rec.Links[i].BeforeHeal = l.Proxy.Stats()
			st, err := l.Proxy.Set(proxy.Fault{Kind: proxy.None})
			rec.Links[i].Healed = st
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}
	// received is how much the slowed links delivered to the follower
	// since then.
	var base uint64
	received := func() uint64 {
		var n uint64
		for _, l := range held {
			n += l.Proxy.Stats().BackwardSent
		}
		return n - base
	}
	cl := r.t.cs[lead]
	fetching := false
	var logBefore int64
	for attempt := 0; attempt < 3 && !fetching; attempt++ {
		if _, _, err := n.signal(syscall.SIGSTOP); err != nil {
			return errors.Join(err, release())
		}
		// Writes through the leader far past the window and a checkpoint,
		// so that the leader sends the follower a snapshot.
		for k := range 300 {
			q := history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sinstall%d", prefix, k), Value: fmt.Sprintf("bulk-%d-%d-%s", attempt, k, strings.Repeat("b", 1000))}}
			r.t.r.call(cl, installClient, q, r.t.cfg.requestTimeout)
		}
		rec.Links = rec.Links[:0]
		for _, l := range held {
			st, err := l.Proxy.Set(slow)
			rec.Links = append(rec.Links, linkRecord{Link: l.Name, Requested: slow, Installed: st})
			if err != nil {
				return errors.Join(err, release())
			}
		}
		base = received()
		logBefore = logSize(n)
		if _, _, err := n.signal(syscall.SIGCONT); err != nil {
			return errors.Join(err, release())
		}
		rec.Installed = true
		// The fetch started, and part of its stream came.
		fetching = waitUntil(ctx, 10*time.Second, func() bool {
			return strings.Contains(logSince(n, logBefore), "fetching the snapshot") && received() >= 16<<10
		})
		if !fetching {
			if err := release(); err != nil {
				return err
			}
		}
	}
	if _, _, err := n.signal(syscall.SIGKILL); err != nil {
		return errors.Join(err, release())
	}
	if err := release(); err != nil {
		return err
	}
	text := logSince(n, logBefore)
	if i := strings.Index(text, "fetching the snapshot"); i >= 0 {
		rec.Env.Fetching = strings.SplitN(text[i:], "\n", 2)[0]
	}
	// The crash must cut the fetch: no installed line before the kill.
	rec.Effect = fetching && !strings.Contains(text, "installed the snapshot")
	err := r.c.start(n)
	rec.Healed, rec.HealedAt = err == nil, r.since()
	return err
}

// installClient is the history client of crashInInstall's writes.
const installClient = 50

// stall holds the reads of every connection of the slow client once on,
// until released: the TCP window of a client that is barely alive.
type stall struct {
	on      atomic.Bool
	release chan struct{}
}

// stallConn is one connection of the slow client.
type stallConn struct {
	net.Conn
	s *stall
}

func (c *stallConn) Read(b []byte) (int, error) {
	if c.s.on.Load() {
		<-c.s.release
	}
	return c.Conn.Read(b)
}

// slowWatch opens a watch with previous values on target's client link,
// then stops reading from its connection for the hold, while the
// workload writes, so the node's sends to it back up. The node must keep
// serving everyone else (the probes go through every node) and, once the
// client reads again, deliver every event the watch missed, up to the
// revision of a read through another node, or cancel it. A watch through
// that node from the same start must have the same events.
func (r *runner) slowWatch(ctx context.Context, s step, target int, rec *record) error {
	st := &stall{release: make(chan struct{})}
	cl, err := clientv3.New(clientv3.Config{
		Endpoints: []string{r.c.clients[target].Proxy.Addr()}, TLS: r.c.tls, DialTimeout: 2 * time.Second, Logger: zap.NewNop(),
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			return &stallConn{Conn: c, s: st}, nil
		})},
	})
	if err != nil {
		return err
	}
	defer cl.Close() //nolint:errcheck // the slow client goes at the end of the step
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := cl.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	var start int64
	select {
	case resp := <-wc:
		if !resp.Created {
			return fmt.Errorf("the slow client's watch answered %+v before it was created", resp)
		}
		start = resp.Header.Revision
	case <-time.After(5 * time.Second):
		return errors.New("the slow client's watch was not created")
	}
	st.on.Store(true)
	rec.Installed = true
	from := r.since()
	holdErr := pause(ctx, s.Hold)
	close(st.release)
	gctx, gcancel := context.WithTimeout(ctx, r.t.cfg.requestTimeout)
	g, err := r.t.cs[others(target)[0]].Get(gctx, prefix+"k0")
	gcancel()
	if err != nil {
		return errors.Join(holdErr, fmt.Errorf("read the revision the slow watch must reach: %w", err))
	}
	end := g.Header.Revision
	got, last, canceled := readWatch(wc, end)
	if !canceled && last < end {
		return errors.Join(holdErr, fmt.Errorf("the slow watch from %d reached %d, not %d, within 10s of its release", start, last, end))
	}
	ref, refLast, _ := readWatch(r.t.cs[others(target)[0]].Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(start+1)), last)
	if refLast < last {
		return errors.Join(holdErr, fmt.Errorf("a watch through another node from %d reached %d, not %d, within 10s", start+1, refLast, last))
	}
	if err := sameEvents(got, ref, last); err != nil {
		return errors.Join(holdErr, fmt.Errorf("the slow watch from %d: %w", start, err))
	}
	rec.Env.Exited = fmt.Sprintf("after %v stalled from %v: revisions %d to %d of %d, canceled %v", s.Hold, from, start+1, last, end, canceled)
	rec.Effect = true
	rec.Healed, rec.HealedAt = true, r.since()
	return holdErr
}

// watchEvent is what a watch delivered for one key at one revision.
type watchEvent struct {
	Rev  int64
	Key  string
	Type mvccpb.Event_EventType
}

// readWatch reads wc until an event at or past rev, a cancel, or 10 s,
// and returns the events, the last revision among them and whether the
// watch was canceled.
func readWatch(wc clientv3.WatchChan, rev int64) (events []watchEvent, last int64, canceled bool) {
	for deadline := time.After(10 * time.Second); last < rev; {
		select {
		case resp, ok := <-wc:
			if !ok || resp.Canceled {
				return events, last, true
			}
			for _, ev := range resp.Events {
				events = append(events, watchEvent{ev.Kv.ModRevision, string(ev.Kv.Key), ev.Type})
				last = max(last, ev.Kv.ModRevision)
			}
		case <-deadline:
			return events, last, false
		}
	}
	return events, last, false
}

// sameEvents compares got with the events of ref up to revision last.
func sameEvents(got, ref []watchEvent, last int64) error {
	ref = slices.DeleteFunc(slices.Clone(ref), func(e watchEvent) bool { return e.Rev > last })
	for i := range max(len(got), len(ref)) {
		switch {
		case i >= len(got):
			return fmt.Errorf("missed %d events, the first %+v", len(ref)-i, ref[i])
		case i >= len(ref):
			return fmt.Errorf("got %d events no other watch got, the first %+v", len(got)-i, got[i])
		case got[i] != ref[i]:
			return fmt.Errorf("got %+v where another watch got %+v", got[i], ref[i])
		}
	}
	return nil
}

// several runs two faults at once on different nodes: a gray node and a
// lossy link to another.
func (r *runner) several(ctx context.Context, s step, lead, target int, rec *record) error {
	a := target
	b := others(a)[s.Pick%2]
	rec.Env.Parts = []string{fmt.Sprintf("%s on node%d", cpuGray, a+1), fmt.Sprintf("%s on node%d-node%d", netemLoss, lead+1, b+1)}
	recA, recB := &record{Env: &envRecord{}}, &record{Env: &envRecord{}}
	sa, sb := s, s
	sa.Fault, sb.Fault = cpuGray, netemLoss
	errs := make(chan error, 2)
	go func() { errs <- r.cpuFault(ctx, sa, a, recA) }()
	l := r.c.peers[lead][b]
	if b == lead {
		l = r.c.peers[lead][a]
	}
	go func() { errs <- r.netemFault(ctx, sb, l, recB) }()
	err := errors.Join(<-errs, <-errs)
	rec.Installed = recA.Installed && recB.Installed
	rec.Effect = recA.Effect && recB.Effect
	rec.Healed, rec.HealedAt = recA.Healed && recB.Healed, r.since()
	rec.Env.CPU, rec.Env.Netem = recA.Env.CPU, recB.Env.Netem
	return err
}

// archive copies every file of each node's log, but spares, into
// dir/wal-archive, every 100 ms until ctx ends and once more after: with
// the small sizes of -env, segments go below each checkpoint, and the
// checkers need the log from its first entry. A copy is refreshed while
// its file changes, and kept after the file is gone.
func (e *environment) archive(ctx context.Context, dir string) <-chan error {
	done := make(chan error, 1)
	go func() {
		seen := map[string]time.Time{}
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- e.archiveOnce(dir, seen)
				return
			case <-tick.C:
				if err := e.archiveOnce(dir, seen); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	return done
}

func (e *environment) archiveOnce(dir string, seen map[string]time.Time) error {
	for i, d := range e.disks {
		src := filepath.Join(d.Data, "wal")
		to := filepath.Join(dir, "wal-archive", fmt.Sprintf("node%d", i+1), "wal")
		if err := os.MkdirAll(to, 0o755); err != nil {
			return err
		}
		ents, err := os.ReadDir(src)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			// A disk being repaired or remounted is skipped this time.
			continue
		}
		for _, ent := range ents {
			if strings.HasPrefix(ent.Name(), "spare-") || ent.IsDir() {
				continue
			}
			info, err := ent.Info()
			if err != nil {
				continue
			}
			key := filepath.Join(to, ent.Name())
			if t, ok := seen[key]; ok && !info.ModTime().After(t) {
				continue
			}
			b, err := os.ReadFile(filepath.Join(src, ent.Name()))
			if err != nil {
				continue
			}
			if err := os.WriteFile(key, b, 0o644); err != nil {
				return err
			}
			seen[key] = info.ModTime()
		}
	}
	return nil
}

// merged builds, for node i, the log the checkers read: the node's own
// files as kept in dir/data, and from the archive the segments below its
// oldest one, which it removed below a checkpoint. Archived copies of
// newer segments may be of files that recovery cut or replaced, as after
// a failed write.
func merged(dir string, i int) (string, error) {
	own := filepath.Join(dir, "data", fmt.Sprintf("node%d", i+1), "wal")
	arch := filepath.Join(dir, "wal-archive", fmt.Sprintf("node%d", i+1), "wal")
	to := filepath.Join(dir, "wal-merged", fmt.Sprintf("node%d", i+1), "wal")
	if err := os.MkdirAll(to, 0o755); err != nil {
		return "", err
	}
	ents, err := os.ReadDir(own)
	if err != nil {
		return "", err
	}
	oldest := ""
	copyFile := func(from, name string) error {
		b, err := os.ReadFile(filepath.Join(from, name))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, name), b, 0o644)
	}
	for _, e := range ents {
		if e.IsDir() || strings.HasPrefix(e.Name(), "spare-") {
			continue
		}
		if strings.HasSuffix(e.Name(), ".wal") && (oldest == "" || e.Name() < oldest) {
			oldest = e.Name()
		}
		if err := copyFile(own, e.Name()); err != nil {
			return "", err
		}
	}
	archived, err := os.ReadDir(arch)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	for _, e := range archived {
		if strings.HasSuffix(e.Name(), ".wal") && oldest != "" && e.Name() < oldest {
			if err := copyFile(arch, e.Name()); err != nil {
				return "", err
			}
		}
	}
	return filepath.Dir(to), nil
}

// mayHaveApplied reports whether a failed member change may still apply.
// clientv3 returns a call that ran out of time as the plain context error,
// which carries no gRPC code, so transientWatch alone misses it.
func mayHaveApplied(err error) bool {
	return transientWatch(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// replaceMember is the safe way back for a member whose disk was erased
// or whose log a restart refuses (#161). Erased: every node loses power
// at once, the target's disk is erased, and the other two start again
// from their disks, which must elect a leader and commit writes within
// livenessBound. Fenced: the target's oldest segment gets a corrupt byte
// while it is down, and its restart must refuse the log. Then, as an
// operator would, the target is removed from the cluster through a
// healthy node and added again as a new member, which starts on an empty
// disk with --initial-cluster-state existing; the cluster must converge.
func (r *runner) replaceMember(ctx context.Context, s step, target int, rec *record) error {
	n, d := r.c.nodes[target], r.e.disks[target]
	healthy := others(target)
	erased := s.Pick%2 == 0
	if erased {
		rec.Env.Parts = []string{"erased"}
		for _, m := range r.c.nodes {
			if _, _, err := m.signal(syscall.SIGKILL); err != nil {
				return err
			}
		}
		for _, disk := range r.e.disks {
			if err := disk.DropCaches(); err != nil {
				return err
			}
		}
		if err := d.Erase(); err != nil {
			return err
		}
		for _, i := range healthy {
			if err := r.c.start(r.c.nodes[i]); err != nil {
				return err
			}
		}
		rec.Installed = true
		if err := r.withProbes(healthy, rec, func() error {
			_, err := r.c.leader(ctx, livenessBound)
			return err
		}); err != nil {
			return fmt.Errorf("the two nodes left after the erase: %w", err)
		}
	} else {
		rec.Env.Parts = []string{"fenced"}
		if _, _, err := n.signal(syscall.SIGKILL); err != nil {
			return err
		}
		segs, err := segments(d.Data)
		if err != nil {
			return err
		}
		seg := segs[0]
		if err := flipByte(seg); err != nil {
			return err
		}
		rec.Env.Disk = filepath.Base(seg)
		rec.Installed = true
		from := logSize(n)
		if err := r.c.startExpecting(n, true); err != nil {
			return err
		}
		refused := waitUntil(ctx, 10*time.Second, func() bool {
			n.mu.Lock()
			defer n.mu.Unlock()
			return n.exitedNow
		})
		if !refused {
			return errors.New("the node started on a corrupt log")
		}
		rec.Env.Exited = strings.TrimSpace(lastLine(logSince(n, from), "corrupt"))
	}
	rec.Effect = true
	// Remove and add through a healthy node. --strict-reconfig-check
	// refuses a change until the node has been connected to its peers
	// for 5 s, which after the cold restart it has not, so each is tried
	// again until it goes through. A try that timed out may have gone
	// through, so a later one may find the member gone, or added.
	ctl := r.c.nodes[healthy[0]].control
	retry := func(what string, call func(c context.Context, ambiguous bool) error) error {
		var err error
		ambiguous := false
		for deadline := time.Now().Add(2 * livenessBound); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = call(cctx, ambiguous)
			cancel()
			if err == nil {
				return nil
			}
			ambiguous = ambiguous || mayHaveApplied(err)
		}
		return fmt.Errorf("%s: %w", what, err)
	}
	old := r.c.ids[target]
	if err := retry(fmt.Sprintf("remove member %x", old), func(c context.Context, ambiguous bool) error {
		_, err := ctl.MemberRemove(c, old)
		if ambiguous && errors.Is(err, rpctypes.ErrMemberNotFound) {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	peerURL := fmt.Sprintf("https://127.0.0.1:%d", r.c.peerPorts[target])
	var newID uint64
	if err := retry("add the new member", func(c context.Context, ambiguous bool) error {
		added, err := ctl.MemberAdd(c, []string{peerURL})
		if err == nil {
			newID = added.Member.ID
			return nil
		}
		if !ambiguous || !errors.Is(err, rpctypes.ErrPeerURLExist) {
			return err
		}
		list, err := ctl.MemberList(c)
		if err != nil {
			return err
		}
		for _, m := range list.Members {
			if slices.Equal(m.PeerURLs, []string{peerURL}) {
				newID = m.ID
				return nil
			}
		}
		return fmt.Errorf("no member has the peer URL %s", peerURL)
	}); err != nil {
		return err
	}
	if err := d.Erase(); err != nil {
		return err
	}
	n.args = rejoinArgs(n.args)
	if err := r.c.start(n); err != nil {
		return err
	}
	r.c.ids[target] = newID
	rec.Env.Link = fmt.Sprintf("member %x replaced by %x", old, newID)
	err := r.c.converged(ctx, 30*time.Second)
	rec.Healed, rec.HealedAt = err == nil, r.since()
	return err
}

// rejoinArgs are a node's arguments to join the cluster as a new member.
func rejoinArgs(args []string) []string {
	out := slices.Clone(args)
	if !slices.Contains(out, "--initial-cluster-state") {
		out = append(out, "--initial-cluster-state", "existing")
	}
	return out
}

func lastLine(text, with string) string {
	for _, line := range slices.Backward(strings.Split(text, "\n")) {
		if strings.Contains(line, with) {
			return line
		}
	}
	return ""
}

// flipByte flips one bit in the middle of a file and syncs it.
func flipByte(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return errors.Join(err, f.Close())
	}
	b := make([]byte, 1)
	at := st.Size() / 2
	if _, err := f.ReadAt(b, at); err != nil {
		return errors.Join(err, f.Close())
	}
	b[0] ^= 0x10
	if _, err := f.WriteAt(b, at); err != nil {
		return errors.Join(err, f.Close())
	}
	return errors.Join(f.Sync(), f.Close())
}
