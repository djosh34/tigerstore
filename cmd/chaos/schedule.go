package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
	"github.com/djosh34/tigerstore/history"
)

// The faults the schedule injects. Every run injects each once, in a
// seeded order.
const (
	killLeader   = "sigkill-leader"
	killFollower = "sigkill-follower"
	stopLeader   = "sigstop-leader"
	stopFollower = "sigstop-follower"
	netDelay     = "delay"
	netSwitching = "switching-delay"
	netStall     = "stall"
	netDrop      = "drop"
	netCorrupt   = "corrupt"
	netDuplicate = "duplicate"
	netReset     = "reset"
	netCut       = "one-direction-cut"
	netSlow      = "slow-link"
)

var allFaults = []string{
	killLeader, killFollower, stopLeader, stopFollower,
	netDelay, netSwitching, netStall, netDrop, netCorrupt, netDuplicate, netReset, netCut, netSlow,
}

// step is one planned fault. Pick chooses among the nodes or links that
// fit when the step runs, as the leader is only known then.
type step struct {
	Fault  string        `json:"fault"`
	Hold   time.Duration `json:"hold"`
	Gap    time.Duration `json:"gap"`
	Pick   uint64        `json:"pick"`
	Dir    proxy.Dir     `json:"dir,omitempty"`
	Delay  time.Duration `json:"delay,omitempty"`
	Delay2 time.Duration `json:"delay2,omitempty"`
	Every  time.Duration `json:"every,omitempty"`
	Rate   int           `json:"rate,omitempty"`
}

// plan makes the seeded schedule for faults.
func plan(seed uint64, faults []string) []step {
	rng := rand.New(rand.NewPCG(seed, 49))
	order := append([]string(nil), faults...)
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	// A replaced member's peers reach it at its own port, past the
	// proxies, so the replacement comes last.
	if k := slices.Index(order, replace); k >= 0 {
		order = append(slices.Delete(order, k, k+1), replace)
	}
	ms := func(lo, hi int) time.Duration { return time.Duration(lo+rng.IntN(hi-lo+1)) * time.Millisecond }
	var out []step
	for _, f := range order {
		s := step{Fault: f, Hold: ms(1500, 3500), Gap: ms(1000, 2000), Pick: rng.Uint64()}
		s.Dir = []proxy.Dir{proxy.Forward, proxy.Backward, proxy.Both}[rng.IntN(3)]
		switch f {
		case netDelay:
			s.Delay = ms(300, 800)
		case netSwitching:
			s.Delay, s.Delay2, s.Every = ms(0, 2), ms(20, 80), ms(100, 400)
		case netSlow:
			s.Rate = 8<<10 + rng.IntN(24<<10)
		case cpuGray, severalAtOnce:
			// A node at 2 to 5 percent of one CPU, which throttles a
			// follower too.
			s.Rate = 2 + rng.IntN(4)
		case diskSlow:
			s.Delay = ms(100, 400)
		}
		if slices.Contains(grayFaults, f) || f == severalAtOnce {
			// Long enough for writes through the healthy nodes to show
			// the liveness bound, and for both faults of several-at-once
			// to show.
			s.Hold = ms(5000, 8000)
		}
		out = append(out, s)
	}
	return out
}

// linkRecord is the evidence for one proxy of a step.
type linkRecord struct {
	Link       string      `json:"link"`
	Requested  proxy.Fault `json:"requested"`
	Installed  proxy.Stats `json:"installed"`
	BeforeHeal proxy.Stats `json:"before_heal"`
	Healed     proxy.Stats `json:"healed"`
}

// processRecord is the evidence for a signal step.
type processRecord struct {
	Node        string `json:"node"`
	Pid         int    `json:"pid"`
	Signal      string `json:"signal"`
	WaitStatus  string `json:"wait_status,omitempty"`
	StateAfter  string `json:"state_after,omitempty"`
	ResumedPid  int    `json:"resumed_pid,omitempty"`
	StateHealed string `json:"state_healed,omitempty"`
}

// record is what happened to one step: requested, installed, took effect
// and healed, with times from the start of the run.
type record struct {
	Step      int            `json:"step"`
	Planned   step           `json:"planned"`
	Leader    string         `json:"leader,omitempty"`
	Requested time.Duration  `json:"requested_at"`
	Installed bool           `json:"installed"`
	Effect    bool           `json:"effect"`
	Healed    bool           `json:"healed"`
	HealedAt  time.Duration  `json:"healed_at,omitempty"`
	Links     []linkRecord   `json:"links,omitempty"`
	Process   *processRecord `json:"process,omitempty"`
	Env       *envRecord     `json:"env,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// runner injects a schedule into a cluster. t is the workload, which
// the faults of the environment e also write through.
type runner struct {
	c     *cluster
	t     *traffic
	e     *environment
	start time.Time
	logf  func(string, ...any)
}

func (r *runner) since() time.Duration { return time.Since(r.start) }

// hold waits out a fault's hold d with a write through node i in it, so
// that a write overlaps the fault however long the workload's own clients
// wait elsewhere, as for a leader change around it (#253).
func (r *runner) hold(ctx context.Context, d time.Duration, i int) error {
	start := time.Now()
	q := history.Request{Put: &history.PutRequest{Key: prefix + "hold", Value: fmt.Sprintf("hold-%d", r.since())}}
	r.t.r.call(r.t.cs[i], holdClient, q, d)
	return pause(ctx, d-time.Since(start))
}

// holdClient is the history client of hold's writes.
const holdClient = 51

// pause waits d or until ctx ends.
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run injects each step and heals it before the next. A step whose fault
// was not installed, did not take effect or did not heal is an error.
func (r *runner) run(ctx context.Context, steps []step) ([]record, error) {
	var out []record
	var errs []error
	for i, s := range steps {
		rec := record{Step: i, Planned: s}
		err := r.inject(ctx, s, &rec)
		if err == nil && !rec.Installed {
			err = errors.New("fault was not installed")
		}
		if err == nil && !rec.Effect {
			err = errors.New("fault was installed but had no effect")
		}
		if err == nil && !rec.Healed {
			err = errors.New("fault did not heal")
		}
		if err != nil {
			rec.Error = err.Error()
			errs = append(errs, fmt.Errorf("step %d (%s): %w", i, s.Fault, err))
		}
		r.logf("step %d %s leader=%s installed=%v effect=%v healed=%v %s", i, s.Fault, rec.Leader, rec.Installed, rec.Effect, rec.Healed, rec.Error)
		out = append(out, rec)
		if err := pause(ctx, s.Gap); err != nil {
			return out, errors.Join(append(errs, err)...)
		}
	}
	return out, errors.Join(errs...)
}

func (r *runner) inject(ctx context.Context, s step, rec *record) error {
	lead, err := r.c.leader(ctx, 15*time.Second)
	if err != nil {
		return err
	}
	rec.Leader = fmt.Sprintf("node%d", lead+1)
	followers := []int{(lead + 1) % 3, (lead + 2) % 3}
	follower := followers[s.Pick%2]
	rec.Requested = r.since()
	if slices.Contains(envFaults, s.Fault) {
		// The leader in a third of the steps, as a gray leader must be
		// fenced too.
		target := follower
		if s.Pick%3 == 0 {
			target = lead
		}
		return r.injectEnv(ctx, s, lead, target, rec)
	}
	switch s.Fault {
	case killLeader, killFollower:
		target := lead
		if s.Fault == killFollower {
			target = follower
		}
		return r.kill(ctx, s, target, rec)
	case stopLeader, stopFollower:
		target := lead
		if s.Fault == stopFollower {
			target = follower
		}
		return r.pauseProcess(ctx, s, target, rec)
	}
	// Links that carry traffic: every client link, and every peer link
	// to or from the leader. Followers do not talk to each other.
	var busy []*link
	busy = append(busy, r.c.clients[:]...)
	for _, f := range followers {
		busy = append(busy, r.c.peers[lead][f], r.c.peers[f][lead])
	}
	one := busy[s.Pick%uint64(len(busy))]
	dir := func(l *link) proxy.Dir {
		if l.From >= 0 {
			// Peer links send one way; replies use the other node's link.
			return proxy.Forward
		}
		return s.Dir
	}
	var faults []linkFault
	switch s.Fault {
	case netDelay:
		// The leader's messages to one follower arrive late, by less than
		// an election timeout: the follower keeps its leader but applies
		// behind, while its own client link stays fast. A read it served
		// from its own state would be stale (B01a).
		faults = append(faults, linkFault{r.c.peers[lead][follower], proxy.Fault{Kind: proxy.Delay, Dir: proxy.Forward, Delay: s.Delay}})
	case netSwitching:
		for _, l := range busy[3:] {
			faults = append(faults, linkFault{l, proxy.Fault{Kind: proxy.Switching, Dir: proxy.Forward, Delay: s.Delay, Delay2: s.Delay2, Every: s.Every}})
		}
	case netStall:
		faults = append(faults, linkFault{one, proxy.Fault{Kind: proxy.Stall, Dir: dir(one)}})
	case netDrop:
		faults = append(faults, linkFault{one, proxy.Fault{Kind: proxy.Drop, Dir: dir(one)}})
	case netCorrupt:
		faults = append(faults, linkFault{one, proxy.Fault{Kind: proxy.Corrupt, Dir: dir(one)}})
	case netDuplicate:
		faults = append(faults, linkFault{one, proxy.Fault{Kind: proxy.Duplicate, Dir: dir(one)}})
	case netReset:
		n := int(s.Pick % 3)
		for _, l := range busy {
			if l.To == n || l.From == n {
				faults = append(faults, linkFault{l, proxy.Fault{Kind: proxy.Reset}})
			}
		}
	case netCut:
		// The leader's messages to one follower vanish; the follower's
		// messages to the leader still arrive.
		faults = append(faults, linkFault{r.c.peers[lead][follower], proxy.Fault{Kind: proxy.Cut, Dir: proxy.Forward}})
	case netSlow:
		// One leader link at a few KiB/s while the other runs at full
		// speed.
		faults = append(faults, linkFault{r.c.peers[lead][follower], proxy.Fault{Kind: proxy.Rate, Dir: proxy.Forward, Rate: s.Rate}})
	default:
		return fmt.Errorf("unknown fault %q", s.Fault)
	}
	return r.network(ctx, s, lead, faults, rec)
}

type linkFault struct {
	l *link
	f proxy.Fault
}

// network installs faults on their proxies, holds them, and heals them.
// Every proxy must report the requested fault installed and at least one
// hit before the heal.
func (r *runner) network(ctx context.Context, s step, lead int, faults []linkFault, rec *record) error {
	rec.Installed, rec.Effect = true, true
	var errs []error
	for _, lf := range faults {
		awaitTraffic(ctx, lf.l, lf.f.Dir)
	}
	for _, lf := range faults {
		st, err := lf.l.Proxy.Set(lf.f)
		lr := linkRecord{Link: lf.l.Name, Requested: lf.f, Installed: st}
		if err != nil || st.Fault != lf.f {
			rec.Installed = false
			errs = append(errs, errors.Join(fmt.Errorf("%s: requested %+v, installed %+v", lf.l.Name, lf.f, st.Fault), err))
		}
		rec.Links = append(rec.Links, lr)
	}
	holdErr := r.hold(ctx, s.Hold, lead)
	rec.Healed = true
	for i, lf := range faults {
		rec.Links[i].BeforeHeal = lf.l.Proxy.Stats()
		if rec.Links[i].BeforeHeal.Hits == 0 {
			rec.Effect = false
			errs = append(errs, fmt.Errorf("%s: no hit in %v", lf.l.Name, s.Hold))
		}
		st, err := lf.l.Proxy.Set(proxy.Fault{Kind: proxy.None})
		rec.Links[i].Healed = st
		if err != nil || st.Fault.Kind != proxy.None {
			rec.Healed = false
			errs = append(errs, errors.Join(fmt.Errorf("%s: heal left %+v", lf.l.Name, st.Fault), err))
		}
	}
	rec.HealedAt = r.since()
	return errors.Join(append(errs, holdErr)...)
}

// trafficWait bounds awaitTraffic.
const trafficWait = 15 * time.Second

// awaitTraffic waits, up to trafficWait, until l has an open connection
// and new bytes in the directions of dir, so that a fault installed then
// has traffic to act on. A client link to a node that just started has
// no client yet for a while (#224). A fault without a direction (Reset)
// needs only the connection.
func awaitTraffic(ctx context.Context, l *link, dir proxy.Dir) bool {
	start := l.Proxy.Stats()
	return waitUntil(ctx, trafficWait, func() bool {
		st := l.Proxy.Stats()
		return st.Open > 0 &&
			(dir&proxy.Forward == 0 || st.ForwardBytes > start.ForwardBytes) &&
			(dir&proxy.Backward == 0 || st.BackwardBytes > start.BackwardBytes)
	})
}

// kill sends SIGKILL to node i, checks that the kernel reports the
// process killed by it, and restarts the node after the hold.
func (r *runner) kill(ctx context.Context, s step, i int, rec *record) error {
	n := r.c.nodes[i]
	ps, pid, err := n.signal(syscall.SIGKILL)
	rec.Process = &processRecord{Node: fmt.Sprintf("node%d", i+1), Pid: pid, Signal: "SIGKILL"}
	if err != nil {
		return err
	}
	rec.Installed = true
	ws, ok := ps.Sys().(syscall.WaitStatus)
	rec.Process.WaitStatus = ps.String()
	rec.Effect = ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
	holdErr := r.hold(ctx, s.Hold, others(i)[0])
	if err := r.c.start(n); err != nil {
		return errors.Join(holdErr, err)
	}
	rec.Healed = true
	rec.HealedAt = r.since()
	n.mu.Lock()
	rec.Process.ResumedPid = n.cmd.Process.Pid
	n.mu.Unlock()
	return holdErr
}

// pauseProcess sends SIGSTOP to node i, checks that the process is
// stopped, and sends SIGCONT after the hold.
func (r *runner) pauseProcess(ctx context.Context, s step, i int, rec *record) error {
	n := r.c.nodes[i]
	_, pid, err := n.signal(syscall.SIGSTOP)
	rec.Process = &processRecord{Node: fmt.Sprintf("node%d", i+1), Pid: pid, Signal: "SIGSTOP"}
	if err != nil {
		return err
	}
	rec.Installed = true
	st, stErr := waitState(pid, "T", false)
	rec.Process.StateAfter = st
	rec.Effect = stErr == nil && st == "T"
	holdErr := r.hold(ctx, s.Hold, others(i)[0])
	if _, _, err := n.signal(syscall.SIGCONT); err != nil {
		return errors.Join(holdErr, stErr, err)
	}
	st, contErr := waitState(pid, "T", true)
	rec.Process.StateHealed = st
	rec.Healed = contErr == nil
	rec.HealedAt = r.since()
	return errors.Join(holdErr, stErr, contErr)
}

// healAll removes every fault and resumes every process, for the end of
// a run and for a run that stopped early.
func (r *runner) healAll() error {
	var errs []error
	for _, l := range r.c.links {
		if _, err := l.Proxy.Set(proxy.Fault{Kind: proxy.None}); err != nil {
			errs = append(errs, err)
		}
	}
	for _, n := range r.c.nodes {
		n.mu.Lock()
		cmd := n.cmd
		n.mu.Unlock()
		if err := cmd.Process.Signal(syscall.SIGCONT); err != nil && !errors.Is(err, os.ErrProcessDone) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
