package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
	"github.com/djosh34/tigerstore/history"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// cutMember partitions the original member's incoming and outgoing peer
// links. Replacement peers use direct URLs, so these cuts run before any
// replacement, as enforced by plan.
func (r *runner) cutMember(i int, rec *record) (func() error, error) {
	var held []*link
	heal := func() error {
		var errs []error
		for k, l := range held {
			rec.Links[k].BeforeHeal = l.Proxy.Stats()
			st, err := l.Proxy.Set(proxy.Fault{Kind: proxy.None})
			rec.Links[k].Healed = st
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}
	for _, j := range others(i) {
		for _, l := range []*link{r.c.peers[i][j], r.c.peers[j][i]} {
			f := proxy.Fault{Kind: proxy.Cut, Dir: proxy.Both}
			st, err := l.Proxy.Set(f)
			if err != nil {
				return heal, errors.Join(err, heal())
			}
			held = append(held, l)
			rec.Links = append(rec.Links, linkRecord{Link: l.Name, Requested: f, Installed: st})
		}
	}
	return heal, nil
}

// losePower uses LazyFS when present and an ordinary SIGKILL otherwise.
func (r *runner) losePower(i int, rec *record) error {
	ps, _, err := r.c.nodes[i].signal(syscall.SIGKILL)
	if err != nil {
		return err
	}
	rec.Env.Exited = ps.String()
	// DropCaches calls LazyFS's power loss before dropping the kernel cache.
	return r.e.disks[i].DropCaches()
}

// tripleLoss takes one follower down, partitions the other and slows the
// leader. No quorum remains, so only recovery, not fault-time availability,
// is required. The disk fault runs below ext4 without LazyFS; under LazyFS
// CPU throttling is used instead, since LazyFS masks disk latency.
func (r *runner) tripleLoss(ctx context.Context, s step, lead int, rec *record) (err error) {
	a, b := others(lead)[0], others(lead)[1]
	heal, err := r.cutMember(b, rec)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, heal()) }()
	lazy := strings.HasSuffix(r.e.disks[lead].Data, ".data")
	if lazy {
		err = r.e.groups[lead].LimitCPU(3)
		defer func() { err = errors.Join(err, r.e.groups[lead].LimitCPU(0)) }()
	} else {
		err = r.e.disks[lead].Delay(25*time.Millisecond, 150*time.Millisecond)
		defer func() { err = errors.Join(err, r.e.disks[lead].Healthy()) }()
	}
	if err != nil {
		return err
	}
	if err := r.losePower(a, rec); err != nil {
		return err
	}
	rec.Env.Parts = []string{fmt.Sprintf("power loss node%d", a+1), fmt.Sprintf("partition node%d", b+1), fmt.Sprintf("gray node%d", lead+1)}
	rec.Installed = true
	holdErr := r.hold(ctx, 3*time.Second, lead)
	if err := r.c.start(r.c.nodes[a]); err != nil {
		return errors.Join(holdErr, err)
	}
	rec.Effect = true
	rec.Healed, rec.HealedAt = true, r.since()
	return holdErr
}

// compactObserved never compacts an event a recorded watch still needs.
// It is still a real replicated compaction, concurrent with recovery and
// cleaning, and is recorded for both witness modes and Porcupine.
func (r *runner) compactObserved(ctx context.Context) func() error {
	stop, done := make(chan struct{}), make(chan error, 1)
	go func() {
		last := int64(0)
		count := 0
		for {
			select {
			case <-stop:
				if count == 0 {
					done <- errors.New("round 3: no observed-revision compaction succeeded")
				} else {
					done <- nil
				}
				return
			case <-time.After(time.Second):
			}
			r.t.r.mu.Lock()
			minRev := int64(1<<63 - 1)
			for _, w := range r.t.r.h.Watches {
				next := int64(1)
				for _, wr := range w.Responses {
					next = resumeRevision(next, wr)
				}
				minRev = min(minRev, next-2)
			}
			r.t.r.mu.Unlock()
			if minRev <= last {
				continue
			}
			for i := range 3 {
				op := r.t.r.call(r.t.cs[i], 60, history.Request{Compact: &history.CompactRequest{Revision: minRev}}, 2*time.Second)
				if op.Err == nil {
					last, count = minRev, count+1
					break
				}
			}
		}
	}()
	return func() error { close(stop); return <-done }
}

// memberRetryRound3 resolves ambiguous changes by inspecting the member
// list before sending the same change again. A failed add has no chosen ID.
func memberRetryRound3(ctx context.Context, what string, call func(context.Context) error) error {
	var err error
	for end := time.Now().Add(45 * time.Second); time.Now().Before(end); {
		cctx, cancel := context.WithTimeout(ctx, requestTimeout)
		err = call(cctx)
		cancel()
		if err == nil {
			return nil
		}
		if e := pause(ctx, 500*time.Millisecond); e != nil {
			return errors.Join(err, e)
		}
	}
	return fmt.Errorf("%s: %w", what, err)
}

// replaceLeaderLearner removes the live leader with a follower partitioned,
// immediately adds its replacement as a learner, and leaves it offline
// while writes move beyond the log window. Another voter then loses power
// during catch-up. Promotion and a second power loss test the new member's
// persisted configuration, not just its in-memory membership.
func (r *runner) replaceLeaderLearner(ctx context.Context, s step, lead int, rec *record) (err error) {
	finishCompact := r.compactObserved(ctx)
	defer func() { err = errors.Join(err, finishCompact()) }()
	n, old := r.c.nodes[lead], r.c.ids[lead]
	a, b := others(lead)[0], others(lead)[1]
	ctl := r.c.nodes[a].control
	heal, err := r.cutMember(b, rec)
	if err != nil {
		return err
	}
	// The remove can commit on the old quorum, but its answer and the
	// following add need the new quorum. Heal independently of that call.
	healed := make(chan error, 1)
	go func() { healed <- errors.Join(pause(ctx, 4*time.Second), heal()) }()
	n.mu.Lock()
	n.expecting = true
	n.mu.Unlock()
	rec.Installed = true
	err = memberRetryRound3(ctx, "remove the leader", func(c context.Context) error {
		ms, err := ctl.MemberList(c)
		if err != nil {
			return err
		}
		for _, m := range ms.Members {
			if m.ID == old {
				_, err := ctl.MemberRemove(c, old)
				return err
			}
		}
		return nil
	})
	if err = errors.Join(err, <-healed); err != nil {
		return err
	}
	if _, _, err := n.signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	peerURL := fmt.Sprintf("https://127.0.0.1:%d", r.c.peerPorts[lead])
	var newID uint64
	if err := memberRetryRound3(ctx, "add replacement learner", func(c context.Context) error {
		ms, err := ctl.MemberList(c)
		if err != nil {
			return err
		}
		for _, m := range ms.Members {
			if slices.Equal(m.PeerURLs, []string{peerURL}) {
				if m.ID == old || !m.IsLearner {
					return fmt.Errorf("replacement has wrong identity or role: %v", m)
				}
				newID = m.ID
				return nil
			}
		}
		resp, err := ctl.MemberAddAsLearner(c, []string{peerURL})
		if err == nil {
			newID = resp.Member.ID
		}
		return err
	}); err != nil {
		return err
	}
	// These writes are in the recorded key space and keep only six live
	// values. The learner has not started and cannot keep up with cleaning.
	for k := range 300 {
		q := history.Request{Put: &history.PutRequest{Key: fmt.Sprintf("%sk%d", prefix, k%6), Value: fmt.Sprintf("replacement-%d-%s", k, strings.Repeat("b", 1024)), PrevKV: true}}
		if op := r.t.r.call(r.t.cs[a], 61, q, requestTimeout); op.Err != nil {
			return fmt.Errorf("catch-up burst: %s", op.Err.Message)
		}
	}
	if err := r.e.erase(r.e.disks[lead]); err != nil {
		return err
	}
	n.args = rejoinArgs(n.args)
	r.c.ids[lead] = newID
	from := logSize(n)
	if err := r.c.start(n); err != nil {
		return err
	}
	if err := r.losePower(b, rec); err != nil {
		return err
	}
	holdErr := r.hold(ctx, 2*time.Second, a)
	if err := r.c.start(r.c.nodes[b]); err != nil {
		return errors.Join(holdErr, err)
	}
	if holdErr != nil {
		return holdErr
	}
	if !waitUntil(ctx, 30*time.Second, func() bool { return strings.Contains(logSince(n, from), "fetching the snapshot") }) {
		return errors.New("replacement learner did not fetch a snapshot")
	}
	if err := memberRetryRound3(ctx, "promote replacement", func(c context.Context) error {
		ms, err := ctl.MemberList(c)
		if err != nil {
			return err
		}
		for _, m := range ms.Members {
			if m.ID == newID && !m.IsLearner {
				return nil
			}
		}
		_, err = ctl.MemberPromote(c, newID)
		return err
	}); err != nil {
		return err
	}
	if err := r.losePower(lead, rec); err != nil {
		return err
	}
	if err := r.c.start(n); err != nil {
		return err
	}
	if err := r.membersAgree(ctx); err != nil {
		return err
	}
	rec.Env.Parts = []string{fmt.Sprintf("removed leader %x", old), fmt.Sprintf("added and promoted learner %x", newID), "snapshot catch-up beside voter power loss", "promoted member power loss", "compaction"}
	rec.Effect, rec.Healed, rec.HealedAt = true, true, r.since()
	return nil
}

func (r *runner) rollingLoss(ctx context.Context, s step, rec *record) (err error) {
	finishCompact := r.compactObserved(ctx)
	defer func() { err = errors.Join(err, finishCompact()) }()
	for k := range 3 {
		i := (k + int(s.Pick%3)) % 3
		if err := r.losePower(i, rec); err != nil {
			return err
		}
		rec.Installed = true
		holdErr := r.hold(ctx, 3*time.Second, others(i)[0])
		if err := r.c.start(r.c.nodes[i]); err != nil {
			return errors.Join(holdErr, err)
		}
		if holdErr != nil {
			return holdErr
		}
		if err := r.membersAgree(ctx); err != nil {
			return err
		}
		rec.Env.Parts = append(rec.Env.Parts, fmt.Sprintf("power cycle node%d", i+1))
	}
	rec.Effect, rec.Healed, rec.HealedAt = true, true, r.since()
	return nil
}

// membersAgree checks each node's local list against the expected IDs,
// not against another node alone. Original peer URLs differ by proxy, so
// compare identities and voter roles. Status must name one of those IDs
// as leader on every node. The terminal recorded reads check data.
func (r *runner) membersAgree(ctx context.Context) error {
	return memberRetryRound3(ctx, "all local memberships and leaders agree", func(c context.Context) error {
		var leader uint64
		for i, n := range r.c.nodes {
			ms, err := n.control.MemberList(c, clientv3.WithSerializable())
			if err != nil {
				return err
			}
			if len(ms.Members) != 3 {
				return fmt.Errorf("node%d has %d members", i+1, len(ms.Members))
			}
			for _, m := range ms.Members {
				if !slices.Contains(r.c.ids[:], m.ID) || m.IsLearner {
					return fmt.Errorf("node%d has unexpected member %v; expected voters %v", i+1, m, r.c.ids)
				}
			}
			st, err := r.c.status(c, i)
			if err != nil {
				return err
			}
			if i == 0 {
				leader = st.Leader
			}
			if st.Header.MemberId != r.c.ids[i] || !slices.Contains(r.c.ids[:], st.Leader) || st.Leader != leader || st.IsLearner {
				return fmt.Errorf("node%d disagrees: %+v", i+1, st)
			}
		}
		return nil
	})
}
