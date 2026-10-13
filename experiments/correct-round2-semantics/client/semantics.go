package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/djosh34/tigerstore/history"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseDiagnostic struct {
	Kind   string `json:"kind"`
	Call   int64  `json:"call"`
	Return int64  `json:"return"`
	ID     int64  `json:"id"`
	TTL    int64  `json:"ttl"`
	Error  string `json:"error,omitempty"`
}

func semantics(c config) (err error) {
	if len(csv(c.endpoints)) != 3 || c.watchers < 3 || c.watchers > 300 {
		return errors.New("require three endpoints and 3..300 watchers")
	}
	cs, err := clients(c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeClients(cs)) }()
	if err = os.MkdirAll(c.out, 0755); err != nil {
		return err
	}
	r := &recorder{origin: time.Now()}
	rc := receipt{Seed: c.seed, Endpoints: csv(c.endpoints), History: filepath.Join(c.out, "history.json")}
	var raw []leaseDiagnostic
	var assertions []string
	watchCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
		rc.Ops = len(r.h.Ops)
		if err != nil {
			rc.Failure = err.Error()
		}
		err = errors.Join(err, save(rc.History, &r.h), save(filepath.Join(c.out, "receipt.json"), rc), save(filepath.Join(c.out, "watch-responses.json"), r.diagnostics), save(filepath.Join(c.out, "lease-diagnostics.json"), raw), save(filepath.Join(c.out, "assertions.json"), assertions))
	}()
	prefix := "/correct-round2/"
	end := clientv3.GetPrefixRangeEnd(prefix)
	call := func(i int, q history.Request) history.Op { return r.call(cs[i], i, q, c.requestTimeout) }
	must := func(i int, q history.Request) (history.Op, error) {
		o := call(i, q)
		if o.Resp == nil {
			return o, fmt.Errorf("required operation failed: %+v: %+v", q, o.Err)
		}
		return o, nil
	}
	addWatch := func(i int, start int64) {
		r.mu.Lock()
		idx := len(r.h.Watches)
		r.h.Watches = append(r.h.Watches, history.Watch{Client: 100 + idx, Req: history.WatchRequest{Key: prefix, End: end, StartRevision: start, PrevKV: true, ProgressNotify: true}})
		r.mu.Unlock()
		wg.Add(1)
		go func() { defer wg.Done(); r.watch(watchCtx, cs[i], idx, prefix) }()
	}
	for i := 0; i < c.watchers; i++ {
		addWatch(i%3, 1)
	}
	// All watchers must be created before writes or faults. A silent watcher fails.
	until := time.Now().Add(15 * time.Second)
	for {
		r.mu.Lock()
		created := 0
		for _, w := range r.h.Watches {
			for _, v := range w.Responses {
				if v.Created {
					created++
					break
				}
			}
		}
		r.mu.Unlock()
		if created == c.watchers {
			break
		}
		if time.Now().After(until) {
			return fmt.Errorf("only %d/%d watchers created", created, c.watchers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	put := func(k, v string, l int64) history.TxnOp {
		return history.TxnOp{Put: &history.PutRequest{Key: prefix + k, Value: v, Lease: l, PrevKV: true}}
	}
	mixed := func(seq int) error {
		i := seq % 3
		key := prefix + fmt.Sprintf("k%d", seq%6)
		v := fmt.Sprintf("seed%d-op%d", c.seed, seq)
		if _, e := must(i, history.Request{Put: &history.PutRequest{Key: key, Value: v, PrevKV: true}}); e != nil {
			return e
		}
		if _, e := must(i, history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: prefix + "missing", Target: history.TargetVersion, Result: history.Equal, Number: 0}}, Success: []history.TxnOp{put("a", v, 0), put("b", v, 0), put("c", v, 0)}}}); e != nil {
			return e
		}
		if _, e := must(i, history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: key, Target: history.TargetVersion, Result: history.Equal, Number: -1}}, Success: []history.TxnOp{put("never", v, 0)}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}}}}); e != nil {
			return e
		}
		if _, e := must(i, history.Request{Delete: &history.DeleteRequest{Key: prefix + "a", End: prefix + "d", PrevKV: true}}); e != nil {
			return e
		}
		_, e := must(i, history.Request{Range: &history.RangeRequest{Key: prefix, End: end}})
		return e
	}
	smokeRounds := 3
	if c.scenario == "lease" {
		smokeRounds = 1
	}
	for seq := 0; seq < smokeRounds; seq++ {
		if e := mixed(seq); e != nil {
			return e
		}
	}
	var grant history.Op
	const leaseID int64 = 49001
	if c.scenario == "lease" {
		grant, err = must(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: leaseID, TTL: 12}})
		if err != nil {
			return err
		}
		r.mu.Lock()
		r.h.Ops = append(r.h.Ops, history.Op{Client: 10000, Call: grant.Call, Return: grant.Call, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: leaseID}}, Err: &history.Error{Code: "Unknown", Message: "lease expiry", Ambiguous: true}})
		r.mu.Unlock()
		_, err = must(1, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{put("lease0", "lease-value0", leaseID), put("lease1", "lease-value1", leaseID), put("lease2", "lease-value2", leaseID)}}})
		if err != nil {
			return err
		}
	}
	if err = os.WriteFile(filepath.Join(c.out, "prepared"), []byte("watchers and initial writes ready\n"), 0644); err != nil {
		return err
	}
	// During faults, keep writes and renewals running. Ambiguous writes remain in history.
	lastKA := grant.Call
	ttl := int64(12)
	renew := func(i int) bool {
		d := leaseDiagnostic{Kind: "keepalive", Call: r.now(), ID: leaseID}
		ctx, stop := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), c.requestTimeout)
		ka, e := cs[i].KeepAliveOnce(ctx, clientv3.LeaseID(leaseID))
		stop()
		d.Return = r.now()
		if e != nil {
			d.Error = e.Error()
		} else {
			d.TTL = ka.TTL
			if ka.TTL > 0 {
				lastKA = d.Call
				ttl = ka.TTL
			}
		}
		raw = append(raw, d)
		return e == nil && ka.TTL > 0
	}
	var live struct {
		Healthy []int `json:"healthy"`
	}
	compacted := false
	grantedDuringFault := false
	blackholeGrant := false
	seq := 3
	faultEnd := time.Now().Add(45 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(c.out, "faults-healed")); e == nil {
			break
		}
		if time.Now().After(faultEnd) {
			return errors.New("controller did not heal faults within 45s")
		}
		q := history.Request{Put: &history.PutRequest{Key: prefix + fmt.Sprintf("k%d", seq%6), Value: fmt.Sprintf("fault-seed%d-op%d", c.seed, seq), PrevKV: true}}
		if c.scenario == "lease" {
			q = history.Request{Range: &history.RangeRequest{Key: prefix + "lease", End: prefix + "leasf"}}
		}
		call(seq%3, q)
		seq++
		if c.scenario == "lease" {
			if !blackholeGrant {
				if _, e := os.Stat(filepath.Join(c.out, "blackhole-live")); e == nil {
					g := r.call(cs[1], 1, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 49003, TTL: 2}}, 200*time.Millisecond)
					if g.Resp != nil || g.Err != nil && g.Err.Ambiguous {
						r.mu.Lock()
						r.h.Ops = append(r.h.Ops, history.Op{Client: 10002, Call: g.Call, Return: g.Call, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 49003}}, Err: &history.Error{Code: "Unknown", Message: "lease expiry", Ambiguous: true}})
						r.mu.Unlock()
					}
					blackholeGrant = true
					if e = os.WriteFile(filepath.Join(c.out, "grant-during-blackhole"), []byte("partitioned grant attempted, ambiguity retained\n"), 0644); e != nil {
						return e
					}
				}
			}
			renew(seq % 3)
			if !grantedDuringFault {
				if e := load(filepath.Join(c.out, "fault-live.json"), &live); e == nil && len(live.Healthy) == 2 {
					i := live.Healthy[0] - 1
					// Node numbers sum to six; convert the missing node to index.
					isolated := 6 - live.Healthy[0] - live.Healthy[1] - 1
					if !renew(i) {
						continue
					}
					g, e := must(i, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 49002, TTL: 2}})
					if e != nil {
						continue
					}
					r.mu.Lock()
					r.h.Ops = append(r.h.Ops, history.Op{Client: 10001, Call: g.Call, Return: g.Call, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 49002}}, Err: &history.Error{Code: "Unknown", Message: "lease expiry", Ambiguous: true}})
					r.mu.Unlock()
					if _, e = must(i, history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{put("faultlease0", "fault0", 49002), put("faultlease1", "fault1", 49002)}}}); e != nil {
						return e
					}
					isolatedGrant := r.call(cs[isolated], isolated, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 49004, TTL: 2}}, 200*time.Millisecond)
					if isolatedGrant.Resp != nil || isolatedGrant.Err != nil && isolatedGrant.Err.Ambiguous {
						r.mu.Lock()
						r.h.Ops = append(r.h.Ops, history.Op{Client: 10003, Call: isolatedGrant.Call, Return: isolatedGrant.Call, Req: history.Request{LeaseRevoke: &history.LeaseRevokeRequest{ID: 49004}}, Err: &history.Error{Code: "Unknown", Message: "lease expiry", Ambiguous: true}})
						r.mu.Unlock()
					}
					if !renew(i) {
						return errors.New("majority renewal failed during paused leader")
					}
					time.Sleep(3 * time.Second)
					o, e := must(i, history.Request{Range: &history.RangeRequest{Key: prefix + "faultlease", End: prefix + "faultleasf"}})
					if e != nil {
						return e
					}
					if o.Resp.Range.Count != 0 || o.Call < g.Call+2*int64(time.Second)-int64(100*time.Millisecond) {
						return errors.New("short lease did not expire atomically during leader pause")
					}
					if e = os.WriteFile(filepath.Join(c.out, "lease-grants-during-fault"), []byte("majority grant, isolated ambiguous grant, majority expiry verified\n"), 0644); e != nil {
						return e
					}
					assertions = append(assertions, "majority lease grant and two-key expiry acknowledged during leader pause; isolated grant retained as ambiguous")
					grantedDuringFault = true
				}
			}
		}
		if c.scenario == "compaction" && !compacted {
			if e := load(filepath.Join(c.out, "fault-live.json"), &live); e == nil && len(live.Healthy) == 2 {
				i := live.Healthy[0] - 1
				o := call(i, history.Request{Range: &history.RangeRequest{Key: prefix, End: end}})
				if o.Resp != nil && o.Resp.Revision > 3 {
					rev := o.Resp.Revision - 2
					co := call(i, history.Request{Compact: &history.CompactRequest{Revision: rev}})
					if co.Resp != nil {
						assertions = append(assertions, fmt.Sprintf("compaction %d acknowledged while leader paused", rev))
						// Every old-revision cancellation is in the checked history. Resume a new
						// logical watch at C+1, never erase the canceled part.
						for j := 0; j < 3; j++ {
							addWatch(j, rev-1)
							addWatch(j, rev+1)
						}
						old := call(i, history.Request{Range: &history.RangeRequest{Key: prefix, End: end, Revision: rev - 1}})
						if old.Err == nil || old.Err.Code != "OutOfRange" {
							return fmt.Errorf("old revision %d not rejected as compacted: %+v", rev-1, old)
						}
						compacted = true
						if e := os.WriteFile(filepath.Join(c.out, "compacted-during-fault"), []byte(fmt.Sprintf("%d\n", rev)), 0644); e != nil {
							return e
						}
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if c.scenario == "compaction" && !compacted {
		return errors.New("compaction did not acknowledge during the fault")
	}
	if c.scenario == "lease" {
		if !grantedDuringFault || !blackholeGrant {
			return errors.New("lease grant coverage missing during partition or paused leader")
		}
		// Renew after healing, then keep all attached keys alive for longer than the
		// granted TTL. The final renewal begins the no-early-expiry interval.
		for j := 0; j < 14; j++ {
			if !renew(j % 3) {
				return errors.New("post-heal keepalive failed")
			}
			o, e := must(j%3, history.Request{Range: &history.RangeRequest{Key: prefix + "lease", End: prefix + "leasf"}})
			if e != nil {
				return e
			}
			if o.Resp.Range.Count != 3 {
				return fmt.Errorf("renewed lease lost keys: %+v", o.Resp)
			}
			time.Sleep(time.Second)
		}
		assertions = append(assertions, "three attached keys survived fourteen successful post-heal renewals")
		expiryEnd := time.Now().Add(25 * time.Second)
		for {
			o, e := must(0, history.Request{Range: &history.RangeRequest{Key: prefix + "lease", End: prefix + "leasf"}})
			if e != nil {
				return e
			}
			if o.Resp.Range.Count == 0 {
				if o.Call < lastKA+ttl*int64(time.Second)-int64(100*time.Millisecond) {
					return errors.New("lease keys disappeared before successful renewal TTL")
				}
				break
			}
			if o.Resp.Range.Count != 3 {
				return errors.New("lease expiry split attached keys")
			}
			if time.Now().After(expiryEnd) {
				return errors.New("lease did not expire within 25s after last keepalive")
			}
			time.Sleep(time.Second)
		}
		ctx, stop := context.WithTimeout(context.Background(), c.requestTimeout)
		lt, e := cs[1].TimeToLive(ctx, clientv3.LeaseID(leaseID))
		stop()
		d := leaseDiagnostic{Kind: "ttl-after-expiry", Call: r.now(), Return: r.now(), ID: leaseID}
		if e != nil {
			d.Error = e.Error()
		} else {
			d.TTL = lt.TTL
		}
		raw = append(raw, d)
		if e != nil || lt.TTL != -1 {
			return errors.New("expired lease still exists")
		}
		// Header revisions, values, previous values and delete uniqueness are checked
		// against the independent committed log by all repository checkers below.
		assertions = append(assertions, fmt.Sprintf("lease expired after last renewal call %d plus TTL %d", lastKA, ttl))
		for _, id := range []int64{49002, 49003, 49004} {
			ctx, stop := context.WithTimeout(context.Background(), c.requestTimeout)
			lt, e := cs[1].TimeToLive(ctx, clientv3.LeaseID(id))
			stop()
			d := leaseDiagnostic{Kind: "fault-grant-ttl-after-expiry", Call: r.now(), Return: r.now(), ID: id}
			if e != nil {
				d.Error = e.Error()
			} else {
				d.TTL = lt.TTL
			}
			raw = append(raw, d)
			if e != nil || lt.TTL != -1 {
				return fmt.Errorf("fault-time lease %d did not expire", id)
			}
		}
	}
	finalRounds := 12
	if c.scenario == "lease" {
		finalRounds = 1
	}
	for j := 0; j < finalRounds; j++ {
		if e := mixed(seq + j); e != nil {
			return e
		}
	}
	// Definite no-revision log barriers fence stock etcd's persisted commit.
	for i := range cs {
		_, e := must(i, history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: prefix + "k0", Target: history.TargetVersion, Result: history.Equal, Number: -1}}, Success: []history.TxnOp{put("never", "barrier", 0)}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: prefix + "k0"}}}}})
		if e != nil {
			return e
		}
	}
	var terminal history.Op
	for i := range cs {
		o, e := must(i, history.Request{Range: &history.RangeRequest{Key: "\x00", End: history.AllKeys}})
		if e != nil {
			return e
		}
		if terminal.Resp == nil {
			terminal = o
		} else if !reflect.DeepEqual(terminal.Resp, o.Resp) {
			return errors.New("terminal member reads disagree")
		}
	}
	r.h.Target = terminal.Resp.Revision
	rc.Target = r.h.Target
	rc.Range = terminal.Resp.Range
	finish := time.Now().Add(c.watchTimeout)
	for {
		complete := true
		r.mu.Lock()
		for _, w := range r.h.Watches {
			covered := false
			for _, v := range w.Responses {
				if v.Progress && v.Revision >= r.h.Target || v.Canceled && v.CompactRevision > 0 {
					covered = true
				}
			}
			complete = complete && covered
		}
		r.mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(finish) {
			return errors.New("watch progress/cancellation target timeout")
		}
		for _, cl := range cs {
			ctx, stop := context.WithTimeout(clientv3.WithRequireLeader(context.Background()), c.requestTimeout)
			e := cl.RequestProgress(ctx)
			stop()
			if e != nil {
				return e
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.scenario == "lease" {
		for i, w := range r.h.Watches {
			deletes := 0
			rev := int64(0)
			for _, v := range w.Responses {
				for _, ev := range v.Events {
					if ev.Type == history.EventDelete && ev.PrevKV != nil && ev.PrevKV.Lease == leaseID {
						deletes++
						if rev == 0 {
							rev = ev.Revision()
						} else if rev != ev.Revision() {
							return errors.New("lease keys deleted at different revisions")
						}
						if v.Time < lastKA+ttl*int64(time.Second)-int64(100*time.Millisecond) {
							return errors.New("lease watch expiry arrived early")
						}
					}
				}
			}
			if deletes != 3 {
				return fmt.Errorf("watch %d received %d lease deletes, expected exactly 3", i, deletes)
			}
		}
		assertions = append(assertions, "each watch received exactly three lease deletes at one revision with prev_kv")
		for i, w := range r.h.Watches {
			count := 0
			rev := int64(0)
			for _, v := range w.Responses {
				for _, ev := range v.Events {
					if ev.Type == history.EventDelete && ev.PrevKV != nil && ev.PrevKV.Lease == 49002 {
						count++
						if rev == 0 {
							rev = ev.Revision()
						} else if rev != ev.Revision() {
							return errors.New("fault-time lease expiry split revisions")
						}
					}
				}
			}
			if count != 2 {
				return fmt.Errorf("watch %d has %d fault-time lease deletes, want two", i, count)
			}
		}
		assertions = append(assertions, "each watch received exactly two deletes from fault-time lease at one revision with prev_kv")
	}
	if c.scenario == "compaction" {
		canceled := 0
		for _, w := range r.h.Watches {
			for _, v := range w.Responses {
				if v.Canceled && v.CompactRevision > 0 {
					canceled++
				}
			}
		}
		if canceled != 3 {
			return fmt.Errorf("expected three old-revision cancels, got %d", canceled)
		}
		assertions = append(assertions, "three old-revision watches canceled and three C+1 watches reached target")
	}
	return errors.Join(r.errs...)
}
