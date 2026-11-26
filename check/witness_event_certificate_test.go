package check

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/djosh34/tigerstore/history"
)

func eventUnit(req history.Request, start *history.Start) *revisionOrder {
	h := &history.History{Start: start, Ops: []history.Op{{Req: req, Err: errUnavailable}}}
	r := newRevisionOrder(newWitness(h, time.Now().Add(time.Minute)))
	r.top = r.first + 1
	return r
}

func eventUnitCert(r *revisionOrder, rev int64) eventCertificate {
	c := r.newEventCertificate(rev)
	for k := range r.w.live.keys {
		c.key(r, rev, k)
	}
	c.candidate(r, 0)
	return c
}

func eventImage() *history.Start {
	return &history.Start{Rev: 10, KVs: []history.KeyValue{{Key: "a", Value: "v", CreateRevision: 2, ModRevision: 9, Version: 3}}}
}

func eventCAS(key string, mod int64, put string) history.Request {
	return history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: key, Target: history.TargetMod, Result: history.Equal, Number: mod}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: put, Value: "new"}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}}}}
}

func TestEventCertificateSoundnessDistinctions(t *testing.T) {
	t.Run("covered-absent-not-live-neighbour", func(t *testing.T) {
		st := eventImage()
		st.KVs = append(st.KVs, history.KeyValue{Key: "b", Value: "b", CreateRevision: 2, ModRevision: 9, Version: 3})
		r := eventUnit(history.Request{Delete: &history.DeleteRequest{Key: "a", End: "c"}}, st)
		r.w.live.kvs[0] = history.KeyValue{} // live.keys[0] is still a.
		r.covered["a"] = []span{{9, 11}}
		if eventUnitCert(r, 11).all {
			t.Fatal("absent covered a cannot block deletion of live uncovered b")
		}
		if !r.fitsGranted(0, 11) {
			t.Fatal("the uncovered neighbour is the sole writer")
		}
	})
	t.Run("interval-low-excluded-high-included", func(t *testing.T) {
		for _, rev := range []int64{9, 10, 11} {
			r := eventUnit(history.Request{Delete: &history.DeleteRequest{Key: "a"}}, eventImage())
			r.top = 20
			r.covered["a"] = []span{{9, 10}}
			if c := eventUnitCert(r, rev); c.all != (rev == 10) {
				t.Fatalf("rev %d: %+v", rev, c)
			}
		}
	})
	for _, tc := range []struct {
		name  string
		req   history.Request
		start *history.Start
	}{
		{"missing-mod-zero", eventCAS("missing", 0, "b"), &history.Start{Rev: 10}},
		{"image-mod-not-create-or-version", eventCAS("a", 9, "b"), eventImage()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := eventUnit(tc.req, tc.start)
			if eventUnitCert(r, 11).all || !r.fitsGranted(0, 11) {
				t.Fatal("successful CAS putting an uncovered different key must fall through")
			}
		})
	}
	t.Run("historical-range-cannot-write", func(t *testing.T) {
		q := eventCAS("a", 8, "b")
		q.Txn.Failure[0].Range.Revision = 1
		r := eventUnit(q, eventImage())
		if c := eventUnitCert(r, 11); !c.all || c.reasons[eventReadOnlyCAS] != 1 {
			t.Fatalf("compacted failure range: %+v", c)
		}
		if r.fitsGranted(0, 11) {
			t.Fatal("compacted range cannot write")
		}
	})
	t.Run("image-empty-lease-is-live", func(t *testing.T) {
		st := eventImage()
		st.Leases = []int64{7}
		r := eventUnit(history.Request{Put: &history.PutRequest{Key: "b", Value: "v", Lease: 7}}, st)
		if len(r.grantsOf[7]) != 0 || !r.w.live.hasLease(7) {
			t.Fatal("image lease setup")
		}
		if eventUnitCert(r, 11).all || !r.fitsGranted(0, 11) {
			t.Fatal("image's empty lease permits the sole writer")
		}
	})
	t.Run("trial-grant-even-covered-put", func(t *testing.T) {
		h := &history.History{Start: eventImage(), Ops: []history.Op{
			{Req: history.Request{Put: &history.PutRequest{Key: "a", Value: "v", Lease: 7}}, Err: errUnavailable},
			{Req: history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 7, TTL: 10}}, Err: errUnavailable},
		}}
		r := newRevisionOrder(newWitness(h, time.Now().Add(time.Minute)))
		r.w.search = r
		r.top = 11
		r.covered["a"] = []span{{9, 11}}
		r.grantsOf[7] = []int{1}
		if eventUnitCert(r, 11).all {
			t.Fatal("trial grant must force unknown before testing covered")
		}
		before := r.w.lastID
		if r.fitsGranted(0, 11) {
			t.Fatal("covered put still fails")
		}
		if r.w.lastID <= before {
			t.Fatal("failed trial must demonstrate the generation side effect")
		}
	})
	t.Run("plain-compaction", func(t *testing.T) {
		r := eventUnit(history.Request{Compact: &history.CompactRequest{Revision: 9}}, eventImage())
		if c := eventUnitCert(r, 11); !c.all || c.reasons[eventCompaction] != 1 || r.fitsGranted(0, 11) {
			t.Fatalf("compaction: %+v", c)
		}
	})
	for _, n := range []int{32, 33} {
		t.Run(fmt.Sprintf("range-end-%d", n), func(t *testing.T) {
			r := eventUnit(history.Request{Delete: &history.DeleteRequest{Key: "a", End: strings.Repeat("z", n)}}, eventImage())
			r.covered["a"] = []span{{9, 11}}
			if c := eventUnitCert(r, 11); c.all != (n == 32) {
				t.Fatalf("endpoint length %d: %+v", n, c)
			}
		})
	}
	for _, q := range []history.Request{
		{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "b"}}, {Delete: &history.DeleteRequest{Key: "a", End: history.AllKeys}}}}},
		{Put: &history.PutRequest{Key: strings.Repeat("x", 33), Lease: 99}},
		{LeaseRevoke: &history.LeaseRevokeRequest{ID: 7}},
	} {
		r := eventUnit(q, eventImage())
		if eventUnitCert(r, 11).all {
			t.Fatalf("unsupported input certified: %+v", q)
		}
	}
	if n := unsafe.Sizeof(eventCertificate{}); n > 64 {
		t.Fatalf("summary owns %d bytes, bound 64", n)
	}
}

func TestEventCertificateGuards(t *testing.T) {
	for _, guard := range []string{"above-top", "acked", "resolved-used-or-invalid", "slow", "acked-revoke", "13-keys", "disabled"} {
		t.Run(guard, func(t *testing.T) {
			r := eventUnit(history.Request{Compact: &history.CompactRequest{Revision: 9}}, eventImage())
			switch guard {
			case "above-top":
				r.top = 10
			case "acked":
				r.setWriter(11, 0)
			case "resolved-used-or-invalid":
				r.resolved[11] = -1
			case "slow":
				r.w.slow = true
			case "acked-revoke":
				r.revokes = r.add(r.revokes, 11, 0)
			case "disabled":
				r.testNoEventCertificate = true
			case "13-keys":
				for len(r.w.live.keys) < 13 {
					r.w.live.keys = append(r.w.live.keys, "z")
					r.w.live.kvs = append(r.w.live.kvs, history.KeyValue{})
				}
			}
			if eventUnitCert(r, 11).all {
				t.Fatal("guard permitted certification")
			}
		})
	}
	t.Run("12-keys", func(t *testing.T) {
		r := eventUnit(history.Request{Compact: &history.CompactRequest{Revision: 9}}, eventImage())
		for len(r.w.live.keys) < 12 {
			r.w.live.keys = append(r.w.live.keys, "z")
			r.w.live.kvs = append(r.w.live.kvs, history.KeyValue{})
		}
		if !eventUnitCert(r, 11).all {
			t.Fatal("12-key limit is inclusive")
		}
	})
	t.Run("existing-advanced-frame", func(t *testing.T) {
		r := eventUnit(history.Request{Compact: &history.CompactRequest{Revision: 9}}, eventImage())
		f := &frame{rev: 11, cands: []int{0, 0}, next: 1, mark: r.save()}
		r.stack = []*frame{f}
		claims := 0
		r.testCertificate = func(int64, eventCertificate, []int) { claims++ }
		if r.write(11) || claims != 0 || f.next != 2 || len(r.stack) != 0 {
			t.Fatal("existing cursor must exhaust normally, without a new certificate")
		}
	})
}

func TestEventOracleStopsAreInconclusive(t *testing.T) {
	o := runEventOracle(eventDeadHistory(1, 12), true, 2_000_000, nil, true)
	if !o.timeout || len(o.sites) == 0 {
		t.Fatal("oracle must stop inside a real pool")
	}
	for _, site := range o.sites {
		if len(site.pool) == 0 || site.fits == 0 || confirmedEventSite(site) {
			t.Fatalf("stopped false fit confirmed a certificate: %+v", site)
		}
	}
}

func TestEventOracleIndependentEnumeration(t *testing.T) {
	h := &history.History{Start: &history.Start{Rev: 2}, Ops: []history.Op{
		{Call: 1, Return: 2, Req: eventCAS("a", 0, "b"), Err: errUnavailable},
		{Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "a", End: history.AllKeys, CountOnly: true}}, Resp: &history.Response{Revision: 3, Range: &history.RangeResponse{Count: 1}}},
	}}
	o := runEventOracle(h, true, 2_000_000, nil, false)
	if o.err != "pass" || len(o.sites) != 1 {
		t.Fatalf("sole CAS setup: %s", o.err)
	}
	for _, site := range o.sites {
		if !slices.Contains(site.pool, 0) || !site.accepted {
			t.Fatal("old enumeration must find the CAS independently")
		}
	}
	// Actually omit the CAS from the claimant's indexed pool, without
	// changing the isolated reference replay. Its now-empty fused list
	// fabricates a certificate; the oracle must refuse to confirm it.
	mutated := runEventOracle(h, false, 2_000_000, func(r *revisionOrder) { r.writing = nil }, false)
	if len(mutated.claims) != 1 {
		t.Fatal("omission mutation did not claim a dead gap")
	}
	for _, claim := range mutated.claims {
		site := o.sites[claim.key]
		if site == nil || !slices.Contains(site.pool, 0) || confirmedEventSite(site) {
			t.Fatal("oracle missed the omitted sole CAS writer")
		}
	}
}

func TestEventCertificateStopsBeforeExhaustion(t *testing.T) {
	for _, sortPhase := range []bool{false, true} {
		t.Run(fmt.Sprint("sort=", sortPhase), func(t *testing.T) {
			h := eventDeadHistory(1, 1)
			q := h.Ops[0]
			h.Ops = h.Ops[len(h.Ops)-1:]
			for i := range 150_000 {
				q.Call = int64((i * 521) % 997)
				q.Client = i + 1
				h.Ops = append(h.Ops, q)
			}
			w := newWitness(h, time.Now().Add(time.Minute))
			r := newRevisionOrder(w)
			w.search = r
			if err := r.index(); err != nil {
				t.Fatal(err)
			}
			if !r.finish(r.first) {
				t.Fatal(r.failure)
			}
			claims := 0
			r.testCertificate = func(int64, eventCertificate, []int) { claims++ }
			w.stop = func() bool {
				if !sortPhase {
					return true
				}
				var pcs [32]uintptr
				n := runtime.Callers(1, pcs[:])
				frames := runtime.CallersFrames(pcs[:n])
				for {
					f, more := frames.Next()
					if strings.Contains(f.Function, "sortWithStep") {
						return true
					}
					if !more {
						break
					}
				}
				return false
			}
			if r.write(11) || !w.stopped || r.failure != nil || claims != 0 || len(r.stack) != 0 {
				t.Fatalf("partial construction treated as exhaustion: stopped=%v failure=%v claims=%d", w.stopped, r.failure, claims)
			}
		})
	}
}

func TestEventCertificateParentCachesAndThresholds(t *testing.T) {
	r := eventUnit(history.Request{Delete: &history.DeleteRequest{Key: "a"}}, eventImage())
	r.covered["a"] = []span{{9, 11}}
	r.w.search = r
	if !r.finish(10) || r.dueWork == nil || r.w.times == nil {
		t.Fatal("finish(parent) must build due and eventBounds")
	}
	beforeID := r.w.lastID
	beforeAt := slices.Clone(r.w.at)
	beforeFirsts := slices.Clone(r.w.firsts)
	if r.write(11) || r.w.lastID != beforeID || !slices.Equal(beforeAt, r.w.at) || !slices.Equal(beforeFirsts, r.w.firsts) || r.revokesByLB != nil {
		t.Fatal("certification changed placements/generations or reached mayRevoke")
	}
	var steps [2]int64
	for j, skip := range []bool{false, true} {
		x := dueShape(130)
		for i := range x.optional {
			x.optional[i].gone = int64(i + 1)
			x.optional[i].by = int64(i + 1)
			x.optional[i].from = int64(i + 1)
		}
		for range x.dueAt(2, 1, 1) {
		}
		m := x.save()
		before := x.w.steps
		if !skip {
			for range x.dueAt(131, 130, 130) {
			}
		}
		cursors := [3]int{x.dueWork.timeAt, x.dueWork.revAt, x.dueWork.fromAt}
		x.restore(m)
		if cursors != [3]int{x.dueWork.timeAt, x.dueWork.revAt, x.dueWork.fromAt} {
			t.Fatal("restore must retain all three due thresholds")
		}
		if !skip && cursors != [3]int{130, 130, 130} {
			t.Fatal("late query must cross bitmap words")
		}
		got := slices.Collect(x.dueAt(65, 64, 64))
		if len(got) != 64 || x.dueWork.timeAt != 64 || x.dueWork.revAt != 64 || x.dueWork.fromAt != 64 {
			t.Fatal("earlier parent alternative did not rewind across bitmap boundary")
		}
		steps[j] = x.w.steps - before
	}
	if steps[1] > steps[0] {
		t.Fatalf("shortcut costs more than traversing thresholds: old %d new %d", steps[0], steps[1])
	}
}
