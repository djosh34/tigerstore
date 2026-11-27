package check

// Part of #876. Scratch diagnostics only; this branch is not for merge.
// None of these observers calls a charged/polling search helper.
import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"

	"github.com/djosh34/tigerstore/history"
)

type r3LogEffect struct {
	Hash        uint64
	Entry       int
	Put, Delete uint16
}
type r3Choice struct {
	Op          int
	Hash        uint64
	Put, Delete uint16
	Events      []history.Event
}
type r3Candidate struct {
	Op   int
	Call *history.Op
	Fits map[string]int64
}
type r3Life struct{ Create, Mod, Version, To int64 }

func r3LifeOf(l lifeTo) r3Life { return r3Life{l.c, l.m, l.v, l.to} }

type r3Sample struct {
	Op          int
	Clause, Key string
	KV          history.KeyValue
	Required    r3Life
	State       []history.KeyValue
}
type r3Pool struct {
	Size                      int
	Certified                 bool
	InitialEligible           bool
	MainReasons               [eventRejectReasons]uint32
	Individual                map[string]int
	Live                      []history.KeyValue
	Created, Written, Deleted []int
}
type r3RepairCandidate struct {
	Op             int
	Call           *history.Op
	LB, ReservedAt int64
	Lease          int64
	LeaseLive      bool
	GrantCount     int
	Grants         []int
}
type r3GrantDetail struct {
	Op           int
	Call         *history.Op
	Used, Placed bool
}
type r3Repair struct {
	At, By            int64
	Skip              int
	Key               string
	State             []history.KeyValue
	CountConstraints  [][4]int64
	AckedPutRevisions []int64
	Candidates        []r3RepairCandidate
	GrantDetails      map[int64][]r3GrantDetail
	Fixed             map[int64]*history.Op
}
type r3Gap struct {
	Revision              int64
	Visits, Constructions int64
	FirstPool, LastPool   *r3Pool
	Candidates            map[int]*r3Candidate
	Samples               map[string]r3Sample
	OmittedCandidateIDs   int64
	ReachClause           string
}

var r3 struct {
	Enabled    bool
	Log        []r3LogEffect
	ActualNear map[int64][]history.KeyValue
	Gaps       [6]r3Gap
	Applying   *frame
	JSON       []byte
	Repair     *r3Repair
}

// LateGapR3Start enables a bounded diagnostic for this one long history.
func LateGapR3Start(target int64) {
	r3.Enabled = true
	r3.Repair = nil
	r3.Log = make([]r3LogEffect, target+2)
	r3.ActualNear = map[int64][]history.KeyValue{}
	for i := range r3.Gaps {
		r3.Gaps[i] = r3Gap{Revision: 602475 + int64(i), Candidates: map[int]*r3Candidate{}, Samples: map[string]r3Sample{}}
	}
}
func r3GapAt(rev int64) *r3Gap {
	if !r3.Enabled || rev < 602475 || rev > 602480 {
		return nil
	}
	return &r3.Gaps[rev-602475]
}
func r3Signature(w *witness, events []history.Event) (uint64, uint16, uint16) {
	h := fnv.New64a()
	var put, del uint16
	kv := func(k history.KeyValue) {
		_, _ = fmt.Fprintf(h, "%q %q %d %d %d %d;", k.Key, k.Value, k.CreateRevision, k.ModRevision, k.Version, k.Lease)
	}
	for _, e := range events {
		_, _ = fmt.Fprintf(h, "event %d;", e.Type)
		kv(e.KV)
		if e.PrevKV != nil {
			kv(*e.PrevKV)
		}
		k := w.live.index(e.KV.Key)
		if k >= 0 && k < 16 {
			if e.Type == history.EventPut {
				put |= 1 << k
			} else {
				del |= 1 << k
			}
		}
	}
	return h.Sum64(), put, del
}
func r3Applied(w *witness, op int, before int64, events []history.Event) {
	if !r3.Enabled {
		return
	}
	if w.search == nil {
		if before == w.live.rev {
			return
		}
		rev := w.live.rev
		if rev >= int64(len(r3.Log)) {
			return
		}
		hash, put, del := r3Signature(w, events)
		r3.Log[rev] = r3LogEffect{hash, len(w.events), put, del}
		if rev >= 602470 && rev <= 602482 {
			r3.ActualNear[rev] = slices.Clone(w.live.kvs)
		}
	} else if f := r3.Applying; f != nil {
		hash, put, del := r3Signature(w, events)
		f.r3 = r3Choice{op, hash, put, del, slices.Clone(events)}
	}
}
func r3Unknown(r *revisionOrder, i int) string {
	q := r.w.reqs[i]
	put := func(p *history.PutRequest) string {
		if p.Lease != 0 && !r.w.live.hasLease(p.Lease) {
			if len(r.grantsOf[p.Lease]) != 0 {
				return "put: missing lease has trial grants"
			}
			return "put: missing lease without trial grants"
		}
		return "put: key not covered unchanged"
	}
	switch {
	case q.Put != nil:
		return put(q.Put)
	case q.Delete != nil:
		return "delete: present keys, none covered unchanged"
	case q.LeaseRevoke != nil:
		return "revoke: unsupported stencil"
	case q.Txn != nil:
		t := q.Txn
		if len(t.Compare) != 1 || len(t.Success) != 1 || len(t.Failure) != 1 || t.Success[0].Put == nil || t.Failure[0].Range == nil {
			return fmt.Sprintf("txn: unsupported shape compare=%d success=%d failure=%d", len(t.Compare), len(t.Success), len(t.Failure))
		}
		c, p := t.Compare[0], t.Success[0].Put
		if c.Target != history.TargetMod || c.Result != history.Equal || p.Lease != 0 {
			return "txn: unsupported predicate or leased put"
		}
		return "CAS: compare holds, " + put(p)
	default:
		return "unsupported request"
	}
}
func r3CandidateAt(r *revisionOrder, g *r3Gap, i int) *r3Candidate {
	c := g.Candidates[i]
	if c == nil && len(g.Candidates) < 4096 {
		c = &r3Candidate{Op: i, Call: &r.w.h.Ops[i], Fits: map[string]int64{}}
		g.Candidates[i] = c
	}
	if c == nil {
		g.OmittedCandidateIDs++
	}
	return c
}
func r3PoolAt(r *revisionOrder, rev int64, cands []int, cert eventCertificate) {
	g := r3GapAt(rev)
	if g == nil || r.w.stopped {
		return
	}
	g.Constructions++
	base := r.newEventCertificate(rev)
	for k := range r.w.live.keys {
		base.key(r, rev, k)
	}
	p := &r3Pool{Size: len(cands), Certified: cert.all, InitialEligible: base.all, MainReasons: cert.reasons, Individual: map[string]int{}, Live: slices.Clone(r.w.live.kvs), Created: slices.Clone(r.byRev(r.created, rev)), Written: slices.Clone(r.byRev(r.written, rev)), Deleted: slices.Clone(r.byRev(r.deleted, rev))}
	names := [eventRejectReasons]string{"empty delete", "blocked delete", "blocked put", "missing lease", "read-only CAS", "compaction"}
	for _, i := range cands {
		_ = r3CandidateAt(r, g, i)
		one := base
		one.candidate(r, i)
		reason := "initial certificate guard"
		if base.all {
			reason = "unknown: " + r3Unknown(r, i)
			if one.all {
				for j, n := range one.reasons {
					if n != 0 {
						reason = names[j]
						break
					}
				}
			}
		}
		p.Individual[reason]++
	}
	if g.FirstPool == nil {
		g.FirstPool = p
	}
	g.LastPool = p
}
func r3FitResult(r *revisionOrder, i int, rev int64, why string, accepted bool) {
	g := r3GapAt(rev)
	if g == nil {
		return
	}
	if r.w.stopped {
		why = "stopped (no rejection verdict)"
	} else if accepted {
		why = "accepted"
	}
	if c := r3CandidateAt(r, g, i); c != nil {
		c.Fits[why]++
	}
}
func r3Failure(r *revisionOrder, i int, rev int64, clause, key string, kv history.KeyValue, l lifeTo, s *state) {
	g := r3GapAt(rev)
	if g == nil || r.w.stopped {
		return
	}
	label := clause + ":" + key
	if _, ok := g.Samples[label]; ok || len(g.Samples) >= 48 {
		return
	}
	g.Samples[label] = r3Sample{Op: i, Clause: clause, Key: key, KV: kv, Required: r3LifeOf(l)}
	if s != nil {
		v := g.Samples[label]
		v.State = slices.Clone(s.kvs)
		g.Samples[label] = v
	}
}

// Observe the existing cardinality relaxation without its charged callbacks.
// Capture once, only after the current candidate passed the earlier clauses.
func r3RepairAt(r *revisionOrder, s *state, skip int) {
	if !r3.Enabled || r3.Repair != nil || s.rev != 602475 {
		return
	}
	k := s.index("k6")
	if k < 0 || s.kvs[k].Version != 0 {
		return
	}
	q := &r3Repair{At: s.rev, By: 602484, Skip: skip, Key: "k6", State: slices.Clone(s.kvs), GrantDetails: map[int64][]r3GrantDetail{}, Fixed: map[int64]*history.Op{}}
	for _, c := range r.counts[602484] {
		q.CountConstraints = append(q.CountConstraints, [4]int64{c.rev, int64(c.lo), int64(c.hi), c.n})
	}
	for _, rev := range r.putRevs[k] {
		if rev > s.rev && rev <= q.By {
			q.AckedPutRevisions = append(q.AckedPutRevisions, rev)
		}
	}
	ids := map[int64]bool{}
	seen := map[int]bool{}
	for _, i := range r.putters[k] {
		if seen[i] || i == skip || r.used[i] || r.lb[i] >= q.By || !touches(s, r.w.reqs[i], k, true) {
			continue
		}
		seen[i] = true
		c := r3RepairCandidate{Op: i, Call: &r.w.h.Ops[i], LB: r.lb[i], ReservedAt: r.reserved[i]}
		if p := r.w.reqs[i].Put; p != nil {
			c.Lease = p.Lease
			c.LeaseLive = s.hasLease(p.Lease)
			c.GrantCount = r.grants[p.Lease]
			c.Grants = slices.Clone(r.grantsOf[p.Lease])
			if p.Lease != 0 {
				ids[p.Lease] = true
			}
		}
		q.Candidates = append(q.Candidates, c)
	}
	for i, req := range r.w.reqs {
		g := req.LeaseGrant
		if g != nil && ids[g.ID] {
			q.GrantDetails[g.ID] = append(q.GrantDetails[g.ID], r3GrantDetail{Op: i, Call: &r.w.h.Ops[i], Used: r.used[i], Placed: r.w.at[i].set()})
		}
	}
	for rev := s.rev + 1; rev <= 602485; rev++ {
		if i, ok := r.writer(rev); ok {
			q.Fixed[rev] = &r.w.h.Ops[i]
		} else if i, ok := r.resolved[rev]; ok {
			q.Fixed[rev] = &r.w.h.Ops[i]
		}
	}
	r3.Repair = q
}

func r3Done(r *revisionOrder) {
	if !r3.Enabled {
		return
	}
	type frameRow struct {
		Revision                                      int64
		Tried, Pool, Remaining                        int
		Chosen                                        r3Choice
		ChosenCall                                    *history.Op
		Actual                                        r3LogEffect
		ActualRequest                                 *history.Request
		SameKeyEffect                                 bool
		EarliestCall, LatestCall, LowestLB, HighestLB int64
	}
	var out struct {
		Base                                                           string
		Repair                                                         *r3Repair
		Revision, Target, Steps, MaxSteps                              int64
		Backtracks                                                     int
		Stopped                                                        bool
		FailureRevision                                                int64
		Failure                                                        string
		Keys                                                           []string
		Gaps                                                           [6]r3Gap
		Frames                                                         []frameRow
		ActualNear                                                     map[int64][]history.KeyValue
		Constraints                                                    map[int64]map[string][]r3Life
		FirstDifferentEffectIndex                                      int
		FramesAboveFirstDifference, RemainingSlotsAboveFirstDifference int
	}
	out.Base = "2e1d1e1a"
	out.Repair = r3.Repair
	out.Revision, out.Target, out.Steps, out.MaxSteps = r.rev, r.top, r.w.steps, r.w.maxSteps
	out.Backtracks, out.Stopped, out.FailureRevision = r.backs, r.w.stopped, r.at
	if r.failure != nil {
		out.Failure = r.failure.Error()
	}
	out.Keys, out.Gaps, out.ActualNear = r.w.keys, r3.Gaps, r3.ActualNear
	out.FirstDifferentEffectIndex = -1
	for j, f := range r.stack {
		v := frameRow{Revision: f.rev, Tried: f.next, Pool: len(f.cands), Remaining: len(f.cands) - f.next, Chosen: f.r3}
		if f.r3.Op >= 0 && f.r3.Op < len(r.w.h.Ops) {
			v.ChosenCall = &r.w.h.Ops[f.r3.Op]
		}
		if f.rev < int64(len(r3.Log)) {
			v.Actual = r3.Log[f.rev]
			if v.Actual.Entry >= 0 && v.Actual.Entry < len(r.w.h.Log) {
				v.ActualRequest = &r.w.h.Log[v.Actual.Entry]
			}
		}
		v.SameKeyEffect = v.Chosen.Hash == v.Actual.Hash && v.Chosen.Put == v.Actual.Put && v.Chosen.Delete == v.Actual.Delete
		if v.Chosen.Op >= 0 && !v.SameKeyEffect && out.FirstDifferentEffectIndex < 0 {
			out.FirstDifferentEffectIndex = j
		}
		if len(f.cands) > 0 {
			v.EarliestCall, v.LowestLB = r.w.calls[f.cands[0]], r.lb[f.cands[0]]
			for _, i := range f.cands {
				v.EarliestCall = min(v.EarliestCall, r.w.calls[i])
				v.LatestCall = max(v.LatestCall, r.w.calls[i])
				v.LowestLB = min(v.LowestLB, r.lb[i])
				v.HighestLB = max(v.HighestLB, r.lb[i])
			}
		}
		out.Frames = append(out.Frames, v)
	}
	if k := out.FirstDifferentEffectIndex; k >= 0 {
		out.FramesAboveFirstDifference = len(out.Frames) - k - 1
		for _, f := range out.Frames[k+1:] {
			out.RemainingSlotsAboveFirstDifference += f.Remaining
		}
	}
	out.Constraints = map[int64]map[string][]r3Life{}
	for _, rev := range []int64{602475, 602476, 602480} {
		byKey := map[string][]r3Life{}
		for key, lives := range r.lives {
			for _, l := range lives {
				if l.c <= rev && l.to >= rev {
					byKey[key] = append(byKey[key], r3LifeOf(l))
				}
			}
		}
		out.Constraints[rev] = byKey
	}
	r3.JSON, _ = json.MarshalIndent(out, "", "  ")
}

// LateGapR3JSON returns only bounded counters/catalogs and scalar frame metadata.
func LateGapR3JSON() []byte { return r3.JSON }
