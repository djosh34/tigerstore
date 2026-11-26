package check

// Scratch-only Actions instrumentation for #876 round 2. This branch is
// never to merge. It does not prune candidates or change the search budget.
import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/djosh34/tigerstore/history"
)

var Measure876Sink func(string, []byte)
var measure876Log []measure876Effect
var measure876LogFirst int64

const (
	m876Stop = iota
	m876RevokeNoKey
	m876ModelError
	m876NoRevision
	m876Response
	m876Unchanged
	m876Lives
	m876Created
	m876Written
	m876Deleted
	m876Counts
	m876Room
	m876Accepted
	m876Clauses
)

var measure876Names = []string{"stop", "revoke-no-key", "model-error", "no-revision-write", "response", "unchanged", "lives", "created", "written", "deleted", "counts", "room", "accepted"}

type measure876Effect struct {
	Hash       [32]byte
	Shape      string
	Request    *history.Request   `json:",omitempty"`
	Events     []history.Event    `json:",omitempty"`
	After      []history.KeyValue `json:",omitempty"`
	EventIndex int
	Op         int
}

type measure876Candidate struct {
	Op                                    int
	Call, LB                              int64
	Kind                                  string
	Request                               history.Request
	Clauses                               [m876Clauses]int64
	GrantNone, GrantTried, GrantSucceeded int64
}

type measure876Certificate struct {
	Certified            bool
	Why                  string
	Items                int
	BlockedPresent       []string
	UnblockedPresent     []string
	AbsentWithoutPutters []string
	AbsentPutter         map[string]int
	AbsentUnchanged      []string
}

type measure876Prefix struct {
	Rev                    int64
	ParentOp               int
	CandidateCount         int
	Certificate            measure876Certificate
	Probes, Fits, Accepted int64
	Exhausted              bool
	Before                 []history.KeyValue
}

type measure876Work struct {
	Calls         int64
	WeightedSteps int64
}

type measure876Stats struct {
	ranks           [2][]int32
	Candidates      [2][]measure876Candidate
	Prefixes        []measure876Prefix
	Finish          [2]measure876Work
	Restore         [2]measure876Work
	Effects         map[int64]measure876Effect
	ReachWhy        int
	DiagnosticItems int64
}

func measure876Target(rev int64) int {
	if rev == 432504 {
		return 0
	}
	if rev == 432505 {
		return 1
	}
	return -1
}

func newMeasure876(r *revisionOrder) *measure876Stats {
	m := &measure876Stats{Effects: map[int64]measure876Effect{}}
	for j := range m.ranks {
		m.ranks[j] = make([]int32, len(r.used))
	}
	return m
}

func measure876Kind(q history.Request) string {
	switch {
	case q.Put != nil:
		return "put"
	case q.Delete != nil:
		return "delete"
	case q.Txn != nil:
		return "txn"
	case q.LeaseRevoke != nil:
		return "revoke"
	case q.LeaseGrant != nil:
		return "grant"
	case q.Compact != nil:
		return "compact"
	default:
		return "other"
	}
}

func (m *measure876Stats) candidate(r *revisionOrder, rev int64, op int) *measure876Candidate {
	j := measure876Target(rev)
	if j < 0 {
		return nil
	}
	k := m.ranks[j][op]
	if k == 0 {
		m.Candidates[j] = append(m.Candidates[j], measure876Candidate{Op: op, Call: r.w.calls[op], LB: r.lb[op], Kind: measure876Kind(r.w.reqs[op]), Request: r.w.reqs[op]})
		k = int32(len(m.Candidates[j]))
		m.ranks[j][op] = k
	}
	return &m.Candidates[j][k-1]
}

// This conservative event certificate needs no counts: a present key must
// forbid every write event through unchanged(), and an absent key must have
// NO eligible unused ambiguous putter. Any txn putter is unknown, including
// a put subsequently undone by an overlapping range delete. Total putter
// work is capped; counts never establish absent-key coverage here.
// Measurement only: a positive result is counted but never used to prune.
func measure876Cert(r *revisionOrder, s *state, used []bool, rev int64) measure876Certificate {
	c := measure876Certificate{AbsentPutter: map[string]int{}}
	if rev > r.top {
		c.Why = "above-top"
		return c
	}
	if _, ok := r.writer(rev); ok {
		c.Why = "acked-writer"
		return c
	}
	if _, ok := r.resolved[rev]; ok {
		c.Why = "resolved"
		return c
	}
	if r.w.slow {
		c.Why = "slow"
		return c
	}
	if len(s.keys) > 12 {
		c.Why = "key-cap"
		return c
	}
	for k, key := range s.keys {
		c.Items++
		if s.kvs[k].Version != 0 {
			if r.unchanged(key, rev) {
				c.BlockedPresent = append(c.BlockedPresent, key)
			} else {
				c.UnblockedPresent = append(c.UnblockedPresent, key)
			}
			continue
		}
		if r.unchanged(key, rev) {
			c.AbsentUnchanged = append(c.AbsentUnchanged, key)
		}
		for _, op := range r.putters[k] {
			c.Items++
			if c.Items > 4096 {
				c.Why = "putter-cap"
				return c
			}
			if used[op] || r.lb[op] >= rev {
				continue
			}
			if at, reserved := r.reserved[op]; reserved && at != rev {
				continue
			}
			c.AbsentPutter[key] = op
			break
		}
		if _, possible := c.AbsentPutter[key]; !possible {
			c.AbsentWithoutPutters = append(c.AbsentWithoutPutters, key)
		}
	}
	switch {
	case len(c.UnblockedPresent) > 0 && len(c.AbsentPutter) > 0:
		c.Why = "present-not-unchanged+absent-putter"
	case len(c.UnblockedPresent) > 0:
		c.Why = "present-not-unchanged"
	case len(c.AbsentPutter) > 0:
		c.Why = "absent-putter"
	default:
		c.Certified, c.Why = true, "all-write-events-impossible"
	}
	return c
}

func (m *measure876Stats) prefix(r *revisionOrder, f *frame) int {
	if measure876Target(f.rev) < 0 {
		return -1
	}
	if len(m.Candidates[measure876Target(f.rev)]) == 0 {
		for _, op := range f.cands {
			m.candidate(r, f.rev, op)
		}
	}
	c := measure876Cert(r, r.w.live, r.used, f.rev)
	m.DiagnosticItems += int64(c.Items)
	parent := -1
	if len(r.w.events) > 0 {
		parent = r.w.events[len(r.w.events)-1].op
	}
	m.Prefixes = append(m.Prefixes, measure876Prefix{Rev: f.rev, ParentOp: parent, CandidateCount: len(f.cands), Certificate: c, Before: slices.Clone(r.w.live.kvs)})
	return len(m.Prefixes) - 1
}

func measure876Applied(w *witness, op int, req *history.Request, events []history.Event) {
	if len(events) == 0 {
		return
	}
	if len(events) > 64 {
		return
	} // Unsupported diagnostic effect size, not a search rule.
	es := slices.Clone(events)
	sort.SliceStable(es, func(i, j int) bool { return es[i].KV.Key < es[j].KV.Key })
	data, _ := json.Marshal(es)
	e := measure876Effect{Hash: sha256.Sum256(data), Op: op, EventIndex: len(w.events)}
	for _, ev := range es {
		if e.Shape != "" {
			e.Shape += ";"
		}
		if ev.Type == history.EventDelete {
			e.Shape += "delete:" + ev.KV.Key
		} else {
			e.Shape += "put:" + ev.KV.Key + "=" + ev.KV.Value
		}
	}
	if w.search == nil {
		if measure876Log == nil {
			measure876LogFirst = 1
			if w.h.Start != nil {
				measure876LogFirst = w.h.Start.Rev
			}
			n := w.h.Target - measure876LogFirst + 2
			if n < 1 || n > 2_000_000 {
				n = 1
			}
			measure876Log = make([]measure876Effect, n)
		}
		idx := w.live.rev - measure876LogFirst
		if idx < 0 || idx > 2_000_000 {
			return
		}
		for int64(len(measure876Log)) <= idx {
			measure876Log = append(measure876Log, measure876Effect{})
		}
		if w.live.rev >= 432350 && w.live.rev <= 432520 {
			e.Events, e.After = es, slices.Clone(w.live.kvs)
			e.Request = req
		}
		measure876Log[idx] = e
		return
	}
	r := w.search
	if r.measure876 == nil {
		return
	}
	if _, held := r.writer(w.live.rev); held {
		return
	}
	if _, held := r.resolved[w.live.rev]; held {
		return
	}
	e.Events, e.After = es, slices.Clone(w.live.kvs)
	e.Request = req
	r.measure876.Effects[w.live.rev] = e
}

type measure876Frame struct {
	Rev                   int64
	Tried, Candidates     int
	Op                    int
	Before                []history.KeyValue
	Actual                measure876Effect
	Log                   measure876Effect
	SameEvents, SameShape bool
	Certificate           measure876Certificate
	NearLives             map[string][][4]int64
	NearCounts            [][4]int64
	NextFixed             map[int64]string
}

func measure876Emit(name string, v any) {
	if Measure876Sink == nil {
		return
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("measure876 marshal %s: %v\n", name, err)
		return
	}
	Measure876Sink(name, b)
}

func (m *measure876Stats) emit(r *revisionOrder) {
	measure876Emit("candidates-432504.json", m.Candidates[0])
	measure876Emit("candidates-432505.json", m.Candidates[1])
	measure876Emit("prefixes.json", m.Prefixes)
	var frames []measure876Frame
	for _, f := range r.stack {
		x := measure876Frame{Rev: f.rev, Tried: f.next, Candidates: len(f.cands), Op: -1, Before: f.mark.live.kvs, NearLives: map[string][][4]int64{}, NextFixed: map[int64]string{}}
		for j := f.mark.events; j < len(r.w.events) && r.w.states[j].rev < f.rev; j++ {
			if r.w.states[j+1].rev == f.rev {
				x.Op = r.w.events[j].op
				if e, ok := m.Effects[f.rev]; ok && e.EventIndex == j && e.Op == x.Op {
					x.Actual = e
				}
				break
			}
		}
		idx := f.rev - measure876LogFirst
		if idx >= 0 && idx < int64(len(measure876Log)) {
			x.Log = measure876Log[idx]
		}
		x.SameEvents = x.Actual.Shape != "" && x.Actual.Hash == x.Log.Hash
		x.SameShape = x.Actual.Shape != "" && x.Actual.Shape == x.Log.Shape
		x.Certificate = measure876Cert(r, f.mark.live, f.mark.used, f.rev)
		// r.lives is in last-write order from noted. Retain the nearby
		// observations and fixed writes, not millions of whole-history rows.
		for _, key := range r.w.keys {
			ls := r.lives[key]
			at := sort.Search(len(ls), func(j int) bool { return ls[j].m >= f.rev })
			lo, hi := max(0, at-1), min(len(ls), at+8)
			for _, l := range ls[lo:hi] {
				x.NearLives[key] = append(x.NearLives[key], [4]int64{l.c, l.m, l.v, l.to})
			}
		}
		at := sort.Search(len(r.countRevs), func(j int) bool { return r.countRevs[j] >= f.rev })
		for _, rev := range r.countRevs[at:] {
			if rev > f.rev+64 || len(x.NearCounts) >= 64 {
				break
			}
			for _, c := range r.counts[rev] {
				if len(x.NearCounts) >= 64 {
					break
				}
				x.NearCounts = append(x.NearCounts, [4]int64{rev, int64(c.lo), int64(c.hi), c.n})
			}
		}
		for rev := f.rev; rev <= f.rev+16; rev++ {
			if op, ok := r.writer(rev); ok {
				b, _ := json.Marshal(r.w.reqs[op])
				x.NextFixed[rev] = string(b)
			}
		}
		frames = append(frames, x)
	}
	measure876Emit("stack.json", frames)
	var totals [2][m876Clauses]int64
	for j, cs := range m.Candidates {
		for _, c := range cs {
			for k, n := range c.Clauses {
				totals[j][k] += n
			}
		}
	}
	measure876Emit("summary.json", struct {
		Progress        string
		Steps           int64
		Clauses         []string
		Totals          [2][m876Clauses]int64
		Finish, Restore [2]measure876Work
		DiagnosticItems int64
		Failure         string
	}{r.progress(), r.w.steps, measure876Names, totals, m.Finish, m.Restore, m.DiagnosticItems, fmt.Sprint(r.failure)})
	var nearby []struct {
		Rev    int64
		Effect measure876Effect
	}
	for rev := int64(432350); rev <= 432520; rev++ {
		idx := rev - measure876LogFirst
		if idx >= 0 && idx < int64(len(measure876Log)) {
			nearby = append(nearby, struct {
				Rev    int64
				Effect measure876Effect
			}{rev, measure876Log[idx]})
		}
	}
	measure876Emit("log-nearby.json", nearby)
	// Locate the log's unobserved put by its key/value, not an assumed
	// operation identity, and show whether the search consumed it earlier.
	var matches []any
	for i, req := range r.w.reqs {
		match := req.Put != nil && req.Put.Key == "k3" && req.Put.Value == "c3-319862"
		if req.Txn != nil {
			for op := range req.Txn.Ops() {
				if op.Put != nil && op.Put.Key == "k3" && op.Put.Value == "c3-319862" {
					match = true
				}
			}
		}
		if !match {
			continue
		}
		var events []int
		for j, e := range r.w.events {
			if e.op == i {
				events = append(events, j)
			}
		}
		c504, c505 := -1, -1
		for _, f := range r.stack {
			if f.rev == 432504 {
				c504 = slices.Index(f.cands, i)
			}
			if f.rev == 432505 {
				c505 = slices.Index(f.cands, i)
			}
		}
		at, reserved := r.reserved[i]
		matches = append(matches, struct {
			Op                         int
			HistoryOp                  history.Op
			Request                    history.Request
			Used                       bool
			LB                         int64
			Reserved                   bool
			ReservedAt                 int64
			Events                     []int
			Candidate504, Candidate505 int
		}{i, r.w.h.Ops[i], req, r.used[i], r.lb[i], reserved, at, events, c504, c505})
	}
	measure876Emit("log-put-matches.json", matches)
}
