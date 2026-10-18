package check

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// WitnessMode says where Witnessed takes the order of the writes from.
type WitnessMode uint8

const (
	// FromLog orders the writes as h.Log, the log the cluster committed,
	// does. Each mutation finds its entry as in Durable.
	FromLog WitnessMode = iota + 1
	// FromRevisions orders them by the revisions clients saw, without
	// h.Log or the Effects. An ambiguous write goes where a read saw what
	// it put. One that no read saw fills a revision that no acked write
	// holds, found by a bounded search.
	FromRevisions
)

func (m WitnessMode) String() string {
	switch m {
	case FromLog:
		return "log"
	case FromRevisions:
		return "revisions"
	}
	return fmt.Sprintf("mode %d", uint8(m))
}

// Witnessed checks that h.Ops are linearizable without searching for the
// order (#112). Every write that changes the store gets its own revision,
// and every response names the revision it was served at, so the
// revisions name the order: the writes by revision, and every other op at
// a state of its header revision. Witnessed builds that order, replays it
// through the model Linearizable uses, and checks that every acked op gets
// exactly its response and that the order follows real time. A pass is a
// linearization, so it is sound whatever the revisions or the log say. A
// wrong log or a wrong guess for an ambiguous write can only make it fail.
//
// What goes where:
//   - A mutation that changed the store is an event of the order: in
//     FromLog its log entry, in FromRevisions its revision. An ambiguous
//     one that never took effect is left out, which is legal for a call
//     that never returned. The log may also hold lease expiries, as
//     LeaseRevoke entries that no op sent.
//   - An acked op that changed nothing goes at the first state of its
//     header revision where the model gives its response and real time
//     allows.
//   - A rejection the model decides (compacted, future revision, lease not
//     found or already there) goes at the first state within its call and
//     return where the model gives the same error. Other rejections
//     changed nothing and are left out, as are serializable reads and
//     failed reads.
//
// The store must be empty at the start of the history. Not finishing
// within timeout is a failure, never a pass.
func Witnessed(h *history.History, mode WitnessMode, timeout time.Duration) error {
	if err := h.Validate(); err != nil {
		return err
	}
	if timeout <= 0 {
		return fmt.Errorf("witness check needs a positive timeout, got %s", timeout)
	}
	w := newWitness(h, time.Now().Add(timeout))
	var err error
	switch mode {
	case FromLog:
		err = w.fromLog()
		if err == nil {
			err = w.observe()
		}
		if err == nil {
			err = w.verify()
		}
	case FromRevisions:
		// The search places every op and checks real time itself, and
		// tries another order when either fails.
		err = w.fromRevisions()
	default:
		return fmt.Errorf("unknown witness mode %d", mode)
	}
	if err == nil && time.Now().After(w.deadline) {
		err = errWitnessTimeout
	}
	if errors.Is(err, errWitnessTimeout) {
		err = fmt.Errorf("%w; %s", err, w.stoppedAt())
	}
	if err != nil {
		return fmt.Errorf("witness check (%s) of %d operations: %w", mode, len(h.Ops), err)
	}
	return nil
}

// errWitnessTimeout ends a check that passed its deadline.
var errWitnessTimeout = errors.New("timed out")

// witness is one check: the order it builds, the replay of its events
// through the model, and where each op goes.
type witness struct {
	h        *history.History
	deadline time.Time
	steps    int
	keys     []string
	// reqs are the ops' requests as the model sees them.
	reqs []history.Request
	// events are the mutations of the order, and states[j] is the store
	// after events[:j].
	events []event
	states []summary
	// live is the store after every event, past every version, and
	// appended the key of each version in the order added.
	live     *state
	past     versions
	appended []string
	// at is where each op of h.Ops goes, and placed lists the ops that
	// apply placed, in order.
	at     []place
	placed []int
	// kvs caches the keys at revision kvsRev, for stateAt.
	kvs    []history.KeyValue
	kvsRev int64
	// observed and toObserve count the ops observe placed and has to
	// place, and search is the search of FromRevisions, for the report
	// of a timeout.
	observed, toObserve int
	search              *revisionOrder
}

// stoppedAt says how far a check got when it ran out of time.
func (w *witness) stoppedAt() string {
	s := fmt.Sprintf("the order held %d events up to revision %d", len(w.events), w.live.rev)
	if w.toObserve > 0 {
		s += fmt.Sprintf(", and %d of the %d ops that changed nothing had their place", w.observed, w.toObserve)
	}
	if w.search != nil {
		s += "; " + w.search.progress()
	}
	return s
}

// event is one mutation of the order, sent by op, or by no op (-1): a
// lease expiry, or a log entry that changed nothing.
type event struct {
	op  int
	req history.Request
	// out is what the model gave it.
	out outcome
}

// summary is what of the store an event can change besides the keys.
type summary struct {
	rev, compacted int64
	leases         []int64
}

// place is where an op goes in the order: event at, which leads from
// state at-1 to state at; or, for an op that changed nothing (seen),
// state at, among the other such ops there by call time.
type place struct {
	set  bool
	seen bool
	at   int
	rank int64
}

func (p place) compare(q place) int {
	if c := cmp.Compare(p.at, q.at); c != 0 {
		return c
	}
	if p.seen != q.seen {
		if p.seen {
			return 1
		}
		return -1
	}
	return cmp.Compare(p.rank, q.rank)
}

func newWitness(h *history.History, deadline time.Time) *witness {
	w := &witness{h: h, deadline: deadline, past: versions{}, at: make([]place, len(h.Ops)), kvsRev: -1}
	w.reqs = make([]history.Request, len(h.Ops))
	for i := range h.Ops {
		w.reqs[i] = modelRequest(&h.Ops[i])
	}
	w.keys = keysOf(slices.Concat(w.reqs, h.Log))
	w.live = newState(w.keys)
	w.live.past = w.past
	w.states = []summary{w.summary(nil)}
	return w
}

// tick counts a step of work and reports whether the deadline passed.
func (w *witness) tick() bool {
	w.steps++
	return w.steps%1024 == 0 && time.Now().After(w.deadline)
}

// summary is the live store's summary after req. It shares the previous
// summary's lease list unless req changed the leases.
func (w *witness) summary(req *history.Request) summary {
	s := summary{rev: w.live.rev, compacted: w.live.compacted}
	if req == nil || req.LeaseGrant != nil || req.LeaseRevoke != nil {
		s.leases = slices.Clone(w.live.leases)
	} else {
		s.leases = w.states[len(w.states)-1].leases
	}
	return s
}

// apply adds an event sent by op and replays it.
func (w *witness) apply(op int, req history.Request) outcome {
	o := w.live.step(req, true)
	w.past.add(o.events)
	for _, e := range o.events {
		w.appended = append(w.appended, e.KV.Key)
	}
	o.events = nil
	w.events = append(w.events, event{op: op, req: req, out: o})
	w.states = append(w.states, w.summary(&req))
	if op >= 0 {
		w.at[op] = place{set: true, at: len(w.events)}
		w.placed = append(w.placed, op)
	}
	return o
}

// stateAt returns state j, which the caller may change.
func (w *witness) stateAt(j int) *state {
	sm := w.states[j]
	if w.kvsRev != sm.rev {
		w.kvs = make([]history.KeyValue, len(w.keys))
		for k, key := range w.keys {
			if kv, ok := w.past.at(key, sm.rev); ok {
				w.kvs[k] = kv
			}
		}
		w.kvsRev = sm.rev
	}
	return &state{keys: w.keys, kvs: slices.Clone(w.kvs), leases: slices.Clone(sm.leases), rev: sm.rev, compacted: sm.compacted, past: w.past}
}

// answers reports whether op i, which changed nothing, gets its response
// or its error at state j.
func (w *witness) answers(i, j int) bool {
	op := &w.h.Ops[i]
	o := w.stateAt(j).step(w.reqs[i], false)
	if op.Err != nil {
		return o.err != nil && sameError(o.err, op.Err)
	}
	return o.err == nil && matches(o, op.Resp)
}

// mutation reports whether op is a mutation that changed the store, as
// far as the client knows: an acked one whose response says so, or an
// ambiguous one.
func mutation(op *history.Op) bool {
	if !op.Req.Mutates() {
		return false
	}
	if op.Resp != nil {
		return respChanges(op)
	}
	return op.Err.Ambiguous
}

// fromLog makes the log's entries the events, each linked to the op that
// sent it.
func (w *witness) fromLog() error {
	if len(w.h.Log) == 0 {
		return errNoLog
	}
	for i, req := range w.h.Log {
		if req.LeaseGrant != nil && req.LeaseGrant.ID == 0 {
			return fmt.Errorf("log entry %d grants a lease without its ID", i)
		}
		w.apply(-1, req)
	}
	// Entries that changed the store, by their request. A grant is also
	// filed with ID -1, for ambiguous grants whose ID the client never
	// learned.
	entries := map[string][]int{}
	file := func(req history.Request, e int) error {
		k, err := fingerprint(req)
		entries[k] = append(entries[k], e)
		return err
	}
	for e, ev := range w.events {
		if ev.out.err != nil || !changes(ev.req, ev.out) {
			continue
		}
		if err := file(ev.req, e); err != nil {
			return err
		}
		if g := ev.req.LeaseGrant; g != nil {
			if err := file(anyGrant(g.TTL), e); err != nil {
				return err
			}
		}
	}
	claim := func(i int, req history.Request, fits func(e int) bool) (bool, error) {
		k, err := fingerprint(req)
		if err != nil {
			return false, err
		}
		for _, e := range entries[k] {
			if w.events[e].op < 0 && fits(e) {
				w.events[e].op = i
				w.at[i] = place{set: true, at: e + 1}
				return true, nil
			}
		}
		return false, nil
	}
	// Acked mutations first, each at the entry that gave its response.
	var ambiguous []int
	for i := range w.h.Ops {
		op := &w.h.Ops[i]
		if !mutation(op) {
			continue
		}
		if op.Resp == nil {
			ambiguous = append(ambiguous, i)
			continue
		}
		ok, err := claim(i, w.reqs[i], func(e int) bool { return matches(w.events[e].out, op.Resp) })
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("acked %s is not in the log with the response it got", w.describe(i))
		}
	}
	// Then ambiguous ones, in call order, each at the first entry that
	// comes after every acked mutation that returned before its call.
	slices.SortFunc(ambiguous, func(a, b int) int { return cmp.Compare(w.h.Ops[a].Call, w.h.Ops[b].Call) })
	bound := w.boundAfter()
	for _, i := range ambiguous {
		op := &w.h.Ops[i]
		req := op.Req
		if g := req.LeaseGrant; g != nil && g.ID == 0 {
			req = anyGrant(g.TTL)
		}
		lo := bound(op.Call)
		if _, err := claim(i, req, func(e int) bool { return e+1 > lo }); err != nil {
			return err
		}
	}
	for e, ev := range w.events {
		if ev.op < 0 && ev.out.err == nil && changes(ev.req, ev.out) && ev.req.LeaseRevoke == nil {
			return fmt.Errorf("log entry %d %s changed the store but no client sent it, or its client got an error that says it had no effect", e, describeRequest(ev.req))
		}
	}
	return nil
}

// boundAfter returns, for a time t, the last event of an acked op placed
// so far that returned before t.
func (w *witness) boundAfter() func(t int64) int {
	type ret struct {
		at   int64
		last int
	}
	var rs []ret
	for i := range w.h.Ops {
		if p := w.at[i]; p.set && w.h.Ops[i].Resp != nil {
			rs = append(rs, ret{w.h.Ops[i].Return, p.at})
		}
	}
	slices.SortFunc(rs, func(a, b ret) int { return cmp.Compare(a.at, b.at) })
	for k := 1; k < len(rs); k++ {
		rs[k].last = max(rs[k].last, rs[k-1].last)
	}
	return func(t int64) int {
		n := sort.Search(len(rs), func(k int) bool { return rs[k].at >= t })
		if n == 0 {
			return 0
		}
		return rs[n-1].last
	}
}

// observe places every op that changed nothing, in call order, at the
// first state that gives its answer and that real time allows: after
// every op that returned before its call, and before every event of an op
// called after it returned. An acked one goes at a state of its header
// revision, a rejection the model decides at any state. The first state
// is never worse: it leaves the ops called later the most room.
func (w *witness) observe() error {
	var ops []int
	for i := range w.h.Ops {
		op := &w.h.Ops[i]
		switch {
		case w.at[i].set:
		case op.Req.Range != nil && op.Req.Range.Serializable:
		case op.Resp != nil:
			if mutation(op) {
				return fmt.Errorf("acked %s changed the store but has no place in the order", w.describe(i))
			}
			ops = append(ops, i)
		case !op.Err.Ambiguous && isModelError(op.Err):
			ops = append(ops, i)
		}
	}
	// The states of each revision.
	first, last := map[int64]int{}, map[int64]int{}
	for j, s := range w.states {
		if _, ok := first[s.rev]; !ok {
			first[s.rev] = j
		}
		last[s.rev] = j
	}
	events := w.eventBounds()
	slices.SortFunc(ops, func(a, b int) int {
		return cmp.Or(cmp.Compare(w.h.Ops[a].Call, w.h.Ops[b].Call), cmp.Compare(a, b))
	})
	placed := newPlacedMax(w.h, ops)
	w.observed, w.toObserve = 0, len(ops)
	for _, i := range ops {
		if w.tick() {
			return errWitnessTimeout
		}
		w.observed++
		op := &w.h.Ops[i]
		lo, hi := events(op)
		lo = max(lo, placed.before(op.Call))
		f, l := lo, hi
		if op.Resp != nil {
			var ok bool
			if f, ok = first[op.Resp.Revision]; !ok {
				return fmt.Errorf("%s got header revision %d, which no state of the order has (the last is %d)", w.describe(i), op.Resp.Revision, w.live.rev)
			}
			l = last[op.Resp.Revision]
			lo, hi = max(lo, f), min(hi, l)
		}
		j, ok := w.find(i, lo, hi)
		if !ok {
			return w.unanswered(i, f, l, lo, hi)
		}
		w.at[i] = place{set: true, seen: true, at: j, rank: op.Call}
		placed.add(i, j)
	}
	return nil
}

// find returns the first state in [lo, hi] at which op i gets its answer.
func (w *witness) find(i, lo, hi int) (int, bool) {
	for j := lo; j <= hi; j++ {
		if w.answers(i, j) {
			return j, true
		}
	}
	return 0, false
}

// unanswered is the failure of op i, whose revision spans states [f, l],
// and which real time allows in [lo, hi].
func (w *witness) unanswered(i, f, l, lo, hi int) error {
	var b strings.Builder
	b.WriteString(w.describe(i))
	if lo > hi {
		fmt.Fprintf(&b, " has no state that real time allows: its revision's states are %d to %d, real time allows %d to %d", f, l, lo, hi)
		return errors.New(b.String())
	}
	o := w.stateAt(lo).step(w.reqs[i], false)
	fmt.Fprintf(&b, " gets no state from %d to %d that answers it; at state %d (revision %d) the model gives %s", lo, hi, lo, w.states[lo].rev, outputOf(o))
	fmt.Fprintf(&b, "; the order around it: %s", w.around(lo))
	return errors.New(b.String())
}

// around describes the events near state j.
func (w *witness) around(j int) string {
	var parts []string
	for e := max(1, j-2); e <= min(len(w.events), j+2); e++ {
		ev := w.events[e-1]
		who := "no op"
		if ev.op >= 0 {
			who = fmt.Sprintf("op %d", ev.op)
		}
		parts = append(parts, fmt.Sprintf("event %d (%s, to revision %d) %s", e, who, w.states[e].rev, describeRequest(ev.req)))
	}
	return strings.Join(parts, "; ")
}

func outputOf(o outcome) string {
	if o.err != nil {
		return fmt.Sprintf("error %q", o.err.Message)
	}
	return output{resp: o.resp}.String()
}

// describe names op i for a failure.
func (w *witness) describe(i int) string { return describeOp(w.h, i) }

// eventBounds returns, for an op, the states that the placed events allow
// it at: after every event of an acked op that returned before its call,
// and before every event of an op called after it returned.
func (w *witness) eventBounds() func(op *history.Op) (int, int) {
	bound := w.boundAfter()
	type call struct {
		at    int64
		first int
	}
	var cs []call
	for i := range w.h.Ops {
		if p := w.at[i]; p.set && !p.seen {
			cs = append(cs, call{w.h.Ops[i].Call, p.at})
		}
	}
	slices.SortFunc(cs, func(a, b call) int { return cmp.Compare(a.at, b.at) })
	for k := len(cs) - 2; k >= 0; k-- {
		cs[k].first = min(cs[k].first, cs[k+1].first)
	}
	return func(op *history.Op) (int, int) {
		hi := len(w.states) - 1
		n := sort.Search(len(cs), func(k int) bool { return cs[k].at > op.Return })
		if n < len(cs) {
			hi = cs[n].first - 1
		}
		return bound(op.Call), hi
	}
}

// placedMax keeps the latest state of the ops placed so far among a set,
// by their return times.
type placedMax struct {
	h       *history.History
	returns []int64
	rank    map[int]int
	tree    []int
}

func newPlacedMax(h *history.History, ops []int) *placedMax {
	p := &placedMax{h: h, rank: map[int]int{}, tree: make([]int, len(ops)+1)}
	sorted := slices.Clone(ops)
	slices.SortFunc(sorted, func(a, b int) int { return cmp.Or(cmp.Compare(h.Ops[a].Return, h.Ops[b].Return), cmp.Compare(a, b)) })
	for k, i := range sorted {
		p.rank[i] = k + 1
		p.returns = append(p.returns, h.Ops[i].Return)
	}
	return p
}

func (p *placedMax) add(i, j int) {
	for k := p.rank[i]; k < len(p.tree); k += k & -k {
		p.tree[k] = max(p.tree[k], j)
	}
}

// before returns the latest state of a placed op that returned before t.
func (p *placedMax) before(t int64) int {
	n := sort.Search(len(p.returns), func(k int) bool { return p.returns[k] >= t })
	m := 0
	for k := n; k > 0; k -= k & -k {
		m = max(m, p.tree[k])
	}
	return m
}

// verify checks that the order follows real time: an op that returned
// before another was called comes before it. An ambiguous op never
// returned.
func (w *witness) verify() error {
	var ops []int
	for i, p := range w.at {
		if p.set {
			ops = append(ops, i)
		}
	}
	ret := func(i int) int64 {
		if w.h.Ops[i].Err != nil && w.h.Ops[i].Err.Ambiguous {
			return math.MaxInt64
		}
		return w.h.Ops[i].Return
	}
	byCall, byReturn := slices.Clone(ops), slices.Clone(ops)
	slices.SortFunc(byCall, func(a, b int) int { return cmp.Compare(w.h.Ops[a].Call, w.h.Ops[b].Call) })
	slices.SortFunc(byReturn, func(a, b int) int { return cmp.Compare(ret(a), ret(b)) })
	latest, by := place{}, -1
	k := 0
	for _, b := range byCall {
		if w.tick() {
			return errWitnessTimeout
		}
		for ; k < len(byReturn) && ret(byReturn[k]) < w.h.Ops[b].Call; k++ {
			if a := byReturn[k]; by < 0 || w.at[a].compare(latest) > 0 {
				latest, by = w.at[a], a
			}
		}
		if by >= 0 && latest.compare(w.at[b]) >= 0 {
			return fmt.Errorf("the order puts %s before %s, which returned before it was called", w.describe(b), w.describe(by))
		}
	}
	return nil
}
