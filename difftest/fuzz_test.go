//go:build difftest

package difftest

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	fuzzDuration = flag.Duration("difffuzz.duration", time.Minute, "how long TestDiffFuzz runs random sequences")
	fuzzSeed     = flag.Uint64("difffuzz.seed", 0, "first seed of TestDiffFuzz; 0 picks one from the clock")
	fuzzOut      = flag.String("difffuzz.out", "testdata/fuzz", "where TestDiffFuzz saves a shrunk mismatch, and TestDiffFuzzFound reads them")
)

// fuzzKeys is how many keys a sequence uses, f/k0 to f/k5.
const fuzzKeys = 6

// fuzzOp is one request of a random sequence, in a form that does not
// depend on the server: keys by index, leases by the order the sequence
// granted them, revisions relative to the sequence's start.
type fuzzOp struct {
	Kind string `json:"kind"`
	Key  int    `json:"key,omitempty"`
	// End is 0 for one key, -1 for every key from Key on, else key End-1
	// as the exclusive end.
	End   int    `json:"end,omitempty"`
	Value []byte `json:"value,omitempty"`
	// Lease is a lease of the sequence, from 1; one above its grants is a
	// lease nobody granted.
	Lease       int   `json:"lease,omitempty"`
	PrevKV      bool  `json:"prevKV,omitempty"`
	IgnoreValue bool  `json:"ignoreValue,omitempty"`
	IgnoreLease bool  `json:"ignoreLease,omitempty"`
	Rev         int64 `json:"rev,omitempty"`
	Limit       int64 `json:"limit,omitempty"`
	CountOnly   bool  `json:"countOnly,omitempty"`
	KeysOnly    bool  `json:"keysOnly,omitempty"`
	Physical    bool  `json:"physical,omitempty"`
	TTL         int64 `json:"ttl,omitempty"`
	// ID asks for a lease ID in a grant, one of 1 to 4 of the
	// sequence's own: 0 lets the server pick.
	ID      int64      `json:"id,omitempty"`
	Compare []fuzzCmp  `json:"compare,omitempty"`
	Success []fuzzOp   `json:"success,omitempty"`
	Failure []fuzzOp   `json:"failure,omitempty"`
	Watch   *fuzzWatch `json:"watch,omitempty"`
}

// fuzzCmp is one compare of a txn. Number is a relative revision for the
// create and mod targets.
type fuzzCmp struct {
	Key    int    `json:"key"`
	Target string `json:"target"`
	Result string `json:"result"`
	Number int64  `json:"number,omitempty"`
	Value  []byte `json:"value,omitempty"`
	Lease  int    `json:"lease,omitempty"`
}

// fuzzWatch is a watch from a relative revision.
type fuzzWatch struct {
	PrevKV bool `json:"prevKV,omitempty"`
}

// fuzzCase is a sequence as saved: a mismatch, shrunk.
type fuzzCase struct {
	Name string   `json:"name"`
	Ops  []fuzzOp `json:"ops"`
}

// fuzzGen makes random sequences.
type fuzzGen struct {
	rng    *rand.Rand
	grants int
	writes int64
}

func (g *fuzzGen) value() []byte {
	switch g.rng.IntN(10) {
	case 0:
		return nil
	case 1:
		// Bytes past the 64 that a recorded value shows, and every byte
		// value.
		b := make([]byte, 65+g.rng.IntN(200))
		for i := range b {
			b[i] = byte(g.rng.IntN(256))
		}
		return b
	}
	return fmt.Appendf(nil, "v%d", g.rng.IntN(1000))
}

func (g *fuzzGen) end() int {
	switch g.rng.IntN(4) {
	case 0:
		return -1
	case 1:
		return 1 + g.rng.IntN(fuzzKeys+1)
	}
	return 0
}

func (g *fuzzGen) lease() int {
	if g.rng.IntN(3) > 0 {
		return 0
	}
	return 1 + g.rng.IntN(g.grants+1)
}

// rev is a relative revision around the ones the sequence wrote: past,
// current, future, zero and negative.
func (g *fuzzGen) rev() int64 { return g.rng.Int64N(g.writes+4) - 1 }

func (g *fuzzGen) branchOp() fuzzOp {
	switch g.rng.IntN(3) {
	case 0:
		return fuzzOp{Kind: "put", Key: g.rng.IntN(fuzzKeys), Value: g.value(), Lease: g.lease(), PrevKV: g.rng.IntN(2) == 0}
	case 1:
		return fuzzOp{Kind: "del", Key: g.rng.IntN(fuzzKeys), End: g.end(), PrevKV: g.rng.IntN(2) == 0}
	}
	op := fuzzOp{Kind: "get", Key: g.rng.IntN(fuzzKeys), End: g.end(), Limit: g.rng.Int64N(3)}
	if g.rng.IntN(3) == 0 {
		op.Rev = g.rev()
	}
	return op
}

// branch returns n ops of a txn branch. A put of a key that an earlier
// delete of the branch removed becomes a read: after a compaction at that
// txn's revision, etcd v3.7.2 keeps such a put in its index but not its
// values, and its next read of the key stops it with "range failed to find
// revision pair" (seed 3636324239000001190 of the long run on 0281f342).
func (g *fuzzGen) branch(n int) []fuzzOp {
	var out []fuzzOp
	for range n {
		op := g.branchOp()
		if op.Kind == "put" && slices.ContainsFunc(out, func(d fuzzOp) bool { return d.Kind == "del" && deletes(d, op.Key) }) {
			op = fuzzOp{Kind: "get", Key: op.Key}
		}
		out = append(out, op)
	}
	return out
}

// deletes reports whether delete d removes key k.
func deletes(d fuzzOp, k int) bool {
	switch {
	case d.End == -1:
		return k >= d.Key
	case d.End > 0:
		return d.Key <= k && k < d.End-1
	}
	return k == d.Key
}

func (g *fuzzGen) compare() fuzzCmp {
	c := fuzzCmp{Key: g.rng.IntN(fuzzKeys), Result: []string{"EQUAL", "GREATER", "LESS", "NOT_EQUAL"}[g.rng.IntN(4)]}
	switch g.rng.IntN(5) {
	case 0:
		c.Target, c.Number = "VERSION", g.rng.Int64N(4)
	case 1:
		c.Target, c.Number = "CREATE", g.rev()
	case 2:
		c.Target, c.Number = "MOD", g.rev()
	case 3:
		c.Target, c.Value = "VALUE", g.value()
	default:
		c.Target, c.Lease = "LEASE", g.lease()
	}
	return c
}

func (g *fuzzGen) op() fuzzOp {
	k := g.rng.IntN(fuzzKeys)
	var op fuzzOp
	switch p := g.rng.IntN(100); {
	case p < 25:
		op = fuzzOp{Kind: "put", Key: k, Value: g.value(), Lease: g.lease(), PrevKV: g.rng.IntN(2) == 0, IgnoreValue: g.rng.IntN(8) == 0, IgnoreLease: g.rng.IntN(8) == 0}
	case p < 45:
		op = fuzzOp{Kind: "get", Key: k, End: g.end(), Limit: g.rng.Int64N(3), CountOnly: g.rng.IntN(6) == 0, KeysOnly: g.rng.IntN(6) == 0}
		if g.rng.IntN(3) == 0 {
			op.Rev = g.rev()
		}
	case p < 55:
		op = fuzzOp{Kind: "del", Key: k, End: g.end(), PrevKV: g.rng.IntN(2) == 0}
	case p < 80:
		op = fuzzOp{Kind: "txn"}
		for range g.rng.IntN(3) {
			op.Compare = append(op.Compare, g.compare())
		}
		op.Success = g.branch(g.rng.IntN(4))
		op.Failure = g.branch(g.rng.IntN(3))
	case p < 84:
		op = fuzzOp{Kind: "compact", Rev: g.rev(), Physical: g.rng.IntN(2) == 0}
	case p < 90:
		op = fuzzOp{Kind: "grant", TTL: 300 + g.rng.Int64N(300)}
		if g.rng.IntN(3) == 0 {
			op.ID = 1 + g.rng.Int64N(4)
		}
		g.grants++
	case p < 94:
		op = fuzzOp{Kind: "revoke", Lease: 1 + g.rng.IntN(g.grants+1)}
	default:
		op = fuzzOp{Kind: "watch", Key: k, End: g.end(), Rev: g.rev(), Watch: &fuzzWatch{PrevKV: g.rng.IntN(2) == 0}}
	}
	if op.Kind != "get" && op.Kind != "watch" && op.Kind != "grant" {
		g.writes++
	}
	return op
}

// fuzzRun runs a sequence on one server.
type fuzzRun struct {
	*run
	tag int64
	// leases are the IDs the server granted, in order.
	leases []int64
}

func (f *fuzzRun) key(i int) []byte { return f.run.key(fmt.Sprintf("f/k%d", i)) }

func (f *fuzzRun) end(op fuzzOp) []byte {
	switch {
	case op.End == -1:
		return []byte{0}
	case op.End > 0:
		return f.key(op.End - 1)
	}
	return nil
}

func (f *fuzzRun) lease(n int) int64 {
	switch {
	case n == 0:
		return 0
	case n <= len(f.leases):
		return f.leases[n-1]
	}
	// A lease nobody granted.
	return 1<<40 + int64(n)
}

func (f *fuzzRun) rev(rel int64) int64 {
	if rel <= 0 {
		return rel
	}
	return f.abs(rel)
}

func (f *fuzzRun) requestOp(op fuzzOp) *pb.RequestOp {
	switch op.Kind {
	case "put":
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: f.put(op)}}
	case "del":
		return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: f.del(op)}}
	}
	return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: f.get(op)}}
}

func (f *fuzzRun) put(op fuzzOp) *pb.PutRequest {
	return &pb.PutRequest{Key: f.key(op.Key), Value: op.Value, Lease: f.lease(op.Lease), PrevKv: op.PrevKV, IgnoreValue: op.IgnoreValue, IgnoreLease: op.IgnoreLease}
}

func (f *fuzzRun) del(op fuzzOp) *pb.DeleteRangeRequest {
	return &pb.DeleteRangeRequest{Key: f.key(op.Key), RangeEnd: f.end(op), PrevKv: op.PrevKV}
}

func (f *fuzzRun) get(op fuzzOp) *pb.RangeRequest {
	return &pb.RangeRequest{Key: f.key(op.Key), RangeEnd: f.end(op), Revision: f.rev(op.Rev), Limit: op.Limit, CountOnly: op.CountOnly, KeysOnly: op.KeysOnly}
}

func (f *fuzzRun) compare(c fuzzCmp) *pb.Compare {
	cmp := &pb.Compare{Key: f.key(c.Key), Result: pb.Compare_CompareResult(pb.Compare_CompareResult_value[c.Result])}
	switch c.Target {
	case "VERSION":
		cmp.Target, cmp.TargetUnion = pb.Compare_VERSION, &pb.Compare_Version{Version: c.Number}
	case "CREATE":
		cmp.Target, cmp.TargetUnion = pb.Compare_CREATE, &pb.Compare_CreateRevision{CreateRevision: f.rev(c.Number)}
	case "MOD":
		cmp.Target, cmp.TargetUnion = pb.Compare_MOD, &pb.Compare_ModRevision{ModRevision: f.rev(c.Number)}
	case "VALUE":
		cmp.Target, cmp.TargetUnion = pb.Compare_VALUE, &pb.Compare_Value{Value: c.Value}
	default:
		cmp.Target, cmp.TargetUnion = pb.Compare_LEASE, &pb.Compare_Lease{Lease: f.lease(c.Lease)}
	}
	return cmp
}

// do runs one op and records its answer.
func (f *fuzzRun) do(i int, op fuzzOp) {
	f.t.Helper()
	name := fmt.Sprintf("%d %s", i, op.Kind)
	switch op.Kind {
	case "put":
		f.run.put(name, f.put(op))
	case "get":
		f.run.get(name, f.get(op))
	case "del":
		f.run.del(name, f.del(op))
	case "txn":
		req := &pb.TxnRequest{}
		for _, c := range op.Compare {
			req.Compare = append(req.Compare, f.compare(c))
		}
		for _, o := range op.Success {
			req.Success = append(req.Success, f.requestOp(o))
		}
		for _, o := range op.Failure {
			req.Failure = append(req.Failure, f.requestOp(o))
		}
		f.txn(name, req)
	case "compact":
		f.compact(name, f.rev(op.Rev), op.Physical)
	case "grant":
		id := op.ID
		if id != 0 {
			// The same on both servers, and on no other sequence.
			id += f.tag * 8
		}
		if id := f.grant(name, id, op.TTL); id != 0 {
			f.leases = append(f.leases, id)
		}
	case "revoke":
		f.revoke(name, f.lease(op.Lease))
	case "watch":
		f.watch(name, &pb.WatchCreateRequest{Key: f.key(op.Key), RangeEnd: f.end(op), StartRevision: f.rev(op.Rev), PrevKv: op.Watch.PrevKV})
	default:
		f.t.Fatalf("unknown fuzz op %q", op.Kind)
	}
}

// fuzzPair is etcd and tigerstore, side by side.
type fuzzPair struct {
	t          *testing.T
	etcd, ours *client
	tag        int64
}

// diff runs ops on both servers under fresh keys and returns the first
// step where their answers differ, or "".
func (p *fuzzPair) diff(ops []fuzzOp) string {
	p.t.Helper()
	p.tag++
	var steps [2][]step
	var truth func(key string, rel int64) *kv
	for k, c := range []*client{p.etcd, p.ours} {
		f := &fuzzRun{run: newRun(p.t, c, p.tag, "fuzz"), tag: p.tag}
		f.exact = true
		var h *keyHistory
		if k == 0 {
			h = watchHistory(f.run, f.base+1)
		}
		for i, op := range ops {
			f.do(i, op)
		}
		steps[k] = f.steps
		if h != nil {
			h.finish(f.run)
			truth = func(key string, rel int64) *kv { return f.kv(h.before(f.run.key(key), f.abs(rel))) }
		}
		// A lease left to expire would delete its keys on each server at
		// its own time, during a later sequence, and move that server's
		// revision under it.
		for _, id := range f.leases {
			if _, err := c.lease.LeaseRevoke(f.ctx, &pb.LeaseRevokeRequest{ID: id}); err != nil && status.Code(err) != codes.NotFound {
				p.t.Fatalf("revoke lease %d after the sequence: %v", id, err)
			}
		}
	}
	for i := range steps[0] {
		if !sameStep(steps[0][i], steps[1][i]) && !stricterWatch(ops, steps[0], truth, steps[0][i], steps[1][i]) {
			e, _ := json.Marshal(steps[0][i])
			o, _ := json.Marshal(steps[1][i])
			return fmt.Sprintf("step %s: etcd v3.7.2 answers %s, tigerstore %s", steps[0][i].Do, e, o)
		}
	}
	return ""
}

// stricterWatch reports whether tigerstore's answer to a watch differs
// from etcd's only as the stricter rule of the boundary tests allows (see
// stricter). After a compaction at C, etcd sends the update at C without
// its previous value, as the version before C is compacted; tigerstore
// must send it, or cancel the watch as compacted at C. So for a watch with
// previous values that starts at a revision the sequence compacted
// (etcd's steps say which): tigerstore may cancel it as compacted there,
// or add to an update or delete at the start revision the previous value
// that etcd leaves out, which must be the key's version just before, as
// etcd's own history of the sequence (truth) shows. Everything else must
// be the same.
func stricterWatch(ops []fuzzOp, etcdSteps []step, truth func(key string, rel int64) *kv, etcd, ours step) bool {
	var i int
	if _, err := fmt.Sscanf(etcd.Do, "%d watch", &i); err != nil || i >= len(ops) || ops[i].Watch == nil || !ops[i].Watch.PrevKV {
		return false
	}
	start := ops[i].Rev
	compacted := false
	for k, op := range ops[:i] {
		compacted = compacted || op.Kind == "compact" && op.Rev == start && k < len(etcdSteps) && etcdSteps[k].Code == "OK"
	}
	if !compacted {
		return false
	}
	type event struct {
		Type   string `json:"type"`
		KV     *kv    `json:"kv"`
		PrevKV *kv    `json:"prevKV,omitempty"`
	}
	type watch struct {
		Created         bool     `json:"created"`
		CreatedRevision int64    `json:"createdRevision,omitempty"`
		Canceled        bool     `json:"canceled,omitempty"`
		CancelRevision  int64    `json:"cancelRevision,omitempty"`
		CompactRevision int64    `json:"compactRevision,omitempty"`
		CancelReason    string   `json:"cancelReason,omitempty"`
		Events          []event  `json:"events,omitempty"`
		Problems        []string `json:"problems,omitempty"`
	}
	var e, o watch
	if etcd.Code != "OK" || ours.Code != "OK" || json.Unmarshal(etcd.Result, &e) != nil || json.Unmarshal(ours.Result, &o) != nil {
		return false
	}
	if o.Canceled {
		// The canceled answer the boundary tests allow, and nothing else.
		return o.Created && o.CreatedRevision == e.CreatedRevision && o.CompactRevision == start && o.CancelRevision == 0 &&
			o.CancelReason == "" && len(o.Events) == 0 && len(o.Problems) == 0
	}
	if e.Canceled || e.Created != o.Created || e.CreatedRevision != o.CreatedRevision || !slices.Equal(e.Problems, o.Problems) || len(e.Events) != len(o.Events) {
		return false
	}
	added := false
	for k := range e.Events {
		x, y := e.Events[k], o.Events[k]
		if x.Type != y.Type || !samePtr(x.KV, y.KV) {
			return false
		}
		if samePtr(x.PrevKV, y.PrevKV) {
			continue
		}
		// Only the value etcd compacted away may be added.
		update := x.Type == "DELETE" || x.KV.Version > 1
		if x.PrevKV != nil || x.KV.Mod != start || !update || !samePtr(y.PrevKV, truth(x.KV.Key, start)) || y.PrevKV == nil {
			return false
		}
		added = true
	}
	return added
}

// keyHistory is every change to a sequence's keys on etcd, from a watch
// opened before the sequence: the truth that a previous value etcd
// compacted away must match.
type keyHistory struct {
	mu     sync.Mutex
	events []*mvccpb.Event
	header int64
	err    error
	stream pb.Watch_WatchClient
	done   chan struct{}
}

func watchHistory(r *run, from int64) *keyHistory {
	r.t.Helper()
	stream, err := r.c.watch.Watch(r.ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	prefix := []byte(r.prefix)
	if err := stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: prefix, RangeEnd: prefixEnd(prefix), StartRevision: from,
	}}}); err != nil {
		r.t.Fatal(err)
	}
	h := &keyHistory{stream: stream, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		for {
			resp, err := stream.Recv()
			h.mu.Lock()
			if err != nil {
				h.err = err
				h.mu.Unlock()
				return
			}
			h.events = append(h.events, resp.Events...)
			h.header = max(h.header, resp.Header.GetRevision())
			h.mu.Unlock()
		}
	}()
	return h
}

// finish waits until the watch reached the current revision and ends it.
func (h *keyHistory) finish(r *run) {
	r.t.Helper()
	cur, err := r.c.kv.Range(r.ctx, &pb.RangeRequest{Key: []byte(r.prefix)})
	if err != nil {
		r.t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		h.mu.Lock()
		reached, err := h.header >= cur.Header.Revision, h.err
		h.mu.Unlock()
		if reached {
			break
		}
		if err != nil || time.Now().After(deadline) {
			r.t.Fatalf("etcd's history watch did not reach revision %d: %v", cur.Header.Revision, err)
		}
		if err := h.stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
			r.t.Fatal(err)
		}
	}
	_ = h.stream.CloseSend()
}

// before is key as it was just before revision rev, nil if absent.
func (h *keyHistory) before(key []byte, rev int64) *mvccpb.KeyValue {
	h.mu.Lock()
	defer h.mu.Unlock()
	var last *mvccpb.KeyValue
	for _, e := range h.events {
		if string(e.Kv.Key) != string(key) || e.Kv.ModRevision >= rev {
			continue
		}
		if e.Type == mvccpb.DELETE {
			last = nil
		} else {
			last = e.Kv
		}
	}
	return last
}

func samePtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// shrink removes ops from a sequence that has a mismatch, ever smaller
// runs of them, while one remains, up to 300 attempts.
func (p *fuzzPair) shrink(ops []fuzzOp) ([]fuzzOp, string) {
	p.t.Helper()
	why := p.diff(ops)
	attempts := 0
	for chunk := len(ops) / 2; chunk >= 1; chunk /= 2 {
		for i := 0; i+chunk <= len(ops) && attempts < 300; attempts++ {
			cand := slices.Concat(ops[:i], ops[i+chunk:])
			if w := p.diff(cand); w != "" {
				ops, why = cand, w
				continue
			}
			i += chunk
		}
	}
	return ops, why
}

func newFuzzPair(t *testing.T) *fuzzPair {
	t.Helper()
	ca := os.Getenv("TIGERSTORE_DIFF_CA")
	return &fuzzPair{t: t, etcd: dialAt(t, "TIGERSTORE_DIFF_ETCD", ca), ours: dialAt(t, "TIGERSTORE_DIFF_ENDPOINT", ca), tag: time.Now().UnixNano() % 1e9 * 1000}
}

// TestDiffFuzz sends random sequences of puts, reads at current and past
// revisions, deletes, txns with every compare target, compactions, lease
// grants and revokes and watches to etcd v3.7.2 and tigerstore, each
// sequence under keys of its own, and compares every answer as the
// difftest records it: revisions relative to the sequence's start, lease
// IDs by grant order, exact value bytes, txn results and errors. A
// mismatch is shrunk and saved as a named case in -difffuzz.out, which
// TestDiffFuzzFound runs from then on.
func TestDiffFuzz(t *testing.T) {
	p := newFuzzPair(t)
	seed := *fuzzSeed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("first seed %d", seed)
	// A step that fails, as when a server stops answering, ends the test
	// without naming the sequence.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the sequence it ran last was seed %d", seed)
		}
	})
	n := 0
	for deadline := time.Now().Add(*fuzzDuration); time.Now().Before(deadline); seed++ {
		g := &fuzzGen{rng: rand.New(rand.NewPCG(seed, 148))}
		ops := make([]fuzzOp, 30)
		for i := range ops {
			ops[i] = g.op()
		}
		n++
		if p.diff(ops) == "" {
			continue
		}
		ops, why := p.shrink(ops)
		c := fuzzCase{Name: fmt.Sprintf("seed-%d", seed), Ops: ops}
		b, err := json.MarshalIndent(c, "", "\t")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(*fuzzOut, c.Name+".json")
		if err := os.MkdirAll(*fuzzOut, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("seed %d differs from etcd v3.7.2 after %d sequences; shrunk to %d ops in %s: %s", seed, n, len(ops), path, why)
	}
	t.Logf("%d sequences of 30 ops, seeds to %d, answered as etcd v3.7.2 does", n, seed-1)
}

// TestDiffFuzzFound runs every saved mismatch of TestDiffFuzz against
// both servers; each must now get etcd's answers.
func TestDiffFuzzFound(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(*fuzzOut, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	p := newFuzzPair(t)
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var c fuzzCase
		if err := json.Unmarshal(b, &c); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		t.Run(strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			p.t = t
			if why := p.diff(c.Ops); why != "" {
				t.Fatal(why)
			}
		})
	}
}

// TestDiffFuzzWatchRule checks stricterWatch on a sequence that puts a
// key twice, compacts at the second put and watches from there with
// previous values: etcd sends the update without its previous value.
// tigerstore may add exactly the version before, or cancel the watch as
// compacted at the start; a wrong value or lease, a cancel with other
// metadata, or a sequence that never compacted there, may not.
func TestDiffFuzzWatchRule(t *testing.T) {
	ops := []fuzzOp{
		{Kind: "put", Key: 0, Value: []byte("original")},
		{Kind: "put", Key: 0, Value: []byte("new")},
		{Kind: "compact", Rev: 2},
		{Kind: "watch", Key: 0, Rev: 2, Watch: &fuzzWatch{PrevKV: true}},
	}
	etcdSteps := []step{{Do: "0 put", Code: "OK"}, {Do: "1 put", Code: "OK"}, {Do: "2 compact", Code: "OK"}}
	original := &kv{Key: "f/k0", Value: `"original"`, Create: 1, Mod: 1, Version: 1}
	truth := func(key string, rel int64) *kv {
		if key == "f/k0" && rel == 2 {
			return original
		}
		return nil
	}
	watch := func(result string) step { return step{Do: "3 watch", Code: "OK", Result: json.RawMessage(result)} }
	update := `{"type":"PUT","kv":{"key":"f/k0","value":"\"new\"","create":1,"mod":2,"version":2}`
	etcd := watch(`{"created":true,"createdRevision":2,"events":[` + update + `}]}`)
	cases := []struct {
		name  string
		ours  string
		steps []step
		want  bool
	}{
		{"the version before", `{"created":true,"createdRevision":2,"events":[` + update + `,"prevKV":{"key":"f/k0","value":"\"original\"","create":1,"mod":1,"version":1}}]}`, etcdSteps, true},
		{"cancel as compacted", `{"created":true,"createdRevision":2,"canceled":true,"compactRevision":2}`, etcdSteps, true},
		{"another value", `{"created":true,"createdRevision":2,"events":[` + update + `,"prevKV":{"key":"f/k0","value":"\"CORRUPTED\"","create":1,"mod":1,"version":1}}]}`, etcdSteps, false},
		{"another lease", `{"created":true,"createdRevision":2,"events":[` + update + `,"prevKV":{"key":"f/k0","value":"\"original\"","create":1,"mod":1,"version":1,"lease":99}}]}`, etcdSteps, false},
		{"cancel at another created revision", `{"created":true,"createdRevision":999,"canceled":true,"compactRevision":2}`, etcdSteps, false},
		{"cancel with a reason", `{"created":true,"createdRevision":2,"canceled":true,"compactRevision":2,"cancelReason":"other"}`, etcdSteps, false},
		{"no compaction there", `{"created":true,"createdRevision":2,"canceled":true,"compactRevision":2}`, []step{etcdSteps[0], etcdSteps[1], {Do: "2 compact", Code: "OutOfRange"}}, false},
	}
	for _, c := range cases {
		if got := stricterWatch(ops, c.steps, truth, etcd, watch(c.ours)); got != c.want {
			t.Errorf("%s: stricterWatch says %v, want %v", c.name, got, c.want)
		}
	}
}

// TestDiffFuzzNoPutAfterDelete checks that no generated txn branch puts a key
// an earlier delete of it removed, which etcd v3.7.2 does not survive
// after a compaction at that revision.
func TestDiffFuzzNoPutAfterDelete(t *testing.T) {
	if !deletes(fuzzOp{Kind: "del", Key: 1, End: -1}, 4) || deletes(fuzzOp{Kind: "del", Key: 1, End: 3}, 2) || !deletes(fuzzOp{Kind: "del", Key: 1, End: 3}, 1) || deletes(fuzzOp{Kind: "del", Key: 1}, 2) {
		t.Fatal("deletes covers the wrong keys")
	}
	for seed := range uint64(3000) {
		g := &fuzzGen{rng: rand.New(rand.NewPCG(seed, 148))}
		for range 30 {
			op := g.op()
			for _, branch := range [][]fuzzOp{op.Success, op.Failure} {
				for i, o := range branch {
					if o.Kind == "put" && slices.ContainsFunc(branch[:i], func(d fuzzOp) bool { return d.Kind == "del" && deletes(d, o.Key) }) {
						t.Fatalf("seed %d: a branch puts k%d after deleting it: %+v", seed, o.Key, branch)
					}
				}
			}
		}
	}
}
