package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// The differential test of the fast path (fast.go, #602): the same
// requests go to two one-node clusters, one that takes them on the fast
// path where it may and one that takes every request through Submit. The
// replies, the revisions, the entries applied and the stores must be the
// same, byte for byte.

// choices is where a differential run takes its choices from: a seeded
// generator, or the bytes of a fuzz input.
type choices interface {
	// intn returns a number in [0, n).
	intn(n int) int
}

type seeded struct{ r *rand.Rand }

func (s seeded) intn(n int) int { return s.r.IntN(n) }

// fuzzed takes its choices from bytes, and 0 once they ran out.
type fuzzed struct{ b []byte }

func (f *fuzzed) intn(n int) int {
	if len(f.b) == 0 {
		return 0
	}
	v := int(f.b[0])
	f.b = f.b[1:]
	if n > 256 && len(f.b) > 0 {
		v = v<<8 | int(f.b[0])
		f.b = f.b[1:]
	}
	return v % n
}

// fastAnswers collects the fast path's replies, encoded as they come.
type fastAnswers struct{ got map[uint64]string }

func (a *fastAnswers) Answer(r *Reply) { a.got[r.ID] = canonReply(r) }

// canonReply encodes a reply for comparison, while the values it points
// to are still the ones the turn handed out.
func canonReply(r *Reply) string {
	var b strings.Builder
	fmt.Fprintf(&b, "id %d err %v rev %d term %d more %v", r.ID, r.Err, r.Rev, r.Term, r.MorePieces)
	det := proto.MarshalOptions{Deterministic: true}
	if r.PrevKv != nil {
		m, _ := det.Marshal(r.PrevKv)
		fmt.Fprintf(&b, " prev %x", m)
	}
	if r.Resp != nil {
		m, _ := det.Marshal(r.Resp)
		fmt.Fprintf(&b, " resp %T %x", r.Resp, m)
	}
	for _, kv := range r.Range.KVs {
		fmt.Fprintf(&b, " kv %q %q %d %d %d %d", kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
	}
	fmt.Fprintf(&b, " rests %d", len(r.Rests))
	return b.String()
}

// diffNode is one side of a differential run.
type diffNode struct {
	s       *single
	fast    bool
	answers fastAnswers
	replies map[uint64]string
	applied [][]byte
	taken   int
}

func newDiffNode(t testing.TB, fast bool) *diffNode {
	d := &diffNode{fast: fast, answers: fastAnswers{got: map[uint64]string{}}, replies: map[uint64]string{}}
	d.s = startSingleWith(t, simfs.New(), 7, func(c *Config) {
		c.Record = true
		c.Slow = !fast
	})
	d.s.afterTurn = func(eff Effects) {
		for i := range eff.Replies {
			d.replies[eff.Replies[i].ID] = canonReply(&eff.Replies[i])
		}
		for _, a := range eff.Applied {
			d.applied = append(d.applied, bytes.Clone(a.Data))
		}
	}
	// Short transactions and the fast path need originVersion, which the
	// node announces in its first ticks.
	for range 100 {
		if d.s.n.ActiveVersion() >= originVersion {
			break
		}
		d.s.n.Tick()
		d.s.run()
	}
	if v := d.s.n.ActiveVersion(); v < originVersion {
		t.Fatalf("the node runs version %d", v)
	}
	// What the start applied is the same on both sides.
	d.applied = nil
	return d
}

// submit hands the node a request: on the fast path where it takes it.
func (d *diffNode) submit(r Request) {
	if d.fast && d.s.n.SubmitFast(&r, &d.answers) {
		d.taken++
		return
	}
	d.s.n.Submit(r)
}

// genReq is a request the generator made, which each side decodes from
// the client's encoding for itself, as the server does.
type genReq struct {
	op   proto.Message // nil for a put in wire form
	wire []byte
	// fastShape says that the fast path takes it whenever nothing rare
	// is going on: a turn of several requests holds only these, so both
	// sides put them in one entry in the same order.
	fastShape bool
}

func (g genReq) request(t testing.TB, id uint64) Request {
	t.Helper()
	r := Request{ID: id}
	switch op := g.op.(type) {
	case nil:
		put, err := ParsePut(g.wire)
		if err != nil {
			t.Fatal(err)
		}
		r.Put = put
	case *pb.TxnRequest:
		r.Wire, r.Op = g.wire, new(pb.TxnRequest)
		if err := DecodeTxn(&r, nil); err != nil {
			t.Fatal(err)
		}
	default:
		r.Op = proto.Clone(op)
	}
	return r
}

func genKey(c choices) []byte { return []byte(fmt.Sprintf("/k/%d", c.intn(6))) }

func genValue(c choices) []byte {
	v := make([]byte, c.intn(40))
	for i := range v {
		v[i] = byte('a' + c.intn(26))
	}
	return v
}

// genWrite makes a request; with fastOnly, one of a fast shape.
func genWrite(t testing.TB, c choices, fastOnly bool) genReq {
	t.Helper()
	k := 4
	if !fastOnly {
		k = 9
	}
	switch c.intn(k) {
	case 0, 1:
		p := &pb.PutRequest{Key: genKey(c), Value: genValue(c), PrevKv: c.intn(2) == 0}
		if !fastOnly && c.intn(4) == 0 {
			// A put the checks refuse, or one with a lease no node granted.
			switch c.intn(3) {
			case 0:
				p.Key = nil
			case 1:
				p.Lease = 99
			default:
				p.IgnoreValue = true
			}
		}
		w, _ := proto.Marshal(p)
		return genReq{wire: w, fastShape: p.Lease == 0 && len(p.Key) > 0 && !p.IgnoreValue}
	case 2, 3:
		key := genKey(c)
		cmp := &pb.Compare{Key: key, Result: pb.Compare_CompareResult(c.intn(4)), Target: pb.Compare_CompareTarget(c.intn(3))}
		switch cmp.Target {
		case pb.Compare_VERSION:
			cmp.TargetUnion = &pb.Compare_Version{Version: int64(c.intn(4))}
		case pb.Compare_CREATE:
			cmp.TargetUnion = &pb.Compare_CreateRevision{CreateRevision: int64(c.intn(30))}
		default:
			cmp.TargetUnion = &pb.Compare_ModRevision{ModRevision: int64(c.intn(30))}
		}
		txn := &pb.TxnRequest{Compare: []*pb.Compare{cmp}}
		if c.intn(4) == 0 {
			txn.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: key, PrevKv: c.intn(2) == 0}}}}
		} else {
			txn.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key, Value: genValue(c), PrevKv: c.intn(2) == 0}}}}
		}
		if c.intn(2) == 0 {
			txn.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key}}}}
		}
		w, _ := proto.Marshal(txn)
		return genReq{op: txn, wire: w, fastShape: true}
	case 4:
		// A transaction of another shape: two puts.
		txn := &pb.TxnRequest{Success: []*pb.RequestOp{
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: genKey(c), Value: genValue(c)}}},
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("/other"), Value: genValue(c)}}},
		}}
		w, _ := proto.Marshal(txn)
		return genReq{op: txn, wire: w}
	case 5:
		return genReq{op: &pb.DeleteRangeRequest{Key: genKey(c), PrevKv: c.intn(2) == 0}}
	case 6:
		return genReq{op: &pb.RangeRequest{Key: []byte("/k/"), RangeEnd: []byte("/k0")}}
	case 7:
		return genReq{op: &pb.CompactionRequest{Revision: int64(c.intn(20))}}
	default:
		return genReq{op: &pb.LeaseGrantRequest{TTL: 60, ID: int64(1 + c.intn(3))}}
	}
}

// diffRun runs turns of generated requests on both sides and compares
// them after each turn.
func diffRun(t testing.TB, c choices, turns int) (taken int) {
	t.Helper()
	fast, slow := newDiffNode(t, true), newDiffNode(t, false)
	id := uint64(1000)
	var ids []uint64
	for turn := range turns {
		k := 1
		if c.intn(2) == 0 {
			k += c.intn(6)
		}
		for i := range k {
			g := genWrite(t, c, k > 1)
			id++
			ids = append(ids, id)
			fast.submit(g.request(t, id))
			slow.submit(g.request(t, id))
			_ = i
		}
		fast.s.run()
		slow.s.run()
		for _, id := range ids {
			got, ok := fast.answers.got[id]
			if !ok {
				got, ok = fast.replies[id]
			}
			want, wok := slow.replies[id]
			if !ok || !wok || got != want {
				t.Fatalf("turn %d: request %d is answered\n  on the fast side: %q (%v)\n  on the slow side: %q (%v)", turn, id, got, ok, want, wok)
			}
		}
		ids = ids[:0]
		if f, s := fast.s.n.state.Hash(), slow.s.n.state.Hash(); f != s {
			t.Fatalf("turn %d: the states' hashes are %x on the fast side, %x on the slow side", turn, f, s)
		}
		if f, s := fast.s.n.kv.Rev(), slow.s.n.kv.Rev(); f != s {
			t.Fatalf("turn %d: the revisions are %d on the fast side, %d on the slow side", turn, f, s)
		}
		if !slices.EqualFunc(fast.applied, slow.applied, bytes.Equal) {
			t.Fatalf("turn %d: the entries applied differ:\n  fast %x\n  slow %x", turn, fast.applied, slow.applied)
		}
	}
	return fast.taken
}

// TestFastPathMatchesSlowPath runs seeds of generated requests through
// both paths.
func TestFastPathMatchesSlowPath(t *testing.T) {
	taken := 0
	for seed := range uint64(20) {
		taken += diffRun(t, seeded{rand.New(rand.NewPCG(seed, 0x66617374))}, 60)
	}
	if taken < 200 {
		t.Fatalf("the fast path took only %d requests", taken)
	}
}

// FuzzFastPath is the differential test with the fuzzer's choices.
func FuzzFastPath(f *testing.F) {
	for seed := range uint64(4) {
		r := rand.New(rand.NewPCG(seed, 1))
		b := binary.LittleEndian.AppendUint64(nil, r.Uint64())
		for range 64 {
			b = append(b, byte(r.UintN(256)))
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		diffRun(t, &fuzzed{b: b}, 1+len(b)/16)
	})
}
