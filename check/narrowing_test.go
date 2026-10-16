package check

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// narrowingHistory runs random puts, deletes and reads on three keys, one
// at a time in order, from four clients whose calls overlap their
// neighbours'. A quarter of the writes time out; half of those took
// effect. It returns the history with an Effect on each that did, as a
// log places it, and the same history without them.
func narrowingHistory(rng *rand.Rand) (narrowed, plain *history.History) {
	keys := []string{"a", "b", "c"}
	st := newState(keys)
	h := &history.History{}
	for i := range 30 {
		at := int64(i+1) * 10
		key := keys[rng.IntN(len(keys))]
		var req history.Request
		switch rng.IntN(3) {
		case 0:
			req.Put = &history.PutRequest{Key: key, Value: fmt.Sprintf("v%d", i), PrevKV: true}
		case 1:
			req.Delete = &history.DeleteRequest{Key: key, PrevKV: true}
		default:
			req.Range = &history.RangeRequest{Key: key}
		}
		op := history.Op{Client: i % 4, Call: at - rng.Int64N(16), Return: at + rng.Int64N(16), Req: req}
		if req.Mutates() && rng.IntN(4) == 0 {
			op.Err = &history.Error{Code: "Unavailable", Message: "timed out", Ambiguous: true}
			if rng.IntN(2) == 0 {
				st.step(req, false)
				op.Effect = &history.Effect{By: at + 1, Revision: st.rev}
			}
		} else {
			o := st.step(req, false)
			op.Resp, op.Err = o.resp, o.err
		}
		h.Ops = append(h.Ops, op)
	}
	p := *h
	p.Ops = append([]history.Op(nil), h.Ops...)
	for i := range p.Ops {
		p.Ops[i].Effect = nil
	}
	return h, &p
}

// staleRead makes one successful read of h answer with an older value of
// its key, from a put before it, and reports whether there was one.
func staleRead(rng *rand.Rand, h *history.History) bool {
	start := rng.IntN(len(h.Ops))
	for j := range h.Ops {
		i := (start + j) % len(h.Ops)
		op := &h.Ops[i]
		if op.Req.Range == nil || op.Resp == nil || len(op.Resp.Range.KVs) == 0 {
			continue
		}
		for k := i - 1; k >= 0; k-- {
			p := h.Ops[k]
			if p.Req.Put == nil || p.Req.Put.Key != op.Req.Range.Key || p.Resp == nil || p.Req.Put.Value == op.Resp.Range.KVs[0].Value {
				continue
			}
			resp := *op.Resp
			rr := *resp.Range
			rr.KVs = []history.KeyValue{{Key: p.Req.Put.Key, Value: p.Req.Put.Value, CreateRevision: rr.KVs[0].CreateRevision, ModRevision: p.Resp.Revision, Version: rr.KVs[0].Version}}
			resp.Range = &rr
			op.Resp = &resp
			return true
		}
	}
	return false
}

// TestNarrowingNeverAccepts is #159's independent check of the narrowing
// #153 added: Linearizable fixes an ambiguous write that the log shows took
// effect to that revision and to a time by which it did. Given a true
// Effect, that only takes linearizations away. On random histories of a
// sequential run, every true history passes with and without the Effects,
// and of the copies with a stale read, the check with Effects passes none
// that the check without them rejects.
func TestNarrowingNeverAccepts(t *testing.T) {
	rng := rand.New(rand.NewPCG(159, 153))
	rejected := 0
	for i := range 120 {
		narrowed, plain := narrowingHistory(rng)
		if err := Linearizable(narrowed, 10*time.Second); err != nil {
			t.Fatalf("history %d with Effects: %v", i, err)
		}
		if err := Linearizable(plain, 10*time.Second); err != nil {
			t.Fatalf("history %d without Effects: %v", i, err)
		}
		k := rng.IntN(len(narrowed.Ops))
		mutated := rand.New(rand.NewPCG(uint64(i), uint64(k)))
		if !staleRead(mutated, narrowed) {
			continue
		}
		staleRead(rand.New(rand.NewPCG(uint64(i), uint64(k))), plain)
		withEffects := Linearizable(narrowed, 10*time.Second)
		without := Linearizable(plain, 10*time.Second)
		if withEffects == nil && without != nil {
			t.Fatalf("history %d: a stale read passes with Effects but not without: %v", i, without)
		}
		if withEffects != nil {
			rejected++
		}
	}
	if rejected < 40 {
		t.Fatalf("only %d histories with a stale read were rejected", rejected)
	}
	t.Logf("%d histories with a stale read rejected", rejected)
}
