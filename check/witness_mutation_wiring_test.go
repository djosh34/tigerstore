package check

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func mutationPutHistory(t *testing.T, n int) (*history.History, history.Request) {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	b := newBuilder(t, keys...)
	txn := &history.TxnRequest{}
	for _, key := range keys {
		txn.Success = append(txn.Success, history.TxnOp{Put: &history.PutRequest{Key: key, Value: "v"}})
	}
	req := history.Request{Txn: txn}
	put := b.do(0, req)
	b.do(0, history.Request{Range: &history.RangeRequest{End: history.AllKeys, CountOnly: true}})
	h := b.done()
	h.Ops[put].Resp, h.Ops[put].Err = nil, errUnavailable
	return h, req
}

func TestWitnessPublicMutationWiring(t *testing.T) {
	h, _ := mutationPutHistory(t, 1100)
	var used int64
	if err := Witnessed(h, FromRevisions, time.Minute, StepsUsed(&used)); err != nil {
		t.Fatal(err)
	}
	if used != 1133 {
		t.Fatalf("public large-map put transaction used %d steps, want 1133", used)
	}
	if err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(1132)); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("public mutation/rollback budget was bypassed: %v", err)
	}
}

func TestWitnessPutMutationProductionWiring(t *testing.T) {
	for _, path := range []string{"trial", "stateAt", "scopedState"} {
		t.Run(path, func(t *testing.T) {
			h, req := mutationPutHistory(t, 100)
			w := newWitness(h, time.Now().Add(time.Minute))
			if err := w.fromRevisions(); err != nil {
				t.Fatal(err)
			}
			var s *state
			undo := func() {}
			switch path {
			case "trial":
				s, undo = w.search.try(w.live, req)
			case "stateAt":
				s = w.stateAt(0)
			case "scopedState":
				s = w.scopedState(0, 0)
			}
			before := w.steps
			o := s.step(req, true)
			undo()
			if o.err != nil || len(o.events) != 100 || w.steps-before < 46 {
				t.Fatalf("production %s lost put mutation charging: error %v, %d events, %d steps", path, o.err, len(o.events), w.steps-before)
			}
		})
	}
}
