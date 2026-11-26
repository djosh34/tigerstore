package check

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestMutationWeightAndBudget(t *testing.T) {
	r := dueShape(0)
	for range 1000 {
		if r.mutationStep(8) {
			t.Fatal("small map stopped")
		}
	}
	if r.w.steps != 0 || r.w.workTicks != 0 {
		t.Fatal("small-map writes lost their existing scan-only accounting")
	}
	for range 1000 {
		if r.mutationStep(9) {
			t.Fatal("unbounded large map stopped")
		}
	}
	if r.w.steps != 500 || r.w.workTicks != 16 {
		t.Fatalf("large-map weight: steps %d, ticks %d, want 500 and 16", r.w.steps, r.w.workTicks)
	}
	r = dueShape(0)
	r.w.maxSteps = 3
	for range 1000 {
		if r.mutationStep(9) {
			break
		}
	}
	if !r.w.stopped || r.w.steps != 4 || !r.mutationStep(0) {
		t.Fatalf("mutation bypassed/forgot stop: %v, %d steps", r.w.stopped, r.w.steps)
	}
}

func TestMutationWeightInReplay(t *testing.T) {
	for _, budget := range []int64{0, 3} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			start := &history.Start{Rev: 1}
			for i := range 100 {
				start.KVs = append(start.KVs, history.KeyValue{Key: fmt.Sprintf("k%03d", i), Version: 1, CreateRevision: 1, ModRevision: 1})
			}
			w := newWitness(&history.History{Start: start}, time.Now().Add(time.Minute))
			r := newRevisionOrder(w)
			w.search, w.maxSteps = r, budget
			w.live.stop, w.live.mutate = r.step, r.mutationStep
			o := w.live.step(history.Request{Delete: &history.DeleteRequest{End: history.AllKeys}}, true)
			if budget == 0 {
				if o.err != nil || o.resp.Delete.Deleted != 100 || w.steps < 46 || w.steps > 47 {
					t.Fatalf("real large-map replay was not charged: %v, %d steps", o, w.steps)
				}
			} else if o.err != errReplayStopped || !w.stopped || w.steps != budget+1 {
				t.Fatalf("replay escaped mutation budget: %v, stopped %v, %d steps", o, w.stopped, w.steps)
			}
		})
	}
}

func TestMutationWeightKeepsPolling(t *testing.T) {
	r := dueShape(0)
	polls := 0
	r.w.stop = func() bool { polls++; return true }
	for range 1024 * 64 {
		if r.mutationStep(9) {
			break
		}
	}
	if !r.w.stopped || polls != 1 || r.w.workTicks != 1024 {
		t.Fatalf("large-map writes delayed cancellation: stopped %v, polls %d, ticks %d", r.w.stopped, polls, r.w.workTicks)
	}
}
