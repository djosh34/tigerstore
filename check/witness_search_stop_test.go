package check

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// armSearchStop changes its answer while the phase is running. The second
// poll stops it; tick's latch must carry that stop out to search, even if a
// helper returns a partial set or a false fit. No subsequent work may poll
// the hook again or turn that cancellation into a failure explanation.
func armSearchStop(r *revisionOrder) *int {
	calls := new(int)
	r.w.steps, r.workLeft = 0, 0
	r.w.stop = func() bool {
		*calls++
		return *calls == 2
	}
	return calls
}

func assertSearchStopped(t *testing.T, r *revisionOrder, calls *int) {
	t.Helper()
	if !r.w.stopped || *calls != 2 || r.w.steps != 2048 {
		t.Fatalf("stopped=%v, polls=%d, steps=%d; want stop at the second poll, within 2*1024*64 work items", r.w.stopped, *calls, r.w.steps)
	}
	before := r.w.steps
	if err := r.search(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("search lost its inner phase's cancellation: %v", err)
	}
	if *calls != 2 || r.w.steps != before || len(r.w.events) != 0 {
		t.Fatal("search kept working after the stop")
	}
}

func TestSearchStopInsideDue(t *testing.T) {
	for _, phase := range []string{"build", "query", "lateDue", "expire", "expireEarly"} {
		t.Run(phase, func(t *testing.T) {
			r := dueShape(150_000)
			if phase != "build" {
				for range r.due(1000, 1) {
				}
			}
			calls := armSearchStop(r)
			switch phase {
			case "build", "query":
				for range r.due(1000, 1) {
				}
			case "lateDue":
				_ = r.lateDue(1000, 1)
			case "expire":
				r.expire(1000, 1)
			case "expireEarly":
				r.expireEarly(1)
			}
			assertSearchStopped(t, r, calls)
		})
	}
}

func TestSearchReturnsStopFromDue(t *testing.T) {
	r := dueShape(150_000)
	// Build both indexes before arming the hook, so the stop is in the
	// retained query, not in indexing or the first call's sort.
	_ = r.earlyDue(1)
	for range r.due(1000, 1) {
	}
	calls := armSearchStop(r)
	if err := r.search(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("search lost the due stop: %v", err)
	}
	assertSearchStopped(t, r, calls)
}

func TestSearchStopInsideFitsRoom(t *testing.T) {
	h := &history.History{Ops: []history.Op{{Req: history.Request{Put: &history.PutRequest{Key: "k"}}, Err: errUnavailable}}}
	r := newRevisionOrder(newWitness(h, time.Now().Add(time.Hour)))
	r.putters = [][]int{make([]int, 150_000)}
	r.room = &room{
		ends: []int64{3}, byM: []lifeOf{{k: 0, c: 2, m: 3, v: 3}},
		puts: make([][]int64, 1), deletes: make([][]int64, 1), leasesOf: make([][]int64, 1),
	}
	r.top, r.limit, r.indexed = 2, 2, true
	r.resolved[2] = 0
	calls := armSearchStop(r)
	// The put writes revision 2 and passes the life checks. reachable then
	// enters roomReachable and coverers: roomNeed needs two more puts by 3.
	if err := r.search(); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("search lost the fits/roomReachable stop: %v", err)
	}
	assertSearchStopped(t, r, calls)
}

func TestSearchStopInsideCountsAndSolver(t *testing.T) {
	for _, phase := range []string{"reachable", "roomNeed", "fewestActions", "readsFit"} {
		t.Run(phase, func(t *testing.T) {
			r := dueShape(150_000)
			r.optional = nil
			r.countRevs = []int64{2}
			r.counts[2] = make([]count, 150_000)
			r.room = &room{byM: make([]lifeOf, 150_000)}
			for j := range r.room.byM {
				r.room.byM[j] = lifeOf{m: 2, c: 3}
			}
			calls := armSearchStop(r)
			switch phase {
			case "reachable":
				r.room = nil
				if r.reachable(r.w.live, -1) {
					t.Fatal("cancelled count check passed")
				}
			case "roomNeed":
				_ = r.roomNeed(r.w.live, 2)
			case "fewestActions":
				actions := make([][]int, 150_000)
				for j := range actions {
					actions[j] = []int{0}
				}
				if fewestActions([]int{1}, actions, 1, r.step) <= 1 {
					t.Fatal("cancelled solver found a cover")
				}
			case "readsFit":
				reads := make([]int, 150_000)
				if r.readsFit(optional{reads: reads}) {
					t.Fatal("cancelled reads passed")
				}
			}
			assertSearchStopped(t, r, calls)
		})
	}
}

func TestSearchInnerWorkBudget(t *testing.T) {
	for _, budget := range []int64{1, 100, 1000} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			r := dueShape(20_000)
			r.w.maxSteps = budget
			for range r.due(1000, 1) {
			}
			if !r.w.stopped || r.w.steps != budget+1 {
				t.Fatalf("due bypassed budget: stopped=%v, steps=%d", r.w.stopped, r.w.steps)
			}
			if err := r.search(); !errors.Is(err, ErrWitnessTimeout) {
				t.Fatalf("budget stop became a verdict: %v", err)
			}
		})
	}
}
