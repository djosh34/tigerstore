package check

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func recheckStore(n int) *state {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%06d", i)
	}
	s := newState(keys, &history.Start{Rev: 2})
	for i, key := range keys {
		s.kvs[i] = history.KeyValue{Key: key, Value: "old", CreateRevision: 2, ModRevision: 2, Version: 1}
	}
	return s
}

// A put followed by a start-revision range must see the original value,
// without allocating a full store snapshot for every two-op transaction.
func TestHistoricalTxnSnapshotScalesWithTouchedKeys(t *testing.T) {
	for _, keys := range []int{1, 100_000} {
		t.Run(strconv.Itoa(keys), func(t *testing.T) {
			s := recheckStore(keys)
			r := dueShape(0)
			s.stop = r.step
			q := &history.RangeRequest{Key: s.keys[0]}
			txn := &history.TxnRequest{Success: []history.TxnOp{
				{Put: &history.PutRequest{Key: s.keys[0], Value: "new"}},
				{Range: q},
			}}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := range 128 {
				q.Revision = s.rev
				o := s.stepTxn(txn, false)
				if o.err != nil || o.resp == nil {
					t.Fatalf("txn %d: %v", i, o.err)
				}
				got := o.resp.Txn.Results[1].Range.KVs
				if len(got) != 1 || got[0].Version != int64(i+1) {
					t.Fatalf("txn %d lost its before value: %v", i, got)
				}
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("%d keys: %d bytes, %d weighted steps, %d polling ticks", keys, allocated, r.w.steps, r.w.workTicks)
			if allocated > 2<<20 {
				t.Fatalf("two-op txns copied untouched keys: allocated %d bytes, want <= 2 MiB", allocated)
			}
		})
	}
}

// Copies that cannot use the transaction overlay must still poll during
// copying, not merely before or after a single unbounded slices.Clone.
func TestReplayClonePollsPerCopiedKey(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(strconv.FormatBool(stop), func(t *testing.T) {
			const keys = 100_000
			s := recheckStore(keys)
			r := dueShape(0)
			s.stop = r.step
			polls := 0
			r.w.stop = func() bool { polls++; return stop }
			c := s.clone()
			if stop {
				if !r.w.stopped || polls != 1 || r.w.workTicks > 1024 {
					t.Fatalf("copy missed its bounded stop: stopped=%v, polls=%d, ticks=%d", r.w.stopped, polls, r.w.workTicks)
				}
			} else if r.w.stopped || !s.equal(c) || r.w.steps < keys/1024 || r.w.workTicks < keys/64 {
				t.Fatalf("copy did not charge its keys: stopped=%v, steps=%d, ticks=%d", r.w.stopped, r.w.steps, r.w.workTicks)
			}
		})
	}
}

func TestRevisionLimitTxnCopyStops(t *testing.T) {
	s := recheckStore(100_000)
	s.rev = math.MaxInt64
	r := dueShape(0)
	s.stop = r.step
	r.w.stop = func() bool { return true }
	o := s.stepTxn(&history.TxnRequest{}, false)
	if o.err != errReplayStopped || !r.w.stopped || r.w.workTicks > 1024 {
		t.Fatalf("revision-limit trial lost its copy's cancellation: err=%v, stopped=%v, ticks=%d", o.err, r.w.stopped, r.w.workTicks)
	}
}

func TestCachedStateCopyPollsPerCopiedKey(t *testing.T) {
	s := recheckStore(100_000)
	r := dueShape(0)
	w := r.w
	w.search = r
	w.keys, w.kvs, w.kvsRev = s.keys, s.kvs, s.rev
	w.states[0].rev = s.rev // Make stateAt use its already-filled cache.
	polls := 0
	w.stop = func() bool { polls++; return true }
	_ = w.stateAt(0)
	if !w.stopped || polls != 1 || w.workTicks > 1024 {
		t.Fatalf("cached copy missed its bounded stop: stopped=%v, polls=%d, ticks=%d", w.stopped, polls, w.workTicks)
	}
}

func TestRevisionTimeoutOmitsEarlierRejection(t *testing.T) {
	h := budgetHistory(t, "witness-control-320-1529.json.gz")
	if err := Witnessed(h, FromRevisions, time.Minute); err != nil {
		t.Fatalf("uncapped history should pass: %v", err)
	}
	err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(3000))
	if !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("want timeout: %v", err)
	}
	for _, progress := range []string{"the search was at revision", "gaps open", "had gone back"} {
		if !strings.Contains(err.Error(), progress) {
			t.Fatalf("lost search progress %q: %v", progress, err)
		}
	}
	if strings.Contains(err.Error(), "the furthest it got") || strings.Contains(err.Error(), "no acked op wrote revision") {
		t.Fatalf("timeout carries an earlier branch rejection: %v", err)
	}
}

func TestLargeCovererOrderingPollsDuringMerge(t *testing.T) {
	const n = 1_000_000
	r := dueShape(0)
	r.putters = [][]int{make([]int, n), make([]int, n)}
	for i := range n {
		r.putters[0][i], r.putters[1][i] = 2*i, 2*i+1
	}
	s := newState([]string{"a", "b"}, nil)
	requested, observed := int64(-1), int64(-1)
	visited, requestedItems := 0, 0
	r.w.stop = func() bool {
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".(*revisionOrder).mergeCoverers") {
				if requested < 0 {
					// Request just after this poll. The next poll must
					// see it within 65,536 further inner work items.
					requested = r.w.workTicks
					requestedItems = visited
					return false
				}
				observed = r.w.workTicks
				return true
			}
			if !more {
				return false
			}
		}
	}
	// Zero range variables work for both the old eager slice and the
	// polled iterator, so this regression also runs on the reviewed head.
	for range r.coverers(s, []int{1, 1, 0, 0}) {
		visited++
	}
	if !r.w.stopped || requested < 0 || requestedItems == 0 || observed < 0 || (observed-requested)*64 > 65_536 || visited-requestedItems > 65_536 {
		t.Fatalf("large ordering missed its in-flight stop: stopped=%v, requested=%d, observed=%d ticks, further candidates=%d", r.w.stopped, requested, observed, visited-requestedItems)
	}
	t.Logf("requested after %d candidates; stopped within %d more candidates / %d accounting ticks", requestedItems, visited-requestedItems, observed-requested)
}
