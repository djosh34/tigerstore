package check

import (
	"cmp"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestRevisionIndexesKeepSparseAndDenseEntries(t *testing.T) {
	lists := revisionLists[int]{dense: make([]revisionRange, 8)}
	want := map[int64][]int{}
	writers := revisionWriters{dense: make([]int, 8)}
	for i, rev := range []int64{2, -1, 7, 2, math.MaxInt64, 0, -1} {
		lists.add(rev, i)
		want[rev] = append(want[rev], i)
		writers.set(rev, i)
	}
	got := map[int64][]int{}
	for rev, vs := range lists.all() {
		got[rev] = slices.Clone(vs)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lists = %v, want %v", got, want)
	}
	for rev, vs := range want {
		if !slices.Equal(lists.get(rev), vs) {
			t.Fatalf("get(%d) = %v, want %v", rev, lists.get(rev), vs)
		}
		i, ok := writers.get(rev)
		if !ok || i != vs[len(vs)-1] {
			t.Fatalf("writer(%d) = %d, %v", rev, i, ok)
		}
	}
	if _, ok := writers.get(3); ok {
		t.Fatal("absent writer is present")
	}
}

func TestTimeSortMatchesComparisonSort(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 3))
	w := &witness{}
	for _, n := range []int{0, 1, 63, 64, 1000, 128, 2000} {
		stamps := make([]int64, n)
		ops := rng.Perm(n)
		for i := range stamps {
			stamps[i] = int64(rng.Uint64())
			if i%3 == 0 {
				stamps[i] = int64(i % 11)
			}
		}
		if n > 1 {
			stamps[0], stamps[1] = math.MinInt64, math.MaxInt64
		}
		want := slices.Clone(ops)
		slices.SortFunc(want, func(a, b int) int {
			return cmp.Or(cmp.Compare(stamps[a], stamps[b]), cmp.Compare(a, b))
		})
		w.sortOpsByTime(ops, func(i int) int64 { return stamps[i] })
		if !slices.Equal(ops, want) {
			t.Fatalf("size %d: got %v, want %v", n, ops, want)
		}
	}
}

func TestVersionCursorMatchesBinarySearchAfterRestore(t *testing.T) {
	rng := rand.New(rand.NewPCG(499, 4))
	v := versions{"a": &keyVersions{}}
	var rev int64 = 1
	for n := 0; n < 10000; n++ {
		if rng.IntN(4) == 0 && len(v["a"].kvs) > 0 {
			vs := v["a"].kvs
			v["a"].kvs = vs[:rng.IntN(len(vs))]
			rev = 1
			if len(v["a"].kvs) > 0 {
				rev = v["a"].kvs[len(v["a"].kvs)-1].ModRevision
			}
		} else {
			rev += int64(rng.IntN(4) + 1)
			kv := history.KeyValue{Key: "a", ModRevision: rev, Version: 1}
			if rng.IntN(5) == 0 {
				kv.Version = 0
			}
			v.add([]history.Event{{KV: kv}})
		}
		for _, at := range []int64{0, rev, int64(rng.IntN(int(rev + 3))), math.MaxInt64} {
			vs := v["a"].kvs
			i, _ := slices.BinarySearchFunc(vs, at+1, func(kv history.KeyValue, target int64) int {
				return cmp.Compare(kv.ModRevision, target)
			})
			var want history.KeyValue
			present := i > 0 && vs[i-1].Version != 0
			if present {
				want = vs[i-1]
			}
			got, ok := v.at("a", at)
			if got != want || ok != present {
				t.Fatalf("iteration %d at %d: got %+v, %v; want %+v, %v", n, at, got, ok, want, present)
			}
		}
	}
}

func TestFirstAnswerDoesNotSurviveAChangedPrefix(t *testing.T) {
	put := history.Request{Put: &history.PutRequest{Key: "a", Value: "old"}}
	read := history.Request{Range: &history.RangeRequest{Key: "a"}}
	h := &history.History{Ops: []history.Op{
		{Call: 1, Return: 2, Req: put},
		{Call: 3, Return: 4, Req: read},
	}}
	w := newWitness(h, time.Now().Add(time.Minute))
	r := &revisionOrder{w: w}
	m := r.saveOrder()
	h.Ops[0].Resp = w.apply(0, put).resp
	h.Ops[1].Resp = w.live.clone().step(read, false).resp
	w.firstAnswers[1] = firstAnswer{id: w.states[1].id, at: 1}
	if err := w.placeSeen([]int{1}, false); err != nil {
		t.Fatal(err)
	}
	oldID := w.states[1].id
	r.restoreOrder(m)
	w.apply(0, history.Request{Put: &history.PutRequest{Key: "a", Value: "new"}})
	if oldID == w.states[1].id {
		t.Fatal("replacement state reused its abandoned identity")
	}
	if err := w.placeSeen([]int{1}, false); err == nil {
		t.Fatal("an abandoned prefix's cached answer was reused")
	}
}

func TestDenseLivesKeepConflictsAndHighestObservation(t *testing.T) {
	r := &revisionOrder{w: &witness{}, lifeHeads: revisionWriters{dense: make([]int, 8)}, lifeByKey: map[string][]int{}}
	want := map[life]int64{}
	for _, x := range []struct {
		l  life
		to int64
	}{
		{life{2, 3, 2}, 3}, {life{2, 3, 2}, 7}, {life{2, 3, 2}, 4},
		{life{3, 3, 1}, 5}, {life{2, 3, 9}, 6}, {life{100, 101, 2}, 102},
	} {
		r.recordLife("a", x.l, x.to)
		want[x.l] = max(want[x.l], x.to)
	}
	r.recordLife("b", life{2, 3, 2}, 100)
	got := map[life]int64{}
	for l, to := range r.keyLives("a") {
		got[l] = to
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lives = %v, want %v", got, want)
	}
}
