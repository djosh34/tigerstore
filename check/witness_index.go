package check

import (
	"iter"
	"maps"
)

// revisionLists keeps the usual small, dense revisions out of hash tables.
// Revisions outside the dense prefix still work, without an allocation
// proportional to a revision supplied by a client.
type revisionLists[T any] struct {
	dense  []revisionRange
	links  []revisionLink[T]
	values []T
	packed bool
	sparse map[int64][]T
}

type revisionRange struct{ lo, hi int }
type revisionLink[T any] struct {
	value T
	prev  int
}

// pack turns the lists collected by add into contiguous, immutable
// slices. The per-revision index has no pointers and no list needs its
// own allocation. Before packing lo is the head+1 and hi is the count.
func (t *revisionLists[T]) pack() {
	if t.packed {
		return
	}
	t.values = make([]T, len(t.links))
	n := 0
	for rev, r := range t.dense {
		end := n + r.hi
		j := end
		for i := r.lo; i != 0; i = t.links[i-1].prev {
			j--
			t.values[j] = t.links[i-1].value
		}
		t.dense[rev] = revisionRange{n, end}
		n = end
	}
	t.links = nil
	t.packed = true
}

func (t *revisionLists[T]) get(rev int64) []T {
	if rev >= 0 && rev < int64(len(t.dense)) {
		t.pack()
		r := t.dense[rev]
		return t.values[r.lo:r.hi]
	}
	return t.sparse[rev]
}

func (t *revisionLists[T]) add(rev int64, v T) {
	if rev >= 0 && rev < int64(len(t.dense)) {
		if t.packed {
			panic("adding to a packed revision index")
		}
		r := &t.dense[rev]
		t.links = append(t.links, revisionLink[T]{value: v, prev: r.lo})
		r.lo, r.hi = len(t.links), r.hi+1
		return
	}
	if t.sparse == nil {
		t.sparse = make(map[int64][]T)
	}
	t.sparse[rev] = append(t.sparse[rev], v)
}

func (t *revisionLists[T]) all() iter.Seq2[int64, []T] {
	return func(yield func(int64, []T) bool) {
		t.pack()
		for rev, r := range t.dense {
			if r.lo < r.hi && !yield(int64(rev), t.values[r.lo:r.hi]) {
				return
			}
		}
		for rev, vs := range t.sparse {
			if !yield(rev, vs) {
				return
			}
		}
	}
}

type keyLife struct {
	key  string
	life life
	to   int64
	next int
}

// recordLife returns whether this is a new observation. Lives sharing a
// mod revision are chained, preserving conflicting observations too.
func (r *revisionOrder) recordLife(key string, l life, to int64) bool {
	head, ok := r.lifeHeads.get(l.m)
	if !ok {
		head = -1
	}
	for i := head; i >= 0; i = r.lifeValues[i].next {
		v := &r.lifeValues[i]
		if v.key == key && v.life == l {
			v.to = max(v.to, to)
			return false
		}
	}
	i := len(r.lifeValues)
	r.lifeValues = append(r.lifeValues, keyLife{key: key, life: l, to: to, next: head})
	r.lifeHeads.set(l.m, i)
	r.lifeByKey[key] = append(r.lifeByKey[key], i)
	return true
}

func (r *revisionOrder) keyLives(key string) iter.Seq2[life, int64] {
	if r.w.slow {
		return maps.All(r.lives[key])
	}
	return func(yield func(life, int64) bool) {
		for _, i := range r.lifeByKey[key] {
			v := &r.lifeValues[i]
			if !yield(v.life, v.to) {
				return
			}
		}
	}
}

// revisionWriters encodes op i as i+1; zero is an absent writer.
type revisionWriters struct {
	dense  []int
	sparse map[int64]int
}

func (t *revisionWriters) get(rev int64) (int, bool) {
	if rev >= 0 && rev < int64(len(t.dense)) {
		i := t.dense[rev]
		return i - 1, i != 0
	}
	i, ok := t.sparse[rev]
	return i, ok
}

func (t *revisionWriters) set(rev int64, i int) {
	if rev >= 0 && rev < int64(len(t.dense)) {
		t.dense[rev] = i + 1
		return
	}
	if t.sparse == nil {
		t.sparse = make(map[int64]int)
	}
	t.sparse[rev] = i
}
