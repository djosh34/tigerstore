package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"unsafe"
)

func TestIndexesRandom(t *testing.T) {
	a := &arena{}
	for i := 0; i < 2000; i++ {
		a.add(fmt.Sprintf("/registry/events/bench-003-0042/object-%012d", i))
	}
	h := newFlat(a, 2000)
	o := &ordered{a: a}
	m := map[string]uint32{}
	r := rand.New(rand.NewPCG(577, 4))
	for step := 0; step < 20000; step++ {
		id := uint32(r.IntN(2000))
		key := a.key(id)
		switch r.IntN(3) {
		case 0:
			h.insert(id)
			o.insert(id)
			m[key] = id
		case 1:
			_, exists := m[key]
			if h.remove(key) != exists || o.remove(key) != exists {
				t.Fatal("delete mismatch", step)
			}
			delete(m, key)
		case 2:
			v, ok := h.lookup(strings.Clone(key))
			want, exists := m[key]
			if ok != exists || (ok && v != want) {
				t.Fatal("lookup mismatch", step)
			}
		}
		if step%100 == 0 {
			want := make([]string, 0, len(m))
			for k := range m {
				want = append(want, k)
			}
			slices.Sort(want)
			if len(o.rows) != len(want) {
				t.Fatal("length mismatch")
			}
			for i, k := range want {
				if o.key(o.rows[i]) != k || o.seek(k) != i {
					t.Fatal("ordered mismatch")
				}
			}
			probe := a.key(uint32(r.IntN(2000)))
			idx, _ := slices.BinarySearch(want, probe)
			if o.seek(probe) != idx {
				t.Fatal("lower bound mismatch")
			}
			var keys [16]string
			var sum uint64
			for j := range keys {
				keys[j] = strings.Clone(a.key(uint32(r.IntN(2000))))
				if v, ok := m[keys[j]]; ok {
					sum += uint64(v)
				}
			}
			if h.batch(keys[:]) != sum {
				t.Fatal("batch mismatch")
			}
		}
	}
	if unsafe.Sizeof(entry{}) != 16 || unsafe.Sizeof(record{}) != 8 || unsafe.Sizeof(orderedRecord{}) != 16 {
		t.Fatal("unexpected index layout")
	}
}
func TestRaftAsyncCommitAndRead(t *testing.T) {
	c := newCluster()
	var v tally
	for i := 0; i < 5; i++ {
		c.write(make([]byte, 1024), 8, &v)
		c.read(&v)
		c.compact()
	}
	if c.applied[1] != 42 || c.reads != 5 {
		t.Fatal(c.applied, c.reads)
	}
}
