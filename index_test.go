package main

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
)

func TestIndexes(t *testing.T) {
	for _, kind := range []string{"arena", "btree", "slice"} {
		t.Run(kind, func(t *testing.T) {
			var idx index
			switch kind {
			case "arena":
				idx = newArena()
			case "btree":
				idx = newStock()
			case "slice":
				idx = newBuffered(nil)
			}
			ref := map[string]uint32{}
			r := rand.New(rand.NewPCG(3, 7))
			keys := make([]string, 75000)
			for i := range keys {
				keys[i] = fmt.Sprintf("/registry/%02x/ns-%04d/%016x", r.UintN(255), r.IntN(500), r.Uint64())
			}
			keys = append(keys, "", "\x00", "\xff", "/registry/", "/registry/00/ns-0000/", strings.Repeat("x", 96))
			for i, k := range keys {
				h := uint32(i + 1)
				idx.Put(k, h)
				ref[k] = h
			}
			// Force repeated changes of the common prefix and tombstone/recreate counts.
			for i := 0; i < 10000; i++ {
				k := keys[r.IntN(len(keys))]
				h := uint32(i + 100000)
				if i%3 == 0 {
					h |= dead
				}
				idx.Put(k, h)
				ref[k] = h
			}
			for k, h := range ref {
				if idx.Get(k) != h {
					t.Fatalf("Get(%q) = %d, want %d", k, idx.Get(k), h)
				}
			}
			sort.Strings(keys)
			for q := 0; q < 500; q++ {
				x, y := r.IntN(len(keys)), r.IntN(len(keys))
				if x > y {
					x, y = y, x
				}
				a, b := keys[x], keys[y]
				want := uint64(0)
				var ordered []uint32
				for _, k := range keys[x:y] {
					want += alive(ref[k])
					ordered = append(ordered, ref[k])
				}
				if got := idx.Count(a, b); got != want {
					t.Fatalf("Count(%q,%q)=%d want %d", a, b, got, want)
				}
				j := 0
				idx.Walk(a, b, func(h uint32) bool {
					if j >= len(ordered) || h != ordered[j] {
						t.Fatalf("walk order at %d", j)
					}
					j++
					return true
				})
				if j != len(ordered) {
					t.Fatalf("walk size %d want %d", j, len(ordered))
				}
			}
		})
	}
}
func TestPrefixShrink(t *testing.T) {
	a := newArena()
	for i := 0; i < 128; i++ {
		a.Put(strings.Repeat("x", 80)+fmt.Sprintf("%016x", i), uint32(i+1))
	}
	a.Put("a", 1000)
	if a.Count("", "z") != 129 {
		t.Fatal("prefix shrink lost keys")
	}
	for i := 0; i < 128; i++ {
		if a.Get(strings.Repeat("x", 80)+fmt.Sprintf("%016x", i)) != uint32(i+1) {
			t.Fatal("prefix shrink lookup")
		}
	}
}
func TestBulk(t *testing.T) {
	a := newArena()
	n := 100000
	a.bulk(n, key, func(i int) uint32 { return uint32(i + 1) })
	for i := 0; i < n; i++ {
		if got := a.Get(key(i)); got != uint32(i+1) {
			t.Fatalf("bulk %d=%d", i, got)
		}
	}
	for i := 0; i < n; i += 1000 {
		p := prefix(i, false)
		if a.Count(p, prefixEnd(p)) != 1000 {
			t.Fatal("bulk count")
		}
	}
}
func TestVersionVisibility(t *testing.T) {
	var v versions
	h1 := v.add(version{rev: 1})
	h2 := v.add(version{rev: 4, prev: h1, flags: 1})
	h3 := v.add(version{rev: 6, prev: h2})
	for _, c := range []struct {
		rev  uint64
		want uint32
	}{{0, 0}, {1, h1}, {3, h1}, {4, 0}, {5, 0}, {6, h3}, {99, h3}} {
		if got := v.lookup(h3, c.rev); got != c.want {
			t.Fatalf("rev %d=%d want %d", c.rev, got, c.want)
		}
	}
}
