package mvcc

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// orderModel drives a store's order with the operations a fuzz input
// encodes and checks every walk against the keys sorted.
type orderModel struct {
	t    testing.TB
	s    *Store
	ids  map[string]uint32
	keys []string
}

func newOrderModel(t testing.TB) *orderModel {
	return &orderModel{t: t, s: New(&Heap{}), ids: map[string]uint32{}}
}

func (m *orderModel) insert(key []byte) {
	if _, ok := m.ids[string(key)]; ok {
		return
	}
	k := m.s.insert(key, nil)
	m.ids[string(key)] = k.id
	i, _ := slices.BinarySearch(m.keys, string(key))
	m.keys = slices.Insert(m.keys, i, string(key))
}

func (m *orderModel) remove(key []byte) {
	id, ok := m.ids[string(key)]
	if !ok {
		return
	}
	m.s.remove([]*keyIndex{m.s.rec(id)})
	delete(m.ids, string(key))
	i, _ := slices.BinarySearch(m.keys, string(key))
	m.keys = slices.Delete(m.keys, i, i+1)
}

// want returns the keys from start on, before end if end is not nil.
func (m *orderModel) want(start, end []byte) []string {
	i, _ := slices.BinarySearch(m.keys, string(start))
	j := len(m.keys)
	if end != nil {
		j, _ = slices.BinarySearch(m.keys, string(end))
		j = max(i, j)
	}
	return m.keys[i:j]
}

func (m *orderModel) walk(start, end []byte, limit int) []string {
	var got []string
	m.s.walk(start, end, func(k *keyIndex) bool {
		got = append(got, string(m.s.key(k)))
		return len(got) < limit
	})
	return got
}

func (m *orderModel) check(start, end []byte, limit int) {
	m.t.Helper()
	want := m.want(start, end)
	if len(want) > limit {
		want = want[:limit]
	}
	if got := m.walk(start, end, limit); !slices.Equal(got, want) {
		m.t.Fatalf("walk from %q to %q, at most %d, over %q: got %q, want %q", start, end, limit, m.keys, got, want)
	}
	if m.s.keys.Len() != len(m.keys) {
		m.t.Fatalf("the order holds %d keys, want %d", m.s.keys.Len(), len(m.keys))
	}
}

// key draws a key from a small alphabet that makes deep and shallow
// directories, names that share their first namePrefix bytes, and keys
// that end in '/'.
func orderKey(r *rand.Rand) []byte {
	parts := []string{"/", "a", "b", "/registry/", "pods/", "ns-1/", "ns-2/", "nginx-deployment-5d59d67564-", "x", "\x00", "\xff", "0"}
	var k []byte
	for range r.IntN(5) {
		k = append(k, parts[r.IntN(len(parts))]...)
	}
	return k
}

// TestOrderWalks checks walks of every kind against the sorted keys while
// keys come and go.
func TestOrderWalks(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	m := newOrderModel(t)
	for step := range 20000 {
		switch r.IntN(4) {
		case 0, 1:
			m.insert(orderKey(r))
		case 2:
			if len(m.keys) > 0 {
				m.remove([]byte(m.keys[r.IntN(len(m.keys))]))
			}
		}
		if step%7 == 0 {
			start := orderKey(r)
			var end []byte
			if r.IntN(3) > 0 {
				end = orderKey(r)
			}
			m.check(start, end, 1+r.IntN(40))
			m.check(nil, nil, 1<<30)
		}
	}
	for len(m.keys) > 0 {
		m.remove([]byte(m.keys[0]))
	}
	m.check(nil, nil, 1<<30)
	if n := len(m.s.keys.dirs) - len(m.s.keys.free); n != 1 || len(m.s.keys.byPath) != 1 {
		t.Fatalf("with no keys left the order keeps %d directories, %d by path; want only the root", n, len(m.s.keys.byPath))
	}
}

// TestOrderNestedWalk walks inside a walk, as a range does to read a
// key's neighbours: the outer walk goes on where it was.
func TestOrderNestedWalk(t *testing.T) {
	m := newOrderModel(t)
	for i := range 300 {
		m.insert(fmt.Appendf(nil, "/registry/pods/ns-%d/pod-%03d", i%3, i))
	}
	var got []string
	m.s.walk([]byte("/registry/pods/ns-1/pod-100"), nil, func(k *keyIndex) bool {
		got = append(got, string(m.s.key(k)))
		m.walk([]byte("/registry/pods/ns-0/"), []byte("/registry/pods/ns-0/pod-050"), 1<<30)
		return true
	})
	if want := m.want([]byte("/registry/pods/ns-1/pod-100"), nil); !slices.Equal(got, want) {
		t.Fatalf("the outer walk got %q, want %q", got, want)
	}
}

// FuzzOrder runs the operations a fuzz input encodes: each byte picks an
// operation, the bytes after it make its keys.
func FuzzOrder(f *testing.F) {
	f.Add([]byte("\x00/a/b\x00/a/c\x02/a/\x00\x01/a/b\x02"))
	f.Fuzz(func(t *testing.T, in []byte) {
		m := newOrderModel(t)
		next := func() []byte {
			if len(in) == 0 {
				return nil
			}
			n := min(int(in[0]%12), len(in)-1)
			k := in[1 : 1+n]
			in = in[1+n:]
			return bytes.Clone(k)
		}
		for len(in) > 0 {
			op := in[0] % 4
			in = in[1:]
			switch op {
			case 0:
				m.insert(next())
			case 1:
				m.remove(next())
			default:
				start := next()
				var end []byte
				if op == 3 {
					end = next()
				}
				m.check(start, end, 1<<30)
			}
		}
	})
}
