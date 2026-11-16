package wl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A checkpoint, loaded as a start loads it, holds the state of the store
// it was taken of (stateHash), with its values in the file or left in the
// log; another state hashes otherwise.
func TestACheckpointLoadsToTheStateItWasTakenOf(t *testing.T) {
	for _, takeIn := range []bool{true, false} {
		n, l := ckptNode(t)
		st := n.st
		st.leases[7] = &lease{ttl: 60, keys: map[uint32]struct{}{}}
		putValue(t, st, l, "/a", "a1")       // 2
		putValue(t, st, l, "/b", "b1")       // 3
		putValue(t, st, l, "/gone", "early") // 4
		putValue(t, st, l, "/a", "a2")       // 5
		id, _ := st.find([]byte("/gone"))
		st.rev++
		if _, err := st.del(id, st.rev); err != nil { // 6
			t.Fatal(err)
		}
		st.endApply()
		putValue(t, st, l, "/c", "c")  // 7
		putValue(t, st, l, "/a", "a3") // 8
		st.compact(6)
		st.trimSteps(1 << 20)
		want := st.stateHash()
		if takeIn {
			writeCkpt(t, n, 42)
		} else {
			writeCkptAt(t, n, 42)
		}
		b, err := os.ReadFile(filepath.Join(n.ckptDir, ckptName(42)))
		if err != nil {
			t.Fatal(err)
		}
		got, err := CheckpointStateHash(b, ckptName(42), l.read)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("taking values in %v: the checkpoint loads to state %#x, the store's is %#x", takeIn, got, want)
		}
		putValue(t, st, l, "/a", "a4")
		if st.stateHash() == want {
			t.Fatal("a put left the state's hash as it was")
		}
		// A value the log holds damaged does not load.
		id, _ = st.find([]byte("/b"))
		if k := st.keys[id]; !inCkpt(k.voff) {
			l.b[k.voff] ^= 1
			if _, err := CheckpointStateHash(b, ckptName(42), l.read); !errors.Is(err, ErrBadValue) {
				t.Fatalf("a damaged value in the log: %v, want %v", err, ErrBadValue)
			}
		}
	}
}
