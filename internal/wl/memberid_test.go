package wl

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// badMemberIDs are member IDs wl cannot use (#841): 0, past 32 bits, as
// etcd's 64-bit IDs are, with bit 62 or 63 set, which the peer hello
// marks, and the largest.
var badMemberIDs = []uint64{0, 1 << 32, 1<<40 + 1, 1<<62 - 1, 1<<62 + 1, 1<<63 + 2, math.MaxUint64}

// refusedID checks that err refuses member id, naming it and the bound.
func refusedID(t *testing.T, what string, id uint64, err error) {
	t.Helper()
	if !errors.Is(err, ErrMemberID) {
		t.Errorf("%s %#x: %v, want %v", what, id, err, ErrMemberID)
		return
	}
	for _, want := range []string{"1 to 2^32-1", "etcd's 64-bit", fmt.Sprintf("member %d (%#x)", id, id)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s %#x: %q does not say %q", what, id, err, want)
		}
	}
}

// A node refuses a member ID of 0 or past 32 bits, its own or another
// member's, naming it and the bound, and takes one up to 2^32-1.
func TestAMemberIDPast32BitsIsRefused(t *testing.T) {
	for _, id := range badMemberIDs {
		own := startConfig([]Member{{ID: id}, {ID: 2}, {ID: 3}}, false, 0)
		own.ID = id
		err := own.check()
		refusedID(t, "own ID", id, err)
		if !errors.Is(err, errConfig) {
			t.Errorf("own ID %#x: %v is not a configuration error", id, err)
		}
		other := startConfig([]Member{{ID: 1}, {ID: id}, {ID: 3}}, false, 0)
		refusedID(t, "another member's ID", id, other.check())
	}
	top := startConfig([]Member{{ID: math.MaxUint32}, {ID: 1}}, false, 0)
	top.ID = math.MaxUint32
	if err := top.check(); err != nil {
		t.Errorf("member ID 2^32-1 refused: %v", err)
	}
}

// A restore refuses a member ID past 32 bits before it writes anything:
// the data dir stays empty.
func TestARestoreRefusesAMemberIDPast32Bits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	s := &SnapStore{
		Cluster: 99, Index: 500, Term: 4, Rev: 8, CompactRev: 8,
		Keys: func(yield func(SnapKey, error) bool) {
			yield(SnapKey{Key: []byte("/k"), States: []SnapState{{Value: []byte("v"), Create: 8, Mod: 8, Version: 1}}}, nil)
		},
	}
	if err := WriteSnapshot(f, s); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range badMemberIDs {
		dir := filepath.Join(t.TempDir(), "data")
		members := []Member{{ID: 1, PeerURLs: []string{"https://127.0.0.1:2380"}}, {ID: id, PeerURLs: []string{"https://127.0.0.2:2380"}}}
		_, _, _, err := Restore(path, dir, members, false)
		refusedID(t, "a restore with member", id, err)
		if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
			t.Errorf("a refused restore with member %#x left %d entries in the data dir", id, len(ents))
		}
	}
	members := []Member{{ID: math.MaxUint32, PeerURLs: []string{"https://127.0.0.1:2380"}}}
	if _, _, _, err := Restore(path, filepath.Join(t.TempDir(), "data"), members, false); err != nil {
		t.Errorf("a restore with member 2^32-1: %v", err)
	}
}
