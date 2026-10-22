package sim

import (
	"embed"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
)

//go:embed testdata/cold-84216/node*
var cold84216 embed.FS

// imageDisk is a disk that holds the files of dir in testdata/cold-84216,
// synced.
func imageDisk(t *testing.T, dir string) *simfs.Disk {
	t.Helper()
	d := simfs.New()
	dir = path.Join("testdata/cold-84216", dir)
	ents, err := cold84216.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		b, err := cold84216.ReadFile(path.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		f, err := d.Create(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	return d
}

// cold84216Checkpoint is the checkpoint the cold restart of membership
// seed 84216 started from.
func cold84216Checkpoint(t *testing.T) *checkpoint {
	t.Helper()
	c := &checkpoint{
		at: 49602 * Millisecond, applied: 1342, ids: []uint64{1, 2, 3, 4},
		bad: make([]bool, 4), inactive: make([]bool, 4), started: []bool{true, true, true, true},
	}
	for _, dir := range []string{"node1", "node2", "node3-erased", "node4"} {
		c.disks = append(c.disks, imageDisk(t, dir))
	}
	return c
}

// coldRestart84216 runs the cold restart of seed 84216 with disk erased
// erased, -1 for none, and reports whether it found that no node can win
// an election.
func coldRestart84216(t *testing.T, erased int) bool {
	t.Helper()
	s, err := newSim(Config{Seed: 389, Membership: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.shutdown()
	if err := s.coldRestart(cold84216Checkpoint(t), erased); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.ContainsFunc(s.trace, func(l string) bool { return strings.Contains(l, "no node can win an election") })
}

// TestColdRestartNoElection runs the cold restart of membership seed 84216,
// whose erased disk leaves no node able to win an election, so it cannot
// converge and need not (#389). It failed with "cold restart did not
// converge" while the rule required it.
func TestColdRestartNoElection(t *testing.T) {
	if simrun.Drive(t, 389) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		if !coldRestart84216(t, 2) {
			t.Fatal("the cold restart with node 3 erased did not find that no node can win an election")
		}
	})
}

// TestColdRestartElects is its control: with every disk, the same nodes
// elect a leader and converge.
func TestColdRestartElects(t *testing.T) {
	if simrun.Drive(t, 389) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		if coldRestart84216(t, -1) {
			t.Fatal("the cold restart with every disk found that no node can win an election")
		}
	})
}
