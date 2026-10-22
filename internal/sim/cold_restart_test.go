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
// erased, -1 for none, after change, if any, edits the run and its
// checkpoint. It returns whether the cold restart found that no node can
// win an election, and its error.
func coldRestart84216(t *testing.T, erased int, change func(*Sim, *checkpoint)) (bool, error) {
	t.Helper()
	s, err := newSim(Config{Seed: 389, Membership: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.shutdown()
	c := cold84216Checkpoint(t)
	if change != nil {
		change(s, c)
	}
	err = s.coldRestart(c, erased)
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.ContainsFunc(s.trace, func(l string) bool { return strings.Contains(l, "no node can win an election") }), err
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
		none, err := coldRestart84216(t, 2, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !none {
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
		none, err := coldRestart84216(t, -1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if none {
			t.Fatal("the cold restart with every disk found that no node can win an election")
		}
	})
}

// TestColdRestartNoElectionLost requires that a cold restart where no node
// can win an election still finds the entries applied before on a
// surviving disk, as the run's record has them: entries past every
// surviving log, and an entry 1344 that differs from the record's on the
// only disk that holds it (from the fix-check of #389 on PR #384).
func TestColdRestartNoElectionLost(t *testing.T) {
	if simrun.Drive(t, 389) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		for _, c := range []struct {
			name   string
			change func(*Sim, *checkpoint)
		}{
			{"missing entries", func(_ *Sim, c *checkpoint) { c.applied = 1352 }},
			{"different entry", func(s *Sim, c *checkpoint) {
				c.applied = 1344
				s.log[1344] = logRec{term: 9}
			}},
		} {
			_, err := coldRestart84216(t, 2, c.change)
			if err == nil || !strings.Contains(err.Error(), "no surviving disk holds") {
				t.Errorf("%s: the cold restart returned %v", c.name, err)
			}
		}
	})
}
