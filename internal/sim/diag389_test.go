package sim

// Diagnosis only: can the saved #389 disks elect a leader after node 3
// is erased? Run scripts/sim.sh ./internal/sim '^TestDiag389'.

import (
	"embed"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

//go:embed testdata/389/*/*
var diag389Images embed.FS

func diag389Checkpoint(t *testing.T) *checkpoint {
	t.Helper()
	c := &checkpoint{
		at:       49602 * Millisecond,
		applied:  1342,
		ids:      []uint64{1, 2, 3, 4},
		inactive: make([]bool, 4),
		started:  []bool{true, true, true, true},
		bad:      make([]bool, 4),
	}
	for i, dir := range []string{"node1", "node2", "node3-erased", "node4"} {
		d := simfs.New()
		path := "testdata/389/" + dir
		names, err := diag389Images.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			b, err := diag389Images.ReadFile(path + "/" + name.Name())
			if err != nil {
				t.Fatal(err)
			}
			f, err := d.Create(name.Name())
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
		c.disks = append(c.disks, d)
		w, rec, err := wal.Open(d.Durable())
		if err != nil {
			t.Fatal(err)
		}
		cfg := core.DefaultConfig(uint64(i+1), []uint64{1, 2, 3})
		cfg.Join = i == 3
		n, err := core.New(cfg, rec, w)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("node %d applied %d, learner %v, %s", i+1, n.Applied(), n.IsLearner(), n.LogState())
		for at := max(rec.Root.Index+1, uint64(1342)); at <= rec.Last; at++ {
			e, err := w.Entry(at)
			if err != nil {
				t.Fatal(err)
			}
			if e.GetType() == raftpb.EntryConfChange {
				var cc raftpb.ConfChange
				if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
					t.Fatal(err)
				}
				t.Logf("node %d entry %d term %d: %s", i+1, at, e.GetTerm(), &cc)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func diag389Restart(t *testing.T, erased int) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) struct{} {
		c := diag389Checkpoint(t)
		s, err := newSim(Config{Seed: seed, Membership: true})
		if err != nil {
			t.Fatal(err)
		}
		defer s.shutdown()
		if err := s.coldRestart(c, erased); err != nil {
			t.Fatal(err)
		}
		t.Logf("cold restart returned nil, erased index %d; parent trace %v", erased, s.trace)
		return struct{}{}
	}, nil)
}

func TestDiag389Erased(t *testing.T)       { diag389Restart(t, 2) }
func TestDiag389Restored(t *testing.T)     { diag389Restart(t, -1) }
func TestDiag389EraseLearner(t *testing.T) { diag389Restart(t, 3) }
