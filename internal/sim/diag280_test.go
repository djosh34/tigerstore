package sim

import (
	"fmt"
	"os"
	"testing"
	"testing/synctest"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The actual last-crash WAL images from campaign 39185668816, artifact
// sim-campaign-11, TestSeeds_8255809. The restart trace proves that by the
// 18819ms copy both survivors had also synced term 2, vote 1, commit 0.
// We reproduce that transition, not the seed's unrelated workload/faults.
func TestDiag280ColdRestart(t *testing.T) {
	for _, control := range []bool{false, true} {
		name := "torn-survivors"
		if control {
			name = "complete-node2-control"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, err := newSim(Config{Seed: 280})
				if err != nil {
					t.Fatal(err)
				}
				defer r.shutdown()
				r.healed, r.votes = true, true
				var third *raftpb.Entry
				for i, n := range r.nodes {
					b, err := os.ReadFile(fmt.Sprintf("testdata/diag280/node%d.wal", i+1))
					if err != nil {
						t.Fatal(err)
					}
					d := simfs.New()
					f, err := d.Create("0000000000000001.wal")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.Write(b); err != nil {
						t.Fatal(err)
					}
					if err := f.Sync(); err != nil {
						t.Fatal(err)
					}
					if err := d.SyncDir(); err != nil {
						t.Fatal(err)
					}
					w, rec, err := wal.Open(d)
					if err != nil {
						t.Fatal(err)
					}
					wantLast := []uint64{5, 2, 1}[i]
					wantCommit := []uint64{3, 0, 0}[i]
					if rec.Last != wantLast || rec.HardState.Commit != wantCommit {
						t.Fatalf("node %d image: last %d commit %d, want %d/%d", i+1, rec.Last, rec.HardState.Commit, wantLast, wantCommit)
					}
					if i == 0 {
						third, err = w.Entry(3)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						var tail []*raftpb.Entry
						if control && i == 1 {
							tail = []*raftpb.Entry{third}
						}
						w.Append(&wal.HardState{Term: 2, Vote: 1}, tail)
					}
					if err := w.Sync(); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					n.disk = d.Durable()
					r.traceDisk(n)
				}
				// Disk 1 is gone. Only nodes 2 and 3 participate, as in the
				// failed cold restart, with no faults and all links open.
				r.nodes[0].fenced = true
				for _, n := range r.nodes {
					if err := r.start(n); err != nil {
						t.Fatal(err)
					}
				}
				if err := r.runFor(25 * Second); err != nil {
					t.Fatal(err)
				}
				t.Log(r.report())
				for _, n := range r.nodes[1:] {
					c := n.proc.core
					if c.Leader() == 0 || c.Applied() < 3 || len(c.Members()) != 3 {
						t.Errorf("node %d: leader %d, applied %d, members %v; want a leader and the full bootstrap", n.id, c.Leader(), c.Applied(), c.Members())
					}
				}
			})
		})
	}
}
