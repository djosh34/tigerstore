package sim

import (
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"testing"
	"testing/cryptotest"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestMembershipRemovalColdFixture keeps #211 independent of subsequent
// workload and fault scheduling changes. The disks were copied in seed 88
// at 34,365 ms on 3f852051, before erasing node 1. Index 207 had applied.
func TestMembershipRemovalColdFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/membership-cold-88.json")
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		At               int64
		Applied          uint64
		Term, Hash, Node uint64
		IDs, Runs        []uint64
		Inactive         []bool
		Disks            []map[string][]byte
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Disks) != 4 || len(saved.IDs) != 4 || len(saved.Runs) != 4 || len(saved.Inactive) != 4 || saved.Applied != 207 || saved.Hash == 0 {
		t.Fatal("incomplete cold-restart fixture")
	}
	c := &checkpoint{at: saved.At, applied: saved.Applied, ids: saved.IDs, runs: saved.Runs, inactive: saved.Inactive}
	for _, files := range saved.Disks {
		d := simfs.New()
		for _, name := range slices.Sorted(maps.Keys(files)) {
			f, err := d.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			n, err := f.WriteAt(files[name], 0)
			if err == nil && n != len(files[name]) {
				err = io.ErrShortWrite
			}
			if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		c.disks = append(c.disks, d)
	}
	// Vary the cold election schedule while preserving the durable bytes.
	for seed := uint64(1); seed <= 32; seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cryptotest.SetGlobalRandom(t, seed)
			s := newSim(Config{Seed: seed, Membership: true})
			// Reaching 207 by overwriting the committed removal is not a fix.
			s.log[saved.Applied] = logRec{term: saved.Term, hash: saved.Hash, node: saved.Node}
			if err := s.coldRestart(c, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}
