package sim

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"testing/cryptotest"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// cacheConfig is linearizableConfig with a small memory budget, so the value
// cache is on and reads hit it and miss it.
func cacheConfig(seed uint64) Config {
	cfg := linearizableConfig(seed)
	cfg.MemoryBytes = 16 << 10
	return cfg
}

// TestPastRAMRecovery checks the simulator's own history after packing and
// removing the original segments. The cleaner protocol runs on a durable
// copy, not on the live simulator: concurrent cleaner scheduling is not
// covered here. Each crash boundary recovers a new core, without its cache.
func TestPastRAMRecovery(t *testing.T) {
	var total int
	for seed := uint64(1); seed <= 16; seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			cryptotest.SetGlobalRandom(t, seed)
			s, res, err := runWithState(cacheConfig(seed))
			if err != nil {
				t.Fatal(err)
			}
			checkers(t, res)
			h, err := res.Checkable()
			if err != nil {
				t.Fatal(err)
			}
			if err := agree(h); err != nil {
				t.Fatal(err)
			}
			var cleaned int
			var hits, misses int64
			for _, n := range s.live() {
				m := n.core.Metrics()
				hits += m.CacheHits
				misses += m.CacheMisses
				model, err := durableModel(s, n.disk)
				if err != nil {
					t.Fatal(err)
				}
				nfiles, err := cleanHistory(n.disk.Durable(), n.id, model)
				if err != nil {
					t.Fatalf("node %d: %v", n.id, err)
				}
				cleaned += nfiles
			}
			total += cleaned
			if hits == 0 || misses == 0 {
				t.Fatalf("coverage: removed %d files, %d cache hits, %d cold reads", cleaned, hits, misses)
			}
			t.Logf("removed %d original segments; cache hits %d, cold reads %d; all six checkers passed", cleaned, hits, misses)
		})
	}
	if total == 0 {
		t.Fatal("no original segment was removed in 16 seeds")
	}
}

// The on-disk commit hint can lag applied by one entry. Check recovery
// against that exact prefix of the simulator's independent history, not
// against a state read back from the disk under test.
func durableModel(s *Sim, disk *simfs.Disk) (*mvcc.Store, error) {
	w, rec, err := wal.Open(disk.Durable())
	if err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	entries, err := s.committed(rec.HardState.Commit)
	if err != nil {
		return nil, err
	}
	var ops []proto.Message
	for _, entry := range entries {
		if entry.GetType() != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		if err := core.DecodeBatch(entry.Data, func(_, _, _ uint64, op proto.Message) error {
			ops = append(ops, op)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	model, _, _, err := replay(ops)
	return model, err
}

// cleanHistory uses the same WAL pack/root/remove protocol as the node.
func cleanHistory(d *simfs.Disk, id uint64, model *mvcc.Store) (removed int, err error) {
	w, rec, err := wal.Open(d)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	files := w.Cleanable(rec.Root.Index)
	var upto uint64
	for _, f := range files {
		if !f.Pack {
			upto = f.N
			removed++
		}
	}
	if upto == 0 {
		return 0, nil
	}
	checkpoint, err := w.Checkpoint(rec.Root.Index)
	if err != nil {
		return 0, err
	}
	want := map[mvcc.Ref][]byte{}
	pw, err := w.NewPack()
	if err != nil {
		return 0, errors.Join(err, checkpoint.Close())
	}
	err = core.WalkCheckpoint(checkpoint, rec.Root.Index, func(r mvcc.Ref) error {
		if _, ok := want[r]; ok {
			return nil
		}
		b, err := w.Value(r.Index, r.Off, r.Len)
		if err != nil {
			return err
		}
		want[r] = b
		where, ok := w.Where(r.Index, r.Off, r.Len)
		if ok && !where.Pack && where.N <= upto {
			return pw.Add(r.Index, r.Off, b)
		}
		return nil
	})
	err = errors.Join(err, checkpoint.Close())
	if err != nil {
		return 0, errors.Join(err, pw.Abort())
	}
	pack, err := pw.Finish()
	if err != nil {
		return 0, errors.Join(err, pw.Abort())
	}
	check := func(stage string) (err error) {
		crashed := d.Durable()
		rw, rr, err := wal.Open(crashed)
		if err != nil {
			return fmt.Errorf("%s recovery: %w", stage, err)
		}
		defer func() { err = errors.Join(err, rw.Close()) }()
		for ref, value := range want {
			got, err := rw.Value(ref.Index, ref.Off, ref.Len)
			if err != nil {
				return fmt.Errorf("%s lost checkpoint value at %d/%d: %w", stage, ref.Index, ref.Off, err)
			}
			if !bytes.Equal(got, value) {
				return fmt.Errorf("%s changed checkpoint value at %d/%d", stage, ref.Index, ref.Off)
			}
		}
		cfg := core.DefaultConfig(id, []uint64{1, 2, 3})
		tinyLimits(&cfg)
		n, err := core.New(cfg, rr, rw)
		if err != nil {
			return fmt.Errorf("%s core recovery: %w", stage, err)
		}
		if err := sameLatest(model, n.Store(), rw); err != nil {
			return fmt.Errorf("%s: %w", stage, err)
		}
		return nil
	}
	if err := check("pack synced"); err != nil {
		return 0, errors.Join(err, pack.Close())
	}
	w.InstallPacks(pack, nil)
	if err := check("root buffered"); err != nil {
		return 0, err
	}
	if err := w.Sync(); err != nil {
		return 0, err
	}
	if err := check("root synced"); err != nil {
		return 0, err
	}
	if err := w.RemoveCleaned(upto, nil); err != nil {
		return 0, err
	}
	for _, f := range files {
		if f.Pack {
			continue
		}
		for _, name := range d.DurableNames() {
			if name == wal.SegmentName(f.N) {
				return 0, fmt.Errorf("original segment %s still exists", name)
			}
		}
	}
	return removed, check("segments removed")
}
