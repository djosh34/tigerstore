package sim

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// synthRevTop is the largest revision a synthetic image's store has:
// just under the bound a store loads from a checkpoint (mvcc's
// maxLoadRev, 2^62). Past it the model, main's store, which the checkers
// run against, has too little room for its own arithmetic.
const synthRevTop = 1<<62 - 1

// syntheticSpec draws seed's synthetic image (synthetic.go) for a cluster
// of voters, with keys keys:
//   - its revision 2^24 to 2^40 below synthRevTop, room for every write
//     of its forks;
//   - its Raft index and term, each just past 2^63, where an int64 of
//     them turns negative, or 2^24 to 2^40 (the index) or 2^16 to 2^32
//     (the term) below 2^64, where a sum of them wraps;
//   - its members' IDs within 2^16 of 2^32, the largest wl takes;
//   - 1 to 64 leases with IDs within 2^20 of the largest int64, and
//     TTLs of 1 to 60 minutes; a quarter of the keys are on one;
//   - Kubernetes-like keys, last written within 2^30 revisions of the
//     image, and created up to 2^40 revisions before that.
func syntheticSpec(seed uint64, voters, keys int) *synthSpec {
	rng := rand.New(rand.NewPCG(seed, 0x73796e7468))
	high := func(lo, hi int64) uint64 {
		if rng.IntN(2) == 0 {
			return 1<<63 + uint64(rng.Int64N(hi))
		}
		return math.MaxUint64 - uint64(logUniform(rng, lo, hi))
	}
	spec := &synthSpec{index: high(1<<24, 1<<40), term: high(1<<16, 1<<32)}
	for len(spec.ids) < voters {
		// wl's member IDs have 32 bits: its own (newMemberID), and the
		// proposer a batch names; a larger one it takes, but never
		// answers its writes (#841).
		if id := math.MaxUint32 - rng.Uint64N(1<<16); !slices.Contains(spec.ids, id) {
			spec.ids = append(spec.ids, id)
		}
	}
	st := &synthStore{rev: synthRevTop - logUniform(rng, 1<<24, 1<<40), leases: map[int64]int64{}}
	for n := 1 + rng.IntN(64); len(st.leases) < n; {
		st.leases[math.MaxInt64-rng.Int64N(1<<20)] = 60 + rng.Int64N(3540)
	}
	ids := slices.Sorted(maps.Keys(st.leases))
	resources := []string{"pods", "configmaps", "secrets", "events", "leases", "endpointslices"}
	mods := map[int64]bool{}
	for i := range keys {
		mod := st.rev - rng.Int64N(1<<30)
		for mods[mod] {
			mod = st.rev - rng.Int64N(1<<30)
		}
		mods[mod] = true
		// A key written once has version 1 and was created where it was
		// last written; one written again has a version from 2 to the
		// revisions between its creation and its last write.
		create, version := mod, int64(1)
		if rng.IntN(4) != 0 {
			create = mod - 1 - rng.Int64N(1<<40)
			version = 2 + rng.Int64N(min(mod-create, 1<<20))
		}
		k := mvcc.Seed{
			Key:     fmt.Appendf(nil, "/registry/%s/synth/obj-%05d", resources[rng.IntN(len(resources))], i),
			Value:   fmt.Appendf(nil, "%0*d", 1+rng.IntN(2048), i),
			Create:  create,
			Mod:     mod,
			Version: version,
		}
		if rng.IntN(4) == 0 {
			k.Lease = ids[rng.IntN(len(ids))]
		}
		st.seeds = append(st.seeds, k)
	}
	slices.SortFunc(st.seeds, func(a, b mvcc.Seed) int { return cmp.Compare(string(a.Key), string(b.Key)) })
	spec.store = st
	return spec
}

// synthYears is how long after its synthetic image seed's first fork
// starts, at least: 1 to 30 years, so that every fork runs with the
// clock years past the image.
func synthYears(seed uint64) time.Duration {
	rng := rand.New(rand.NewPCG(seed, 0x79656172))
	return time.Duration(logUniform(rng, int64(365*24*time.Hour), int64(30*365*24*time.Hour)))
}

// synthConfig is the run seed's synthetic image is restored for, whose
// voters, memory budget and disks its forks take (forkConfig): an aging
// run's (agingConfig), with certificates valid for the years its forks
// start after it, and disks sized for keys keys.
func synthConfig(seed uint64, keys int) Config {
	cfg := agingConfig(seed)
	cfg.Objects = keys
	cfg.Faults.DiskBudget = max(16<<20, int64(keys)*8<<10)
	cfg.Faults.DiskCapacity = int(2 * cfg.Faults.DiskBudget)
	return cfg
}

// synthForks restores seed's synthetic image of keys keys for base, then
// runs forks of it, the first synthYears after it, and returns their
// stats added up. $SIM_FORK=k runs fork k alone.
func synthForks(t *testing.T, seed uint64, base Config, keys, forks int, fork func(Config, uint64) Config) Stats {
	t.Helper()
	only := forkOnly(t, forks)
	var img *image
	if !t.Run("image", func(t *testing.T) {
		spec := syntheticSpec(seed, base.voters(), keys)
		var err error
		simrun.BubbleFor(t, time.Hour, func(*testing.T) {
			var s *Sim
			if s, err = newSim(base); err == nil {
				img, err = s.restoreImage(spec)
			}
		})
		if err != nil {
			t.Fatalf("synthetic image %d: %v", seed, err)
		}
		t.Logf("synthetic image %d: %d voters, members %v; index %d, term %d; revision %d, %d keys, %d leases from %d",
			seed, base.voters(), spec.ids, spec.index, spec.term, spec.store.rev, len(spec.store.seeds), len(spec.store.leases), slices.Min(img.start.Leases))
	}) {
		t.FailNow()
	}
	years := synthYears(seed)
	return runForks(t, seed, base, img, forks, only, fork, func(i int) time.Duration { return years + forkStart(seed, i) })
}

// TestSyntheticForks restores a synthetic image for each of $SIM_SEEDS
// seeds from $SIM_SEED (syntheticSpec), of 200 to 5,000 keys, and runs
// forksPerImage forks of it (forkConfig), years after it, every checker
// on each (#502). $SIM_FORK=k runs only fork k. It skips unless
// $SIM_SEEDS is set, as TestAgedForks does.
func TestSyntheticForks(t *testing.T) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: the campaigns say how many images to restore")
	}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		keys := synthKeys(seed)
		return synthForks(t, seed, synthConfig(seed, keys), keys, forksPerImage, forkConfig)
	}, nil)
}

// synthKeys is how many keys TestSyntheticForks's image of seed has.
func synthKeys(seed uint64) int {
	return int(logUniform(rand.New(rand.NewPCG(seed, 0x6b657973)), 200, 5000))
}

// The stores TestSyntheticForks draws are ones the checkers take
// (history.Start): a key of version 1 was created where it was last
// written (seed 223 drew one that was not, #843's review).
func TestSyntheticStoresAreValidStarts(t *testing.T) {
	for seed := range uint64(400) {
		spec := syntheticSpec(seed+1, 3, synthKeys(seed+1))
		if err := (&history.History{Start: spec.store.start()}).Validate(); err != nil {
			t.Fatalf("seed %d: %v", seed+1, err)
		}
	}
}

// A restore onto a simulated disk outlasts a power loss right after it:
// the data dir keeps its checkpoint, its pointer and its log's directory
// (#842).
func TestARestoreOutlastsAPowerLoss(t *testing.T) {
	spec := syntheticSpec(1, 3, 20)
	cfg := synthConfig(1, 20)
	var img *image
	var err error
	simrun.BubbleFor(t, time.Hour, func(*testing.T) {
		var s *Sim
		if s, err = newSim(cfg); err == nil {
			img, err = s.restoreImage(spec)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range img.disks {
		names := d.DurableNames()
		if !slices.Contains(names, "ckpt/CURRENT") || !slices.ContainsFunc(names, func(n string) bool { return strings.HasSuffix(n, ".ckpt") }) || !d.IsDir("wal") {
			t.Errorf("member %d's disk after a power loss holds %v", spec.ids[i], names)
		}
	}
}

// The record of a synthetic image's forks starts at its index: it holds
// no entry before it, takes the snapshot's own index, and lists the
// committed entries from the one after it, to the largest index.
func TestARecordStartsAtTheImage(t *testing.T) {
	for _, from := range []uint64{1<<63 + 10, math.MaxUint64 - 2} {
		s := &Sim{log: &entryLog{from: from}}
		s.log.put(from-1, logRec{term: 1, known: true})
		if _, ok := s.log.get(from - 1); ok {
			t.Fatalf("from %d: an entry before the image was kept", from)
		}
		s.log.put(from, logRec{term: 1})
		if _, ok := s.log.get(from); !ok {
			t.Fatalf("from %d: the image's own index was not kept", from)
		}
		if ents, err := s.committed(from); err != nil || len(ents) != 0 {
			t.Fatalf("from %d: committed up to the image: %v, %v", from, ents, err)
		}
		last := from + 2
		for i := from + 1; i <= last && i != 0; i++ {
			s.log.put(i, logRec{term: 2, known: true, data: s.log.data.add([]byte{byte(i)})})
		}
		ents, err := s.committed(last)
		if err != nil || len(ents) != 2 || ents[1].GetIndex() != last {
			t.Fatalf("from %d: committed to %d: %d entries, %v", from, last, len(ents), err)
		}
	}
}

// A model that starts from a synthetic store takes an empty serializable
// read of the newest revision answered at revision 1, as a member added
// with an empty disk gives, but no read that asks for revision 1, no
// linearizable one, and none that found keys there.
func TestAReadAtRevisionOneOfASyntheticModel(t *testing.T) {
	st, err := core.NewStateAt(2, &mvcc.Heap{}, 1<<40, []mvcc.Seed{{Key: []byte("k"), Value: []byte("v"), Create: 1 << 40, Mod: 1 << 40, Version: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	empty := &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 1}}
	found := &pb.RangeResponse{Header: &pb.ResponseHeader{Revision: 1}, Kvs: []*mvccpb.KeyValue{{Key: []byte("k")}}, Count: 1}
	for _, c := range []struct {
		name string
		req  *pb.RangeRequest
		got  *pb.RangeResponse
		ok   bool
	}{
		{"serializable, empty", &pb.RangeRequest{Key: []byte("k"), Serializable: true}, empty, true},
		{"serializable, keys", &pb.RangeRequest{Key: []byte("k"), Serializable: true}, found, false},
		{"linearizable, empty", &pb.RangeRequest{Key: []byte("k")}, empty, false},
		{"at revision 1, empty", &pb.RangeRequest{Key: []byte("k"), Revision: 1, Serializable: true}, empty, false},
	} {
		if err := sameRead(st.Store(), c.req, c.got); (err == nil) != c.ok {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// TestSyntheticFork restores a synthetic image of 300 keys and runs one
// fork of 30 s from it, every checker on it, as CI's short case.
func TestSyntheticFork(t *testing.T) {
	simrun.Each(t, []uint64{1}, func(t *testing.T, seed uint64) Stats {
		return synthForks(t, seed, synthConfig(seed, 300), 300, 1, func(base Config, seed uint64) Config {
			cfg := forkConfig(base, seed)
			cfg.Duration = 30 * Second
			return cfg
		})
	}, nil)
}
