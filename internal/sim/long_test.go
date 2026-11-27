package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wallclock"

	"github.com/djosh34/tigerstore/check"
)

// Run 45803627951 on 116ba5f4 used 219,708,153 weighted steps in about
// 149 search minutes, reaching revision 432,505 of 634,611 before its
// 155-minute wall stopped it. Its rate is about 24,600 steps/s; round
// down to 24,500. The separate 180M long budget then takes 122m27s.
// Add six minutes for simulation/other checks and thirty minutes of
// margin, rounded up to five minutes: 160m soft / 170m non-yield kill,
// within the campaign binary's 340m. The step budget must stop the search
// first. Seed 1's full check is #876's work, not evidence met by #872.
const (
	longWitnessEstimatedRate = 24_500
	longSeedSetupAllowance   = 6 * time.Minute
	longSeedWallMargin       = 30 * time.Minute
	longSeedWallQuantum      = 5 * time.Minute
	longBudgetTime           = time.Duration((longWitnessStepLimit+longWitnessEstimatedRate-1)/longWitnessEstimatedRate) * time.Second
	longSeedWallLimit        = time.Duration(int64((longBudgetTime+longSeedSetupAllowance+longSeedWallMargin+longSeedWallQuantum-1)/longSeedWallQuantum) * int64(longSeedWallQuantum))
)

// TestBusySeeds runs TestSeeds's seeds with their faults for 1,280 s
// instead of 20 s, every checker on each: the busy seed whose peak memory
// #500 tracks (the length #485 measured). It skips unless $SIM_SEEDS says
// how many seeds to run: a seed takes minutes.
func TestBusySeeds(t *testing.T) {
	longSeeds(t, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Duration = 1280 * Second
		return cfg
	}, 0)
}

// TestCampaignShapeSeeds runs seeds of the long campaign's shape: TestSeeds's
// faults about 16 times further apart, for 4 simulated hours, every
// checker on each (#500). It skips unless $SIM_SEEDS says how many seeds
// to run: a seed takes up to hours.
func TestCampaignShapeSeeds(t *testing.T) {
	longSeeds(t, func(seed uint64) Config {
		cfg := DefaultConfig(seed)
		cfg.Duration = 4 * 3600 * Second
		spreadFaults(&cfg.Faults, 16)
		return cfg
	}, longSeedWallLimit)
}

// longSeeds runs $SIM_SEEDS seeds of the configs that config gives, from
// $SIM_SEED or seed 1, with every checker.
func longSeeds(t *testing.T, config func(seed uint64) Config, wallLimit time.Duration) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: a long seed takes minutes to hours")
	}
	budget := longSeedBudget(wallLimit)
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		w, closeOutput, err := simrun.CreateOutput("heap878-stats")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = closeOutput() }()
		heap878Writer = w
		check.Heap878Phase = heap878Phase
		heap878PhaseHook = heap878Phase
		heap878Phase("simulation-start")
		res := run(t, config(seed))
		heap878Phase("simulation-and-end-checks-complete")
		longCheckers(t, res, budget)
		t.Logf("%+v, %d committed writes", res.Stats, len(res.Log))
		return res.Stats
	}, func(uint64, Stats) bool { return false }, simrun.WithWallLimit(func(uint64) time.Duration { return wallLimit }))
}

func longSeedBudget(wallLimit time.Duration) int64 {
	if wallLimit > 0 {
		return longWitnessStepLimit
	}
	return witnessStepLimit
}

// longCheckers logs the revision witness's steps before handling its
// verdict, so Actions records the full check's count (or its limit).
func longCheckers(t *testing.T, res *Result, budget int64) {
	t.Helper()
	if err := independentWithSteps(res, func(steps int64, cpu, wall time.Duration) {
		t.Logf("revision witness used %d weighted steps (budget %d, native CPU %s, search wall %s, %.2f steps/s)", steps, budget, cpu, wall, float64(steps)/wall.Seconds())
	}, budget); err != nil {
		if heap878Stopped {
			t.Logf("HEAP878 DIAGNOSTIC STOP (not full-seed evidence): %v", err)
			return
		}
		t.Fatal(err)
	}
}

// Scratch-only heap diagnostics. No GC tuning and no forced GC: observe
// the current heap, then stop the diagnostic before the runner is lost.
var heap878Writer io.Writer
var heap878Current string
var heap878Profile int
var heap878NextGrowth uint64
var heap878LastPoll int64
var heap878Stopped bool

func heap878Phase(phase string) {
	if heap878Writer == nil {
		return
	}
	force := phase != heap878Current
	heap878Current = phase
	heap878Snapshot(force)
}

func heap878Snapshot(force bool) {
	simrun.Aside(func() { heap878Sample(force) })
}

func heap878Sample(force bool) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if !force && m.HeapInuse < heap878NextGrowth {
		return
	}
	heap878NextGrowth = (m.HeapInuse/(1<<30) + 1) << 30
	if m.HeapInuse >= 10<<30 {
		heap878Stopped = true
	}
	if heap878Profile >= 64 {
		return
	}
	name := fmt.Sprintf("heap878-%02d", heap878Profile)
	heap878Profile++
	w, closeOutput, err := simrun.CreateOutput(name)
	if err == nil {
		err = pprof.WriteHeapProfile(w)
		err = errors.Join(err, closeOutput())
	}
	_ = json.NewEncoder(heap878Writer).Encode(struct {
		Profile, Phase                         string
		HostNS                                 int64
		HeapInuse, HeapAlloc, Sys, HeapObjects uint64
		NumGC                                  uint32
		Stopped                                bool
		Error                                  string
	}{name, heap878Current, wallclock.Now(), m.HeapInuse, m.HeapAlloc, m.Sys, m.HeapObjects, m.NumGC, heap878Stopped, fmt.Sprint(err)})
}

func heap878Poll() bool {
	if heap878Writer != nil && !heap878Stopped && wallclock.Now()-heap878LastPoll >= int64(500*time.Millisecond) {
		heap878LastPoll = wallclock.Now()
		heap878Snapshot(false)
	}
	return heap878Stopped
}

// TestHostErrorComesFirst checks that a run whose pages on the runner's
// disk failed returns that error, joined with the run's own: a page that
// failed to read reads as zeros, which can fail the run as if a node were
// wrong.
func TestHostErrorComesFirst(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	errHost := errors.New("the runner's disk failed")
	errRun := errors.New("a node is wrong")
	cfg := CalmConfig(1)
	cfg.Duration = 2 * Second
	_, err := runWith(t, cfg, func(s *Sim) error {
		s.pages.err = errHost
		return errRun
	})
	if !errors.Is(err, errHost) || !errors.Is(err, errRun) {
		t.Errorf("the run gave %v, want both %v and %v", err, errHost, errRun)
	}
}

// TestCheckpointCountsTornDisks checks that a disk copy for the cold
// restarts counts as damaged while its node has a torn crash image that no
// start recovered, which recovery may refuse, as for a damaged node, and
// as good otherwise: a cold restart must not start from a log recovery
// refuses, and the copy is never opened to find out (#500).
func TestCheckpointCountsTornDisks(t *testing.T) {
	s := &Sim{nodes: []*node{
		{id: 1, disk: simfs.New(), tornCrash: true},
		{id: 2, disk: simfs.New(), corrupted: true},
		{id: 3, disk: simfs.New()},
	}}
	c := s.newCheckpoint()
	if want := []bool{true, true, false}; !slices.Equal(c.bad, want) {
		t.Errorf("the checkpoint's bad disks are %v, want %v", c.bad, want)
	}
}

// TestHistoryDecodesAgain checks that a run whose checks dropped its
// history, as a long run's do (independent), decodes the same history
// again for a report.
func TestHistoryDecodesAgain(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	res := run(t, DefaultConfig(1))
	var want, got bytes.Buffer
	writeHistory(&want, res)
	res.History = nil
	history, err := res.history()
	if err != nil {
		t.Fatal(err)
	}
	res.History = history
	writeHistory(&got, res)
	if want.Len() == 0 || !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Errorf("the history decoded again holds %d bytes, the run's %d, and they differ", got.Len(), want.Len())
	}
}

// TestCheckableReleasing checks that the checks of a run get the same
// history whether or not they drop the run's own as they convert it, as a
// long run's do, and that the run's history is gone after.
func TestCheckableReleasing(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	res := run(t, DefaultConfig(1))
	want, err := res.Checkable()
	if err != nil {
		t.Fatal(err)
	}
	got, err := res.checkable(true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Error("the history the checks get differs when the run's is dropped as it is converted")
	}
	if res.History != nil {
		t.Errorf("the run still holds %d calls", len(res.History))
	}
}

// TestDroppedHistoryOnError checks that checks that drop a run's history
// as they convert it drop all of it when a call fails to convert, so a
// report decodes the whole history again rather than what is left.
func TestDroppedHistoryOnError(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	res := run(t, DefaultConfig(1))
	n := len(res.History)
	// A sort order on a range etcd's history model refuses.
	res.History = append(res.History, &Record{req: &pb.RangeRequest{Key: []byte("k"), SortOrder: pb.RangeRequest_ASCEND}})
	if _, err := res.checkable(true); err == nil {
		t.Fatal("a call that cannot convert converted")
	}
	if res.History != nil {
		t.Fatalf("after the error the run holds %d calls", len(res.History))
	}
	history, err := res.history()
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != n || slices.Contains(history, nil) {
		t.Errorf("the history decoded again holds %d calls, want %d, none nil", len(history), n)
	}
}

// TestHistoryChunkErr checks that a history decoded again for a report
// returns the error of a chunk on the runner's disk that failed to read.
func TestHistoryChunkErr(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	res := run(t, DefaultConfig(1))
	res.History = nil
	errGone := errors.New("the runner's disk is gone")
	res.chunks.err = errGone
	if _, err := res.history(); !errors.Is(err, errGone) {
		t.Errorf("decoding again gave %v, want %v", err, errGone)
	}
}

// TestLongRunHashes checks that a long run's hashes, which cover its
// history, are the same after its checks dropped the history.
func TestLongRunHashes(t *testing.T) {
	if simrun.Drive(t, 1) {
		return
	}
	res := run(t, DefaultConfig(1))
	want := hashes(res)
	res.long = true
	if err := independent(res); err != nil {
		t.Fatal(err)
	}
	if res.History != nil {
		t.Fatal("the checks of a long run kept its history")
	}
	if got := runHashes(res); got != want {
		t.Errorf("after the checks the hashes are %s, want %s", got, want)
	}
}
