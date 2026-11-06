package sim

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
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
	})
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
	})
}

// longSeeds runs $SIM_SEEDS seeds of the configs that config gives, from
// $SIM_SEED or seed 1, with every checker.
func longSeeds(t *testing.T, config func(seed uint64) Config) {
	if os.Getenv("SIM_SEEDS") == "" {
		t.Skip("set SIM_SEEDS: a long seed takes minutes to hours")
	}
	simrun.Seeds(t, 1, 0, func(t *testing.T, seed uint64) Stats {
		res := run(t, config(seed))
		checkers(t, res)
		t.Logf("%+v, %d committed writes", res.Stats, len(res.Log))
		return res.Stats
	}, func(uint64, Stats) bool { return false })
}

// TestHostErrorComesFirst checks that a run whose pages on the runner's
// disk failed returns that error, joined with the run's own: a page that
// failed to read reads as zeros, which can fail the run as if a node were
// wrong.
func TestHostErrorComesFirst(t *testing.T) {
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

// TestCheckpointKnowsRefusedLogs checks that a disk copy for the cold
// restarts counts as damaged when its node has a torn crash image that no
// start recovered and recovery refuses its log: before the node's next
// start finds out, a cold restart must not start from it. A torn image
// that recovery takes, and a refused log without one, which the node's
// damage marks, keep their disks good.
func TestCheckpointKnowsRefusedLogs(t *testing.T) {
	refused := simfs.New()
	w, _, err := wal.Open(refused)
	if err != nil {
		t.Fatal(err)
	}
	// A commit past the last entry is a log recovery refuses.
	w.Append(&wal.HardState{Term: 1, Commit: 3}, nil)
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	s := &Sim{nodes: []*node{
		{id: 1, disk: refused, tornCrash: true},
		{id: 2, disk: simfs.New(), tornCrash: true},
		{id: 3, disk: refused},
	}}
	c := s.newCheckpoint()
	if want := []bool{true, false, false}; !slices.Equal(c.bad, want) {
		t.Errorf("the checkpoint's bad disks are %v, want %v", c.bad, want)
	}
	if err := s.failure(); err != nil {
		t.Error(err)
	}
}

// TestHistoryDecodesAgain checks that a run whose checks dropped its
// history, as a long run's do (independent), decodes the same history
// again for a report.
func TestHistoryDecodesAgain(t *testing.T) {
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
