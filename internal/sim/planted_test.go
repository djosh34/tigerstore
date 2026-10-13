//go:build planted

package sim

import (
	"errors"
	"flag"
	"fmt"
	"testing"
	"testing/cryptotest"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/internal/planted"
)

var plantedBug = flag.String("planted", "", "name of the planted bug TestPlanted switches on")

// plantedCase is how a planted bug must be caught: within seeds runs of
// cfg, by a simulator rule, by one of the independent checkers, or by
// extra. The seed budgets are the semantics pre-work's on #47.
type plantedCase struct {
	seeds int
	cfg   func(seed uint64) Config
	extra func(*Result) error
}

func stalledLeaderConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Faults = Faults{}
	cfg.StallLeaderAt = 5 * Second
	cfg.LiveFrom = cfg.StallLeaderAt + 10*Second
	cfg.Duration = 25 * Second
	return cfg
}

var plantedCases = map[string]plantedCase{
	"B01a": {seeds: 64, cfg: DefaultConfig},
	"B01b": {seeds: 64, cfg: DefaultConfig},
	"B02":  {seeds: 128, cfg: DefaultConfig},
	"B03":  {seeds: 128, cfg: DefaultConfig},
	"B04a": {seeds: 256, cfg: DefaultConfig},
	"B04b": {seeds: 256, cfg: DefaultConfig},
	"B06a": {seeds: 32, cfg: DefaultConfig},
	"B06b": {seeds: 32, cfg: DefaultConfig},
	"B06c": {seeds: 32, cfg: DefaultConfig},
	"B06d": {seeds: 32, cfg: DefaultConfig},
	"B07":  {seeds: 64, cfg: DefaultConfig},
	"B08":  {seeds: 32, cfg: DefaultConfig},
	"B09a": {seeds: 16, cfg: DefaultConfig},
	"B09b": {seeds: 16, cfg: DefaultConfig},
	"B10":  {seeds: 64, cfg: DefaultConfig},
	"B11":  {seeds: 64, cfg: DefaultConfig},
	"B12":  {seeds: 256, cfg: DefaultConfig},
	"B17": {seeds: 64, cfg: stalledLeaderConfig, extra: func(r *Result) error {
		if r.Stats.StalledStops == 0 {
			return errors.New("the stalled leader never fenced itself")
		}
		return nil
	}},
	"B18": {seeds: 16, cfg: DefaultConfig},
}

// independent runs the checkers of package check on a run.
func independent(res *Result) error {
	h, err := res.Checkable()
	if err != nil {
		return err
	}
	return errors.Join(check.Durable(h), check.Revisions(h), check.Watches(h))
}

// TestPlantedInventory requires a case for every planted bug.
func TestPlantedInventory(t *testing.T) {
	for name := range planted.Bugs {
		if _, ok := plantedCases[name]; !ok {
			t.Errorf("planted bug %s (%s) has no case", name, planted.Bugs[name])
		}
	}
	for name := range plantedCases {
		if _, ok := planted.Bugs[name]; !ok {
			t.Errorf("case %s plants no known bug", name)
		}
	}
}

// TestPlanted switches on the bug named by -planted and fails unless a
// check catches it within its seed budget. A crash of the test binary is
// a failure too, not a catch.
func TestPlanted(t *testing.T) {
	name := *plantedBug
	if name == "" {
		t.Skip("no -planted bug selected")
	}
	c, ok := plantedCases[name]
	if !ok {
		t.Fatalf("unknown planted bug %q", name)
	}
	planted.Active = name
	t.Cleanup(func() { planted.Active = "" })
	for seed := uint64(1); seed <= uint64(c.seeds); seed++ {
		cryptotest.SetGlobalRandom(t, seed)
		res, err := runCatching(c.cfg(seed))
		if err == nil {
			err = independent(res)
		}
		if err == nil && c.extra != nil {
			err = c.extra(res)
		}
		if err != nil {
			t.Logf("%s (%s) caught by seed %d: %v", name, planted.Bugs[name], seed, err)
			return
		}
	}
	t.Fatalf("%s (%s) was not caught within %d seeds", name, planted.Bugs[name], c.seeds)
}

// runCatching runs a seed and turns a panic into an error: Raft panics on
// broken invariants, which stops a real node too.
func runCatching(cfg Config) (res *Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("seed %d panicked: %v", cfg.Seed, p)
		}
	}()
	return Run(cfg)
}
