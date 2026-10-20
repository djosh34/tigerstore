//go:build planted

package node

import (
	"errors"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestPlanted catches the bug that $SIM_PLANTED names: B13 with the slow
// watcher's own test, B26 with the fetch test, and B29 and B30 with seed 1
// of the concurrent cleaner scenario, each of which must fail with its bug
// on. Other bugs belong to other packages.
func TestPlanted(t *testing.T) {
	cases := map[string]struct {
		run func(*testing.T) error
		// caught is how the scenario fails when it catches the bug.
		caught error
	}{
		"B13": {slowWatcherIsCut, errSlowWatcher},
		"B26": {followerRetriesFetch, errFollowerStopped},
		"B29": {cleanSeed1, errLostValue},
		"B30": {cleanSeed1, errLostValue},
	}
	bug := os.Getenv("SIM_PLANTED")
	c, ok := cases[bug]
	if !ok {
		t.Skip("not a bug of this package")
	}
	if simrun.Drive(t) {
		return
	}
	planted.Active = bug
	t.Cleanup(func() { planted.Active = "" })
	err := c.run(t)
	if err == nil {
		t.Fatalf("%s (%s) was not caught", bug, planted.Bugs[bug])
	}
	if !errors.Is(err, c.caught) {
		t.Fatalf("%s: the scenario failed before it could catch anything: %v", bug, err)
	}
	t.Logf("%s (%s) caught: %v", bug, planted.Bugs[bug], err)
}

func cleanSeed1(t *testing.T) error {
	var err error
	simrun.Bubble(t, func(t *testing.T) { err = concurrentCleaning(t, 1) })
	return err
}
