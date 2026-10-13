//go:build planted

package node

import (
	"errors"
	"flag"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
)

var plantedBug = flag.String("planted", "", "name of the planted bug TestPlanted switches on")

// TestPlanted catches B13 with the slow watcher's own test, which must
// fail with the bug on. Other bugs belong to other packages.
func TestPlanted(t *testing.T) {
	if *plantedBug != "B13" {
		t.Skip("not a bug of this package")
	}
	planted.Active = "B13"
	t.Cleanup(func() { planted.Active = "" })
	err := slowWatcherIsCut(t)
	if err == nil {
		t.Fatal("B13 (the node driver waits for a slow watch stream) was not caught")
	}
	if !errors.Is(err, errSlowWatcher) {
		t.Fatalf("B13: the scenario failed before it could catch anything: %v", err)
	}
	t.Logf("B13 (%s) caught: %v", planted.Bugs["B13"], err)
}
