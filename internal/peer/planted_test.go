//go:build planted

package peer

import (
	"errors"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestPlanted catches B14, when $SIM_PLANTED names it, with the write
// deadline's own test, which must fail with the bug on. Other bugs belong
// to other packages.
func TestPlanted(t *testing.T) {
	if os.Getenv("SIM_PLANTED") != "B14" {
		t.Skip("not a bug of this package")
	}
	if simrun.Drive(t) {
		return
	}
	planted.Active = "B14"
	t.Cleanup(func() { planted.Active = "" })
	err := stalledPeerIsDropped(t)
	if err == nil {
		t.Fatal("B14 (the peer writer has no write deadline) was not caught")
	}
	if !errors.Is(err, errNotDropped) {
		t.Fatalf("B14: the scenario failed before it could catch anything: %v", err)
	}
	t.Logf("B14 (%s) caught: %v", planted.Bugs["B14"], err)
}
