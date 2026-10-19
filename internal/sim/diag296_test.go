//go:build planted

package sim

import (
	"regexp"
	"testing"

	"github.com/djosh34/tigerstore/internal/planted"
)

// Hold a heartbeat in the failed node's inbox as cancellation releases
// Turn. The check must keep its finding without running Raft again.
func TestDiag296B20QueuedCommit(t *testing.T) {
	planted.Active, activePlanted, diag296QueueHeartbeat = "B20", "B20", true
	t.Cleanup(func() { planted.Active, activePlanted, diag296QueueHeartbeat = "", "", false })
	_, err := runSeed(t, DefaultConfig(1))
	if err == nil || !regexp.MustCompile(plantedCases["B20"].want).MatchString(err.Error()) {
		t.Fatalf("B20 was not caught by its memory check: %v", err)
	}
	t.Log(err)
}
