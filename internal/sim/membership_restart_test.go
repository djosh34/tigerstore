package sim

import (
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// These seeds exercise cold restarts during membership changes, including
// a removed member that has not learned its removal.
func TestMembershipColdRestart(t *testing.T) {
	if simrun.Drive(t) {
		return
	}
	for _, seed := range []uint64{26, 88, 165} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			res := run(t, membershipConfig(seed))
			checkers(t, res)
		})
	}
}
