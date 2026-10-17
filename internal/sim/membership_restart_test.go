package sim

import (
	"strconv"
	"testing"
)

// These seeds exercise cold restarts during membership changes, including
// a removed member that has not learned its removal.
func TestMembershipColdRestart(t *testing.T) {
	for _, seed := range []uint64{26, 88, 165} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			res := run(t, membershipConfig(seed))
			checkers(t, res)
		})
	}
}
