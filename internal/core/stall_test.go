package core

import (
	"testing"
)

// TestStallAllowsForSize is #553: a leader whose 64 MiB of entries on
// their way to the disk take more than StallTicks election timeouts is
// not stuck if its disk writes at least stallBytesPerTick a tick, so it
// stays heard, whether the bytes are one append or forty written as one
// group; one that writes slower than that, or a small append that takes
// as long, goes silent.
func TestStallAllowsForSize(t *testing.T) {
	const election = 10
	base := int64(StallTicks * election)
	one := []int64{0}
	forty := make([]int64, 40)
	for _, c := range []struct {
		appends []int64
		bytes   int64
		at      int64
		silent  bool
	}{
		{appends: one, bytes: 100, at: base - 1, silent: false},
		{appends: one, bytes: 100, at: base, silent: true},
		{appends: one, bytes: 64 << 20, at: base, silent: false},
		{appends: one, bytes: 64 << 20, at: base + 64 - 1, silent: false},
		{appends: one, bytes: 64 << 20, at: base + 64, silent: true},
		{appends: forty, bytes: 40 << 20, at: base + 27, silent: false},
		{appends: forty, bytes: 40 << 20, at: base + 40, silent: true},
	} {
		n := &Node{cfg: Config{ElectionTicks: election}, isLeader: true, now: c.at, appending: c.appends, unpersisted: c.bytes}
		if got := n.stalled(); got != c.silent {
			t.Errorf("%d appends of %d bytes in all at tick %d: stalled %v, want %v", len(c.appends), c.bytes, c.at, got, c.silent)
		}
	}
}
