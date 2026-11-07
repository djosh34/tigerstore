package sim

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/djosh34/tigerstore/check"
)

// decides reports whether err is a finding about the run: not a stop for
// lack of real time, and not a witness search that ran out of time or
// steps, which say nothing of the history. A joined error decides if any
// of its parts does, unless the runner's own disk failed: the nodes may
// then have read zeros, so no failure of that run decides.
func decides(err error) bool {
	if errors.Is(err, errRunnerDisk) {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(joined.Unwrap(), decides)
	}
	return !errors.Is(err, errWallLimit) && !errors.Is(err, check.ErrWitnessTimeout)
}

// TestDecides checks which failures can catch a planted bug: a stop for
// lack of real time and a witness search that ran out cannot, alone or
// together, even if the control passes; a checker's finding can, also
// joined with one of them, but not on a run whose runner's disk failed.
func TestDecides(t *testing.T) {
	stop := fmt.Errorf("seed 1 at 900 ms: %w", errWallLimit)
	ranOut := fmt.Errorf("witness: %w", fmt.Errorf("witness check (from the log) of 9 operations: %w", check.ErrWitnessTimeout))
	finding := errors.New("revisions: revision 4 has 2 keys, the cluster has 3")
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"stop for real time", stop, false},
		{"witness ran out", ranOut, false},
		{"both", errors.Join(stop, ranOut), false},
		{"a finding", finding, true},
		{"a finding and a witness that ran out", errors.Join(ranOut, finding), true},
		{"the runner's disk", fmt.Errorf("%w: the disks' pages: no space left on device", errRunnerDisk), false},
		{"a finding on a failed runner's disk", errors.Join(fmt.Errorf("%w: the history's chunks: EIO", errRunnerDisk), finding), false},
	} {
		if got := decides(c.err); got != c.want {
			t.Errorf("%s: decides is %v, want %v", c.name, got, c.want)
		}
	}
}
