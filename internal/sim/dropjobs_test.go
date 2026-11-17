package sim

import "testing"

// A process that goes down with an append on its way that replaces
// entries it reported durable may have written it: the next start that
// crashes before it recovers must not count on the replaced entries
// (seed 19458 of TestWLFaultSeeds: a write of entry 4 of a new term,
// short on a full disk, then a crash at a step of the next start).
func TestAStartThatCrashesKeepsTheFloorOfTheJobsItsProcessLost(t *testing.T) {
	n := &node{durableLast: 8, pending: []pendingJob{{first: 4, replaces: true}}}
	if f := n.floor(); f != 3 {
		t.Fatalf("the floor with the replacing append on its way is %d, want 3", f)
	}
	n.dropJobs()
	if len(n.pending) != 0 {
		t.Fatalf("%d jobs still pending after the process went down", len(n.pending))
	}
	if f := n.floor(); f != 3 {
		t.Fatalf("the floor of the next start is %d, want 3: the replacing append may be on the disk", f)
	}
}
