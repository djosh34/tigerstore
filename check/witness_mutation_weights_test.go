package check

import "testing"

func TestMutationWeightAndBudget(t *testing.T) {
	r := dueShape(0)
	for range 1000 {
		if r.mutationStep(8) {
			t.Fatal("small map stopped")
		}
	}
	if r.w.steps != 0 || r.w.workTicks != 0 {
		t.Fatal("small-map writes lost their existing scan-only accounting")
	}
	for range 1000 {
		if r.mutationStep(9) {
			t.Fatal("unbounded large map stopped")
		}
	}
	if r.w.steps != 63 || r.w.workTicks != 16 {
		t.Fatalf("large-map weight: steps %d, ticks %d, want 63 and 16", r.w.steps, r.w.workTicks)
	}
	r = dueShape(0)
	r.w.maxSteps = 3
	for range 1000 {
		if r.mutationStep(9) {
			break
		}
	}
	if !r.w.stopped || r.w.steps != 4 || !r.mutationStep(0) {
		t.Fatalf("mutation bypassed/forgot stop: %v, %d steps", r.w.stopped, r.w.steps)
	}
}

func TestMutationWeightKeepsPolling(t *testing.T) {
	r := dueShape(0)
	polls := 0
	r.w.stop = func() bool { polls++; return true }
	for range 1024 * 64 {
		if r.mutationStep(9) {
			break
		}
	}
	if !r.w.stopped || polls != 1 || r.w.workTicks != 1024 {
		t.Fatalf("large-map writes delayed cancellation: stopped %v, polls %d, ticks %d", r.w.stopped, polls, r.w.workTicks)
	}
}
