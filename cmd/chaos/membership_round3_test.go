package main

import "testing"

func TestRound3ReplacementLast(t *testing.T) {
	for seed := range uint64(100) {
		steps := plan(seed, []string{learnerLeader, rollingLoss, tripleLoss})
		if len(steps) != 3 || steps[2].Fault != learnerLeader {
			t.Fatalf("seed %d: replacement must follow all proxy cuts: %+v", seed, steps)
		}
	}
}
