package main

import (
	"errors"
	"testing"
)

// TestRampMeasuresEveryStep runs -ramp over fake measurements. It measures
// every one of -ramp-steps doublings and returns the best, so two stores
// level at every step report the same max. Before, it stopped at the first
// step that climbed less than 5%, and the pair below, which differs by
// 0.2% at one step, reported maxes a doubling apart (#644, #645's review).
func TestRampMeasuresEveryStep(t *testing.T) {
	run := func(steps int, tputs []float64, fail int) (*result, []int, error) {
		var asked []int
		cfg := config{streams: 256, ramp: true, rampSteps: steps}
		res, err := ramped(cfg, func(streams int) (*result, error) {
			i := len(asked)
			asked = append(asked, streams)
			if i == fail {
				return &result{Throughput: tputs[i]}, errors.New("failed")
			}
			return &result{Throughput: tputs[i]}, nil
		})
		return res, asked, err
	}
	a, askedA, _ := run(4, []float64{100, 105.1, 110.2, 115.5, 122}, -1)
	b, _, _ := run(4, []float64{100, 104.9, 110.2, 115.5, 122}, -1)
	if a.Throughput != b.Throughput {
		t.Errorf("two stores level at every step but one report %v and %v", a.Throughput, b.Throughput)
	}
	if len(askedA) != 5 || askedA[0] != 256 || askedA[4] != 4096 {
		t.Errorf("4 doublings from 256 measured %v, want 256 to 4096", askedA)
	}
	if !a.RampClimbing {
		t.Error("a ramp whose last step climbed 5% is not marked as still climbing")
	}
	// The #642 cell: flat after 512, so the best is 512's and not climbing.
	c, _, _ := run(3, []float64{97356, 101939, 101500, 100800}, -1)
	if c.Throughput != 101939 || c.RampClimbing || len(c.Ramp) != 4 {
		t.Errorf("a flat ramp gave %v over %d steps, climbing %v; want 101939 over 4, not climbing", c.Throughput, len(c.Ramp), c.RampClimbing)
	}
	// A step 5% below the best is past the peak: the ramp stops there,
	// before the steps that overload the store further (#647). The k8s
	// curve of #645's A/A bench at RTT 0.
	k, asked, _ := run(5, []float64{45731, 69036, 81118, 75200, 72494, 59229}, -1)
	if k.Throughput != 81118 || len(asked) != 4 {
		t.Errorf("a curve past its peak gave %v after %d steps, want 81118 after 4", k.Throughput, len(asked))
	}
	// A step less than 5% below goes on.
	if _, asked, _ := run(3, []float64{100, 110, 105, 108}, -1); len(asked) != 4 {
		t.Errorf("a step 4.5%% below the best stopped the ramp after %d steps", len(asked))
	}
	// A failed step ends the ramp and is its result.
	if _, asked, err := run(3, []float64{100, 110, 120, 130}, 1); err == nil || len(asked) != 2 {
		t.Errorf("a failure at the second step gave %v after %d steps", err, len(asked))
	}
}
