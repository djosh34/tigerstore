package main

import (
	"errors"
	"fmt"
	"testing"
)

// TestRampSteps runs -ramp over fake measurements. It measures the
// doublings of -ramp-steps and returns the best, whatever a step climbed,
// so two stores level at every step report the same max: before, it
// stopped at the first step that climbed less than 5%, and the pair below,
// which differs by 0.2% at one step, reported maxes a doubling apart
// (#644, #645's review). It stops at a step more than 5% below the best,
// an overload guard (#647) with an edge of its own, which the test pins
// down too.
func TestRampSteps(t *testing.T) {
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
	// A step more than 5% below the best is most likely past the peak:
	// the ramp stops there, before the steps that overload the store
	// further (#647). The k8s curve of #645's A/A bench at RTT 0.
	k, asked, _ := run(5, []float64{45731, 69036, 81118, 75200, 72494, 59229}, -1)
	if k.Throughput != 81118 || len(asked) != 4 {
		t.Errorf("a curve past its peak gave %v after %d steps, want 81118 after 4", k.Throughput, len(asked))
	}
	// A step less than 5% below goes on, and one exactly 5% below too: the
	// stop needs more.
	if _, asked, _ := run(3, []float64{100, 110, 105, 108}, -1); len(asked) != 4 {
		t.Errorf("a step 4.5%% below the best stopped the ramp after %d steps", len(asked))
	}
	if _, asked, _ := run(3, []float64{100, 95, 99, 98}, -1); len(asked) != 4 {
		t.Errorf("a step exactly 5%% below the best stopped the ramp after %d steps", len(asked))
	}
	// The guard's edge: a curve that climbs back past its best after a
	// drop of more than 5% loses the later step, so two curves 0.2 apart
	// at the drop report different maxes. #645's A/A bench had one such
	// k8s ramp at RTT 2, reported 0.7% low.
	if a, _, _ := run(2, []float64{100, 95.1, 110}, -1); a.Throughput != 110 {
		t.Errorf("a drop of 4.9%% then a climb gave %v, want 110", a.Throughput)
	}
	if b, _, _ := run(2, []float64{100, 94.9, 110}, -1); b.Throughput != 100 {
		t.Errorf("a drop of 5.1%% then a climb gave %v, want 100: the guard stops at the drop", b.Throughput)
	}
	if r, _, _ := run(5, []float64{30148.2, 47795.1, 70182.8, 81143.4, 76206.7, 81684}, -1); r.Throughput != 81143.4 {
		t.Errorf("the A/A bench's recovering ramp gave %v, want 81143.4 after the drop", r.Throughput)
	}
	// A failed step ends the ramp and is its result.
	if _, asked, err := run(3, []float64{100, 110, 120, 130}, 1); err == nil || len(asked) != 2 {
		t.Errorf("a failure at the second step gave %v after %d steps", err, len(asked))
	}
}

// A step in which a k8s copy ran out of its recording counts for nothing:
// the ramp ends there with the best of the steps before, which ran on the
// recording whole, and no error. A store fast enough to get there failed
// the run, as #602's full bench of ae01b0a4 did, on its third repetitions.
// A first step that runs out, or a step that also failed otherwise, fails
// the run still.
func TestARampEndsAtAStepThatRanOutOfTheRecording(t *testing.T) {
	ranOut := errors.Join(fmt.Errorf("copy 16: %w", errTraceTooShort), errors.Join(nil, fmt.Errorf("copy 39: %w", errTraceTooShort)))
	run := func(tputs []float64, failAt int, err error) (*result, int, error) {
		asked := 0
		cfg := config{streams: 256, ramp: true, rampSteps: 5}
		res, gotErr := ramped(cfg, func(streams int) (*result, error) {
			i := asked
			asked++
			if i == failAt {
				return &result{Throughput: tputs[i]}, err
			}
			return &result{Throughput: tputs[i]}, nil
		})
		return res, asked, gotErr
	}
	res, asked, err := run([]float64{43244, 77650, 117764, 133007, 155082, 142675}, 4, ranOut)
	if err != nil || res.Throughput != 133007 || asked != 5 {
		t.Fatalf("a ramp whose fifth step ran out gave %v after %d steps, %v; want 133007 after 5, no error", res.Throughput, asked, err)
	}
	if n := len(res.Ramp); n != 5 || !res.Ramp[4].RanOut || res.Ramp[3].RanOut || res.RampClimbing {
		t.Fatalf("the ramp's steps %+v, climbing %v; want 5, the last marked as run out, not climbing", res.Ramp, res.RampClimbing)
	}
	if _, _, err := run([]float64{43244}, 0, ranOut); !errors.Is(err, errTraceTooShort) {
		t.Fatalf("a first step that ran out gave %v, want the run to fail", err)
	}
	other := errors.Join(fmt.Errorf("copy 16: %w", errTraceTooShort), errors.New("watch stream ended"))
	if _, _, err := run([]float64{43244, 77650, 117764}, 2, other); err == nil {
		t.Fatal("a step that ran out and failed otherwise gave no error")
	}
}
