package main

import "testing"

// TestRampTriesAStepPastAFlatOne runs the ramp's rule over the steps of
// #642's bench, put-1k, seed 223812443, 10 ms (#644). Two repeats on each
// side were level at 256 and 512 streams, but main's climbed 5.2% to 512
// and went on to 1,024, and the candidate's climbed 4.7% and stopped, so
// their maxes were 5.4% apart. A step that climbs less than 5% now leaves
// the ramp going, and it stops at the second such step in a row.
func TestRampTriesAStepPastAFlatOne(t *testing.T) {
	cases := []struct {
		name  string
		steps []float64
		// want is how many steps the ramp takes.
		want int
	}{
		{"climbs 5.2% to 512, flat after 1,024", []float64{97024, 102092, 107827, 103357, 104000}, 5},
		{"climbs 4.7% to 512, then 5.7% to 1,024", []float64{97356, 101939, 107700, 104000, 103000}, 5},
		{"flat twice from the start", []float64{97280, 97587, 98000}, 3},
		{"climbs every step", []float64{10, 20, 40, 80, 160, 320}, 6},
	}
	for _, c := range cases {
		var r rampRule
		took := 0
		for _, tput := range c.steps {
			took++
			if _, stop := r.step(tput); stop {
				break
			}
		}
		if took != c.want {
			t.Errorf("%s: the ramp took %d steps, want %d", c.name, took, c.want)
		}
	}
}
