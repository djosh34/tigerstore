package check

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWitnessTimeoutLimit(t *testing.T) {
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		for _, limit := range []string{"step", "wall"} {
			t.Run(mode.String()+"/"+limit, func(t *testing.T) {
				b := newBuilder(t, "k")
				for range 3000 {
					b.put(0, "k", "v")
				}
				const budget int64 = 100_000
				var used int64
				polls := 0
				opts := []Option{MaxSteps(budget), StepsUsed(&used)}
				if limit == "step" {
					opts = append(opts, MaxSteps(2))
				} else {
					opts = append(opts, StopWhen(func() bool { polls++; return true }))
				}
				err := Witnessed(b.done(), mode, time.Minute, opts...)
				if !errors.Is(err, ErrWitnessTimeout) {
					t.Fatalf("want timeout kind, got %v", err)
				}
				want := "the step budget (2 of 2 weighted steps)"
				if limit == "step" {
					if used != 3 {
						t.Fatalf("used %d, want budget plus exhaustion probe", used)
					}
				} else {
					want = fmt.Sprintf("the real-time limit after %d of %d weighted steps", used, budget)
					if polls != 1 || used <= 0 || used >= budget {
						t.Fatalf("polls %d, used %d: hook did not stop before budget", polls, used)
					}
				}
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("%v: want %q", err, want)
				}
				_, elapsed, ok := strings.Cut(err.Error(), "; elapsed search time ")
				d, parseErr := time.ParseDuration(elapsed)
				if !ok || parseErr != nil || d <= 0 {
					t.Fatalf("missing positive elapsed host search time: %v", err)
				}
				for _, failure := range []string{"the furthest it got", "cannot write it", "the first op it could not place"} {
					if strings.Contains(err.Error(), failure) {
						t.Fatalf("timeout invented a failure explanation: %v", err)
					}
				}
			})
		}
	}
}
