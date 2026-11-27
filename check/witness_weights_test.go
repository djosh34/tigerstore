package check

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWitnessStepsUsed(t *testing.T) {
	for _, mode := range []WitnessMode{FromLog, FromRevisions} {
		for _, kind := range []string{"pass", "fail", "budget", "before check"} {
			t.Run(mode.String()+"/"+kind, func(t *testing.T) {
				b := newBuilder(t, "k")
				for range 100 {
					b.put(0, "k", "v")
				}
				read := b.get(0, "k")
				h := b.done()
				timeout := time.Minute
				var opts []Option
				switch kind {
				case "fail":
					h.Ops[read].Resp.Range.Count++
				case "budget":
					opts = append(opts, MaxSteps(2))
				case "before check":
					timeout = 0
				}
				want := Witnessed(h, mode, timeout, opts...)
				var used int64 = -1
				opts = append(opts, StepsUsed(&used))
				got := Witnessed(h, mode, timeout, opts...)
				// Elapsed host time is diagnostic and varies between runs.
				gotMessage, _, _ := strings.Cut(fmt.Sprint(got), "; elapsed search time ")
				wantMessage, _, _ := strings.Cut(fmt.Sprint(want), "; elapsed search time ")
				if gotMessage != wantMessage {
					t.Fatalf("reporting changed the verdict/message: %v, want %v", got, want)
				}
				if kind == "before check" {
					if used != 0 {
						t.Fatalf("before check reported %d steps, want zero", used)
					}
				} else if used <= 0 {
					t.Fatalf("check reported no steps: %d", used)
				}
				if kind == "budget" && (!errors.Is(got, ErrWitnessTimeout) || used != 3) {
					t.Fatalf("limited check: %v, reported %d, want 3", got, used)
				}
				if kind == "pass" && got != nil || kind == "fail" && got == nil {
					t.Fatalf("unexpected verdict for %s: %v", kind, got)
				}
				if err := Witnessed(h, mode, timeout, StepsUsed(nil)); fmt.Sprint(err) != fmt.Sprint(Witnessed(h, mode, timeout)) {
					t.Fatalf("nil report changed the verdict: %v", err)
				}
			})
		}
	}
}

func TestWeightedWorkKeepsPolling(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		t.Run(fmt.Sprint("capacity=", capacity), func(t *testing.T) {
			r := dueShape(0)
			calls := 0
			r.w.stop = func() bool {
				calls++
				return calls == 2
			}
			step := r.step
			weight := int64(searchWeight)
			if capacity {
				step, weight = r.capacityStep, capacityWeight
			}
			items := 0
			for ; items < 2*1024*64; items++ {
				if step() {
					break
				}
			}
			if !r.w.stopped || calls != 2 || r.w.workTicks != 2048 || r.w.steps != 2048/weight {
				t.Fatalf("%d items: stopped=%v, polls=%d, ticks=%d, steps=%d, weight=%d", items, r.w.stopped, calls, r.w.workTicks, r.w.steps, weight)
			}
			bulk := dueShape(0)
			bulkCalls := 0
			bulk.w.stop = func() bool { bulkCalls++; return bulkCalls == 2 }
			charge := bulk.charge
			if capacity {
				charge = bulk.capacityCharge
			}
			if !charge(2*1024*64) || bulk.w.steps != r.w.steps || bulk.w.workTicks != r.w.workTicks || bulkCalls != calls {
				t.Fatal("bulk weighting changed cancellation frequency or budget accounting")
			}
		})
	}
}

func TestMixedWeightsSharePollingAndBudget(t *testing.T) {
	r, bulk := dueShape(0), dueShape(0)
	var usedUnits int64
	for block := range 4096 {
		capacity := block%2 == 0
		units := int64(ordinaryUnits)
		if capacity {
			units = 1
		}
		for range 63 {
			if capacity {
				r.capacityStep()
			} else {
				r.step()
			}
			usedUnits += units
		}
		if capacity {
			bulk.capacityCharge(63)
		} else {
			bulk.charge(63)
		}
	}
	wantSteps := (usedUnits + workUnitsPerStep - 1) / workUnitsPerStep
	wantTicks := int64((4096*63 + 63) / 64)
	if r.w.steps != wantSteps || r.w.workTicks != wantTicks || bulk.w.steps != wantSteps || bulk.w.workTicks != wantTicks || bulk.budgetLeft != r.budgetLeft || bulk.workLeft != r.workLeft {
		t.Fatalf("mixed classes changed charges/polls: steps %d/%d want %d, ticks %d/%d want %d", r.w.steps, bulk.w.steps, wantSteps, r.w.workTicks, bulk.w.workTicks, wantTicks)
	}
	stopped := dueShape(0)
	calls := 0
	stopped.w.stop = func() bool { calls++; return calls == 2 }
	for i := range 2 * 1024 * 64 {
		step := stopped.step
		if i%126 < 63 {
			step = stopped.capacityStep
		}
		if step() {
			if i != (2048-1)*64 || calls != 2 || stopped.w.workTicks != 2048 {
				t.Fatalf("mixed classes delayed cancellation: item %d, calls %d, ticks %d", i, calls, stopped.w.workTicks)
			}
			return
		}
	}
	t.Fatal("mixed work did not honour the hook at the old work bound")
}

// TestWeightedSavedMargins guards the ratio constraint independently of
// the wall stop. The old counts are measured on base 37e51817; multiplying
// each by seven preserves its fraction of the old 50M budget even at
// the 350M floor, including diagnostic bytes and large-map writes. The
// ordinary budget stays 360M, leaving still more margin;
// long seeds have a separate, runner-time-sized cap under #876.
var weightedSavedRows = []struct {
	name           string
	base, weighted int64
}{
	{"planted-B01a-1.json", 10, 10},
	{"seed-5058843.json", 1479, 1795},
	{"seed-5068434.json", 107, 114},
	{"seed-59113215.json", 272, 293},
	{"seed-77148.json", 777, 841},
	{"seed-888737.json", 141, 158},
	{"witness-425.json.gz", 23712, 25435},
	{"witness-control-80-201.json.gz", 9764, 12340},
	{"witness-control-320-1529.json.gz", 15881, 110660},
	{"grants-81000", 87330, 89394},
	{"gaps-256-expiries-4096", 13271, 62819},
}

func TestWeightedSavedMargins(t *testing.T) {
	for _, tc := range weightedSavedRows {
		t.Run(tc.name, func(t *testing.T) {
			var used int64
			h := budgetHistory(t, tc.name)
			err := Witnessed(h, FromRevisions, time.Hour, StepsUsed(&used), snapshotOracle(t))
			// These saved rows remain exact, not merely within the new
			// general extra-putter bound. Rule-off replays are separate.
			if (tc.name == "grants-81000" || tc.name == "gaps-256-expiries-4096") && err != nil {
				t.Fatalf("synthetic production row %s must pass at %d steps: %v", tc.name, used, err)
			}
			if used != tc.weighted {
				t.Fatalf("saved row changed: new %d recorded %d", used, tc.weighted)
			}
			if errors.Is(err, ErrWitnessTimeout) || used <= 0 || used*50_000_000 > tc.base*350_000_000 {
				t.Fatalf("lost margin: %d weighted steps, base %d, ratio floor 350M/50M; %v", used, tc.base, err)
			}
			t.Logf("base %d, weighted %d, ratio %.6f", tc.base, used, float64(used)/float64(tc.base))
		})
	}
}

func TestWeightedCapacityBudget(t *testing.T) {
	r := dueShape(0)
	r.w.maxSteps = 3
	if !r.capacityCharge(100_000) || r.w.steps != 4 {
		t.Fatalf("capacity work bypassed its deterministic budget: stopped=%v, steps=%d", r.w.stopped, r.w.steps)
	}
	if r.w.stop != nil {
		t.Fatal("capacity budget needed a wall stop")
	}
}
