package sim

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// How often the simulated cluster wakes up on time is the count of
// timers that fire per simulated second, under gosim (#504):
//
//	SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^TestIdleWakeups$' -test.v
//	SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^(TestCalm|TestFaultedLoad)$' -test.v
//
// The first counts an idle cluster's; the others count those of calm and
// faulted runs' load, with their clients'. Every count includes the
// simulator's own timers: the delivery of each message on the simulated
// network and the watch on the run's real time, 10 a second.

// idleSpan is how long TestIdleWakeups counts an idle cluster's timers.
const idleSpan = 60 * time.Second

// TestIdleWakeups forms a cluster with no faults and no clients, as a
// calm run does, lets it settle and counts the timers that fire while it
// sits idle.
func TestIdleWakeups(t *testing.T) {
	if simrun.Race {
		t.Skip("only gosim counts the timers that fire")
	}
	var total uint64
	seeds := 0
	if !simrun.Seeds(t, 1, 1, func(t *testing.T, seed uint64) uint64 {
		var fired uint64
		simrun.Bubble(t, func(t *testing.T) {
			s, err := formCluster(CalmConfig(seed))
			if s != nil {
				defer s.shutdown()
			}
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
			before := simrun.TimerFirings()
			time.Sleep(idleSpan)
			fired = simrun.TimerFirings() - before
		})
		t.Logf("an idle cluster: %d timers fired in %v, %.1f a second", fired, idleSpan, perSecond(fired, idleSpan))
		return fired
	}, func(_ uint64, fired uint64) bool {
		total += fired
		seeds++
		return false
	}) {
		return
	}
	t.Logf("%d seeds of an idle cluster: %.1f timers fired a second", seeds, perSecond(total, time.Duration(seeds)*idleSpan))
}

// perSecond is n per second of d.
func perSecond(n uint64, d time.Duration) float64 { return float64(n) / d.Seconds() }
