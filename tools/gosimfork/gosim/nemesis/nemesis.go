/*
Package nemesis contains pre-built scenarios that introduce problems into
distributed systems to try and trigger rare bugs.
*/
package nemesis

import (
	"log"
	"math/rand"
	"time"

	"github.com/jellevandenhooff/gosim"
)

/*
// Nemsis sleeps a random goroutine for 1 simulated second. It starts after
// calling Yield n times to give goroutines some time to get started.
//
// TODO: Wait some random number of simulated seconds instead of yielding?
// TODO: Do not export this function. It is too specific?
func Nemesis(n int) {
	// figure out how many steps we are running (external argument?)
	for i := 0; i < n; i++ {
		gosim.Yield()
	}
	other := gosimruntime.PickRandomOtherGoroutine() // XXX: make this pick a runnable goroutine instead?
	gosimruntime.PauseGoroutine(other)
	// XXX: what if other has stopped here? make sure it doesn't break
	// slog.Info("sleeping goroutine", "id", g.ID, "selected", g.selected)
	time.Sleep(time.Second)
	gosimruntime.ResumeGoroutine(other)

	// sleep for rand(steps)... or, call Yield N times
	// pause random goroutine gosimruntime.PickRandomOtherGoroutine()
	// sleep (clock) for 1s gosimruntime.PauseGoroutine
	// unpause that goroutine gosimruntime.ContinueGoroutine
}

func Restarter(n int, machines []gosim.Machine) {
	// XXX: figure out which machines exist and are reasonable candidates?
	for i := 0; i < n; i++ {
		gosim.Yield()
	}
	i := rand.Intn(len(machines))
	m := machines[i]
	m.Crash()
	time.Sleep(5 * time.Second)
	m.Restart()
}
*/

// A Scenario is a potentially challenging scenario that can be run and
// introduce chaos to running machines or a network to see if they keep behaving
// as expected.
type Scenario interface {
	Run()
}

// Sleep is a scenario that simply sleeps.
type Sleep struct {
	Duration time.Duration
}

// Run implements Scenario.
func (s Sleep) Run() {
	time.Sleep(s.Duration)
}

// PartitionMachines is a scenario that groups the given addresses in two
// different sets and disables the network between the groups.
type PartitionMachines struct {
	Addresses []string
	Duration  time.Duration
}

// Run implements Scenario.
func (p PartitionMachines) Run() {
	var a, b []string

	for {
		a, b = nil, nil
		for _, addr := range p.Addresses {
			if rand.Intn(2) == 0 {
				a = append(a, addr)
			} else {
				b = append(b, addr)
			}
		}
		if len(p.Addresses) <= 1 || (len(a) > 0 && len(b) > 0) {
			break
		}
	}

	log.Printf("partition machines: partitioning network into %v and %v", a, b)

	for _, a := range a {
		for _, b := range b {
			gosim.SetConnected(a, b, false)
		}
	}

	time.Sleep(p.Duration)

	log.Printf("partition machines: rejoining network between %v and %v", a, b)

	for _, a := range a {
		for _, b := range b {
			gosim.SetConnected(a, b, true)
		}
	}
}

// RestartRandomly restarts one randomly chosen machine.
type RestartRandomly struct {
	// Machines to choose from for restarting
	Machines []gosim.Machine
	// Time to wait between crashing machine and starting again
	Downtime time.Duration
}

// Run implements Scenario.
func (r RestartRandomly) Run() {
	if len(r.Machines) == 0 {
		panic("restart randomly: need at least one machine")
	}
	m := r.Machines[rand.Intn(len(r.Machines))]

	log.Printf("restart randomly: crashing machine %s", m.Label())

	m.Crash()
	time.Sleep(r.Downtime)

	log.Printf("restart randomly: restarting machine %s", m.Label())
	m.RestartWithPartialDisk()
}

type funcScenario func()

func (f funcScenario) Run() {
	f()
}

// Repeat repeats the given scenario a number of times.
func Repeat(scenario Scenario, times int) Scenario {
	return funcScenario(func() {
		for range times {
			scenario.Run()
		}
	})
}

// Sequence runs the given scenearios in sequence.
func Sequence(scenarios ...Scenario) Scenario {
	return funcScenario(func() {
		for _, s := range scenarios {
			s.Run()
		}
	})
}
