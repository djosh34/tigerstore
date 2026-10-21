//go:build race && !sim

package sim

// raceLeavesOut is set under the race detector, where a seed does not play
// out the same way twice and known-failures.txt cannot list it. The runs
// then leave out the faults that hit open bugs: kills that keep the page
// cache (#361) and damage to a last record (#357). Their fixes remove it.
const raceLeavesOut = true
