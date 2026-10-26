//go:build sim

package behavior_test

import (
	"testing"

	"github.com/jellevandenhooff/gosim"
	"github.com/jellevandenhooff/gosim/internal/tests/behavior"
)

var sharedGlobal = 10

func TestGlobalNotShared(t *testing.T) {
	// TODO: Ensure we run this test multiple times to proof we reset globals?
	for i := 0; i < 2; i++ {
		gosim.NewSimpleMachine(func() {
			if sharedGlobal != 10 {
				t.Errorf("expected 10, got %d", sharedGlobal)
			}
			sharedGlobal = 11
		}).Wait()
	}
}

var chain1 = globalReadFromMain + 1

var (
	globalReadFromForTest = behavior.CopiedPrivateGlobal
	globalReadFromMain    = behavior.PublicGlobal
)

var (
	chain2 int
	chain3 int
)

func init() {
	chain2 = chain1 * 2
}

func init() {
	chain3 = chain2 + 3
}

func TestGlobals(t *testing.T) {
	if behavior.PublicGlobal != 20 {
		t.Errorf("expected 20, got %d", behavior.PublicGlobal)
	}
	if globalReadFromMain != 20 {
		t.Errorf("expected 20, got %d", globalReadFromMain)
	}
	if chain1 != 21 {
		t.Errorf("expected 21, got %d", chain1)
	}
	if chain2 != 42 {
		t.Errorf("expected 42, got %d", chain2)
	}
	if chain3 != 45 {
		t.Errorf("expected 45, got %d", chain3)
	}
	if behavior.CopiedPrivateGlobal != 10 {
		t.Errorf("expected 10, got %d", behavior.CopiedPrivateGlobal)
	}
	if behavior.InitializedPrivateGlobal != 35 {
		t.Errorf("expected 35, got %d", behavior.InitializedPrivateGlobal)
	}
	if globalReadFromForTest != 10 {
		t.Errorf("expected 10, got %d", globalReadFromForTest)
	}
	if value := *behavior.PrivateGlobalPtr(); value != 10 {
		t.Errorf("expected 10, got %d", value)
	}
}

// "compare observations" (expect subset?)
// vs
// "tests must pass exactly in both cases" (specificy observations)
// vs
// "when running a bunch of times, i want to observe both this and that"

// - want to have a test that says...
//   - sometimes this happens
//   - sometimes this other things happens
//   - BOTH do happen, given a reasonable number of runs
