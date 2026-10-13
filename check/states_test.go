package check

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// TestStatesEqual checks the comparison of sets of states that Porcupine's
// cache relies on. Fingerprints count a value only by its length, so states
// that differ only in a value of the same length must still compare unequal.
func TestStatesEqual(t *testing.T) {
	timedOut := output{err: errUnavailable}
	after := func(values ...string) states {
		s := newState([]string{"a"})
		s.sum = s.fingerprint()
		ss := states{s}
		for _, v := range values {
			ss, _ = ss.step(input{req: history.Request{Put: &history.PutRequest{Key: "a", Value: v}}}, timedOut)
		}
		return ss
	}
	if !after("1", "2").equal(after("1", "2")) {
		t.Fatal("the same set built twice compares unequal")
	}
	if after("1").equal(after("2")) {
		t.Fatal("sets that differ only in a value of the same length compare equal")
	}
	if after("1").equal(after("1", "2")) {
		t.Fatal("sets of different sizes compare equal")
	}
}
