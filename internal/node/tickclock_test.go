package node

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// steps records what a tickClock handed a core: T for a Tick, E<n> for an
// Elapse of n ticks.
type steps []string

func (s *steps) Tick()              { *s = append(*s, "T") }
func (s *steps) Elapse(ticks int64) { *s = append(*s, fmt.Sprintf("E%d", ticks)) }

// TestTickClock checks what the committer hands its core after a wait, as
// a ticker that drops ticks would deliver them (tickClock).
func TestTickClock(t *testing.T) {
	const all = math.MaxInt64
	for _, c := range []struct {
		name                           string
		due, now, waited, ready, limit int64
		want                           string
	}{
		{name: "woke on time at the due tick", due: 3, now: 3, ready: 3, limit: all, want: "T T T"},
		{name: "woke late for the due tick", due: 3, now: 6, ready: 3, limit: all, want: "T T T E3"},
		// An input queued at tick 2 that the committer took at tick 6:
		// the ticks after 2 it was late for give one Tick.
		{name: "input taken late", due: 5, now: 6, ready: 2, limit: all, want: "T T T E3"},
		{name: "input taken on time", due: 5, now: 2, ready: 2, limit: all, want: "T T"},
		// Busy until tick 4: one Tick for that span, then the ticks it
		// waited for.
		{name: "busy, then waited", due: 10, now: 6, waited: 4, ready: 10, limit: all, want: "T E3 T T"},
		// The late Tick is one a turn, busy or waiting.
		{name: "busy, then late", due: 10, now: 6, waited: 4, ready: 4, limit: all, want: "T E3 E2"},
		{name: "held one short of the due tick", due: 3, now: 3, ready: 3, limit: 2, want: "T T"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got steps
			k := tickClock{due: c.due}
			k.advance(&got, c.now, c.waited, c.ready, c.limit)
			if s := strings.Join(got, " "); s != c.want {
				t.Errorf("got %q, want %q", s, c.want)
			}
			if c.limit == all && k.at != c.now {
				t.Errorf("moved to tick %d, want %d", k.at, c.now)
			}
		})
	}
}

// TestTickClockGoesOn holds the due tick back and then hands it over in a
// second call of the same turn, as turn does for a waiting peer message.
func TestTickClockGoesOn(t *testing.T) {
	var got steps
	k := tickClock{due: 3}
	k.advance(&got, 5, 0, 3, 2)
	k.advance(&got, 5, 0, 3, math.MaxInt64)
	if s := strings.Join(got, " "); s != "T T T E2" {
		t.Errorf("got %q, want %q", s, "T T T E2")
	}
}

// TestTickClockOneLateTick gives a turn its one late Tick in its first
// call, then moves the clock a tick on before the second, as a held turn
// can: the second call has no late Tick of its own.
func TestTickClockOneLateTick(t *testing.T) {
	var got steps
	k := tickClock{due: 5}
	k.advance(&got, 4, 0, 2, math.MaxInt64)
	k.advance(&got, 6, 0, 2, math.MaxInt64)
	if s := strings.Join(got, " "); s != "T T T E1 E2" {
		t.Errorf("got %q, want %q", s, "T T T E1 E2")
	}
}
