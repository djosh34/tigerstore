package node

// coreClock is what the committer moves on in time: core.Node.
type coreClock interface {
	// Tick is one tick of Raft's and the core's clocks.
	Tick()
	// Elapse moves only the clock requests time out by.
	Elapse(ticks int64)
}

// tickClock hands the core the ticks of wall time, counted in ticks from
// the start of the committer's run. The committer sleeps until the next
// tick with work is due (core.Node.Due) or something happens, and a
// ticker that drops ticks is the model of what it then hands over. Raft
// takes no burst of ticks from a committer that was late, by its own work
// or for a CPU to run on, and requests still time out by the wall clock.
//
//   - The ticks it waited for are Ticks, up to the one from which it could
//     have run again (ready): the due tick, or when the first input it
//     takes was queued. Raft sees them as if each had come on time.
//   - Of the ticks after that, and of those that passed while it was busy,
//     one is a Tick, as a ticker holds one tick for a goroutine that has
//     not taken it, and the rest go by Elapse.
//
// A node short of CPU then counts no leader dead for the time it could not
// run, as with a ticker.
type tickClock struct {
	// at is the tick the core has been moved to, by Tick or Elapse, and
	// due the tick its next work is due at, for which the committer wakes.
	at, due int64
	// lateTicked says whether this turn has had its one late Tick.
	lateTicked bool
}

// advance moves c on to now, giving it at most limit Ticks. waited is the
// tick at which the committer last began to wait, and ready the tick from
// which it could have run again. Past limit Ticks it stops, and the next
// call goes on from there. It reports whether it moved c.
func (k *tickClock) advance(c coreClock, now, waited, ready, limit int64) bool {
	from := k.at
	tick := func() bool {
		if limit == 0 {
			return false
		}
		c.Tick()
		limit--
		k.at++
		return true
	}
	// The ticks that passed while the committer was busy.
	if waited > k.at && !k.lateTicked {
		if !tick() {
			return k.at > from
		}
		k.lateTicked = true
	}
	if waited > k.at {
		c.Elapse(waited - k.at)
		k.at = waited
	}
	// The ticks it waited for, up to ready.
	for k.at < now && k.at < ready {
		if !tick() {
			return k.at > from
		}
	}
	// The ticks it was late for.
	if k.at < now && k.at < k.due && !k.lateTicked {
		if !tick() {
			return k.at > from
		}
		k.lateTicked = true
	}
	if now > k.at {
		c.Elapse(now - k.at)
		k.at = now
	}
	return k.at > from
}
