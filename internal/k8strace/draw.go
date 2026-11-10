package k8strace

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"time"
)

// Draw is what a seed picks for a replay of the recordings in a
// directory: one of them, a cluster shape, and the call each copy of it
// begins at, in the first quarter of the trace.
type Draw struct {
	Trace  *Trace
	Starts []int
	// Rand goes on from the draw, for the replay's value bytes.
	Rand *rand.Rand
}

// DrawSeed draws for seed and copies from the *.trace.gz of dir, the
// same way every time.
func DrawSeed(dir string, seed uint64, copies int) (*Draw, error) {
	names, err := filepath.Glob(filepath.Join(dir, "*.trace.gz"))
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no recordings (*.trace.gz) in %s", dir)
	}
	slices.Sort(names)
	rng := rand.New(rand.NewPCG(seed, 0x6b387374726163))
	t, err := Read(names[rng.IntN(len(names))])
	if err != nil {
		return nil, err
	}
	d := &Draw{Trace: t, Rand: rng}
	quarter := max(len(t.Calls)/4, 1)
	for range copies {
		d.Starts = append(d.Starts, rng.IntN(quarter))
	}
	return d, nil
}

// Least is the least time a call can take: a write needs a round trip to a
// quorum and a durable write, a linearizable read a round trip, and a
// serializable read or a progress request no wait at all.
type Least struct {
	Write, Read time.Duration
}

func (l Least) of(c *Call) time.Duration {
	switch c.Kind {
	case CallRange, CallRangeStream:
		if c.Op.Flags&FlagSerializable != 0 {
			return 0
		}
		return l.Read
	case CallProgress:
		return 0
	case CallLeaseKeepAlive, CallLeaseTTL:
		return l.Read
	case CallTxn:
		if !writes(&c.Op) {
			return l.Read
		}
	}
	return l.Write
}

// Chain is the least time the calls from call from on can take, as each
// waits on the replies it waited on when it was recorded: the longest
// path through the waits, each call taking its least time.
func (t *Trace) Chain(from int, l Least) time.Duration {
	done := make([]time.Duration, len(t.Calls))
	var longest time.Duration
	for i := from; i < len(t.Calls); i++ {
		c := &t.Calls[i]
		var start time.Duration
		for _, w := range c.Waits {
			if int(w) >= from {
				start = max(start, done[w])
			}
		}
		done[i] = start + l.of(c)
		longest = max(longest, done[i])
	}
	return longest
}

// Bound is the most calls a second the copies of d can make, however fast
// the store: each copy makes the calls from its start to the trace's end
// in no less than their Chain. With no least time at all there is no
// bound, +Inf.
func (d *Draw) Bound(l Least) float64 {
	var rate float64
	for _, k := range d.Starts {
		chain := d.Trace.Chain(k, l)
		if chain <= 0 {
			return math.Inf(1)
		}
		rate += float64(len(d.Trace.Calls)-k) / chain.Seconds()
	}
	return rate
}
