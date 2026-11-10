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

// Least is the least time a call can take. A write needs a quorum to
// make it durable: a round trip to a follower and its durable write. A
// linearizable read needs a round trip to a quorum, as ReadIndex does; a
// store that served it from a leader's lease, on clocks, could answer
// sooner, and this bound does not count that. A serializable read or a
// progress request needs no wait at all. A Txn takes a write's time only
// if it writes whatever its compares give; one that may only read takes
// a read's.
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
		if !alwaysWrites(&c.Op) {
			return l.Read
		}
	}
	return l.Write
}

// alwaysWrites reports whether Txn op writes whichever branch its compares
// pick.
func alwaysWrites(op *Op) bool {
	branch := func(ops []Op) bool {
		for i := range ops {
			if writes(&ops[i]) {
				return true
			}
		}
		return false
	}
	return branch(op.Success) && (len(op.Compare) == 0 || branch(op.Failure))
}

// Chain is the least time the calls from call from on can take, as each
// waits on the replies it waited on when it was recorded: the longest
// path through the waits, each call taking its least time. A call that
// skip marks is not sent and takes no time, as the replay answers it at
// once; skip may be nil.
func (t *Trace) Chain(from int, l Least, skip []bool) time.Duration {
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
		done[i] = start
		if skip == nil || !skip[i] {
			done[i] += l.of(c)
		}
		longest = max(longest, done[i])
	}
	return longest
}

// Bound is the most calls a second the copies of d can make on average
// over the whole trace, however fast the store: each copy makes the calls
// it sends, from its start to the trace's end, in no less than their
// Chain. Copies other than 0 leave the watch cache's calls to copy 0, so
// they neither send them nor wait for them. It bounds the average over
// the whole trace, not a window: a copy may run faster than this for a
// while. With no least time at all there is no bound, +Inf.
func (d *Draw) Bound(l Least) float64 {
	cache := d.Trace.WatchCache()
	var rate float64
	for id, k := range d.Starts {
		var skip []bool
		calls := len(d.Trace.Calls) - k
		if id != 0 {
			skip = cache
			for i := k; i < len(cache); i++ {
				if cache[i] {
					calls--
				}
			}
		}
		chain := d.Trace.Chain(k, l, skip)
		if chain <= 0 {
			return math.Inf(1)
		}
		rate += float64(calls) / chain.Seconds()
	}
	return rate
}
