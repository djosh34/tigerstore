package main

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// phase is one stretch of load on a connection.
type phase struct {
	// rate is the arrival rate on this connection in requests per second.
	// Arrivals form a Poisson process and never wait for responses, so a
	// slow server builds a backlog whose wait counts in the latency. Zero
	// keeps every stream slot busy instead.
	rate float64
	// Arrivals happen in [start, stop), in ns since epoch. A load that is
	// exhausted ends the phase early.
	start, stop int64
	// drain is how long requests may stay in flight after stop.
	drain time.Duration
}

// worker drives one connection: it owns the writer side.
type worker struct {
	c *conn
	// rng chooses keys; arrivals draws the open-loop arrival times. They
	// are separate streams, so the same seed gives every store the same
	// schedule and the same keys, however fast it answers.
	rng      *rand.Rand
	arrivals *rand.Rand
	stats    stats
	// unsent counts arrivals that were never sent by the end of the drain.
	unsent int64
}

func newWorker(c *conn, seed uint64) *worker {
	return &worker{
		c:        c,
		rng:      rand.New(rand.NewPCG(seed, 0x6c6f616467656e)),
		arrivals: newArrivalRNG(seed),
		stats:    stats{errs: make(map[string]int64)},
	}
}

func newArrivalRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, 0x6172726976616c))
}

// run sends the requests of l for phase p and waits for them to finish.
func (wk *worker) run(l load, p phase) error {
	c := wk.c
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	next := p.stop // the next arrival; with rate 0 requests start whenever a slot is free
	if p.rate > 0 {
		next = p.start + wk.gap(p.rate)
	}
	var backlog []int64 // intended times of arrivals not yet sent
	deadline := p.stop + int64(p.drain)
	for {
		t := now()
		if p.rate > 0 {
			// Arrivals stop at the end of the phase, but the backlog
			// still goes out during the drain: those requests belong to
			// the window, and leaving them out would hide how far
			// behind the server fell.
			for next < p.stop && next <= t {
				backlog = append(backlog, next)
				next += wk.gap(p.rate)
			}
		}

		c.mu.Lock()
		err := c.err
		full := false
		for err == nil && !full {
			intended := t
			if p.rate > 0 {
				if len(backlog) == 0 {
					break
				}
				intended = backlog[0]
			} else if t >= p.stop {
				break
			}
			st := c.newStream(l)
			st.intended, st.sent = intended, t
			if !l.start(c, st) {
				c.free = append(c.free, st)
				break
			}
			if p.rate > 0 {
				backlog = backlog[1:]
			}
			full = len(c.out) >= 256<<10
		}
		c.appendControl()
		inflight := len(c.streams)
		exhausted := l.exhausted()
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if err := c.flush(); err != nil {
			return err
		}

		over := (t >= p.stop && len(backlog) == 0) || exhausted
		if over && inflight == 0 {
			return nil
		}
		if t >= deadline {
			// Arrivals never sent fail, in the window's cohort if they
			// belong to it, and make the result invalid.
			c.mu.Lock()
			wk.unsent += int64(len(backlog))
			inWindow := func(at int64) {
				if at >= wk.stats.winStart && at < wk.stats.winStop {
					wk.stats.cohort++
					wk.stats.cohortFailed++
				}
			}
			for _, at := range backlog {
				inWindow(at)
			}
			// Requests still in flight fail too, once: their streams are
			// forgotten, so a late answer counts nothing.
			for id, st := range c.streams {
				inWindow(st.intended)
				delete(c.streams, id)
			}
			c.mu.Unlock()
			return fmt.Errorf("%s: %d requests in flight and %d never sent %v after the phase", c.addr, inflight, len(backlog), p.drain)
		}
		if full {
			continue
		}
		// Sleep until the next arrival, or until a response or window
		// update frees room.
		wait := deadline
		if !over && t < p.stop {
			wait = min(next, p.stop)
		}
		if wait <= t {
			continue
		}
		timer.Reset(time.Duration(wait - t))
		select {
		case <-c.wake:
		case <-timer.C:
		}
	}
}

// gap draws the time to the next arrival in ns.
func (wk *worker) gap(rate float64) int64 {
	return max(int64(wk.arrivals.ExpFloat64()/rate*1e9), 1)
}
