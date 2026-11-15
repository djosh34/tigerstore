package server

import (
	"container/heap"
	"time"
)

// A gRPC call with a grpc-timeout ends at its deadline whether or not its
// request is complete or the node answered. Each connection keeps the
// deadlines of its calls in a heap on the loop, which runs the earliest
// through loop.After: no timer and no goroutine per call.

// deadlines is a connection's calls with a deadline, earliest first. Each
// stream knows its place (stream.due), so a call that ends leaves at once.
// Loop only.
type deadlines []*stream

func (d deadlines) Len() int           { return len(d) }
func (d deadlines) Less(i, j int) bool { return d[i].deadline.Before(d[j].deadline) }
func (d deadlines) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
	d[i].due, d[j].due = i+1, j+1
}

func (d *deadlines) Push(x any) {
	st := x.(*stream)
	*d = append(*d, st)
	st.due = len(*d)
}

func (d *deadlines) Pop() any {
	old := *d
	st := old[len(old)-1]
	old[len(old)-1] = nil
	*d = old[:len(old)-1]
	st.due = 0
	return st
}

// armDeadline lists st to end at its deadline. Loop only.
func (c *conn) armDeadline(st *stream, at time.Time) {
	st.deadline = at
	heap.Push(&c.deadlines, st)
	c.s.loop.After(c, at)
}

// disarmDeadline drops st's deadline, if it has one. Loop only.
func (c *conn) disarmDeadline(st *stream) {
	if st.due > 0 {
		heap.Remove(&c.deadlines, st.due-1)
	}
}

// expireDue ends the calls whose deadline passed by now, and has the loop
// come back at the next deadline. Loop only.
func (c *conn) expireDue(now time.Time) {
	for len(c.deadlines) > 0 {
		st := c.deadlines[0]
		if st.deadline.After(now) {
			c.s.loop.After(c, st.deadline)
			return
		}
		heap.Pop(&c.deadlines)
		st.expire()
	}
}
