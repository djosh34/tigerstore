package core

// leaseQueue holds leases in the order of a tick each, the earliest first
// and by ID among equal ticks, as etcd's lessor holds them in a heap by
// expiry (#483). Only the leader fills its queues. pos is a lease's place
// in the queue: its index plus one, 0 while it is not in it.
type leaseQueue struct {
	ls  []*lease
	at  func(*lease) int64
	pos func(*lease) *int
}

// set puts l in the queue, or moves it to its place after its tick
// changed.
func (q *leaseQueue) set(l *lease) {
	if p := *q.pos(l); p > 0 {
		q.fix(p - 1)
		return
	}
	q.ls = append(q.ls, l)
	*q.pos(l) = len(q.ls)
	q.up(len(q.ls) - 1)
}

// remove takes l out of the queue, if it is in it.
func (q *leaseQueue) remove(l *lease) {
	p := *q.pos(l)
	if p == 0 {
		return
	}
	i, last := p-1, len(q.ls)-1
	q.swap(i, last)
	q.ls[last] = nil
	q.ls = q.ls[:last]
	*q.pos(l) = 0
	if i < last {
		q.fix(i)
	}
}

// first is the lease with the earliest tick, or nil.
func (q *leaseQueue) first() *lease {
	if len(q.ls) == 0 {
		return nil
	}
	return q.ls[0]
}

// reset empties the queue.
func (q *leaseQueue) reset() {
	for _, l := range q.ls {
		*q.pos(l) = 0
	}
	clear(q.ls)
	q.ls = q.ls[:0]
}

func (q *leaseQueue) less(i, j int) bool {
	a, b := q.at(q.ls[i]), q.at(q.ls[j])
	return a < b || a == b && q.ls[i].id < q.ls[j].id
}

func (q *leaseQueue) swap(i, j int) {
	q.ls[i], q.ls[j] = q.ls[j], q.ls[i]
	*q.pos(q.ls[i]), *q.pos(q.ls[j]) = i+1, j+1
}

// fix moves the lease at i up or down to its place.
func (q *leaseQueue) fix(i int) {
	if !q.down(i) {
		q.up(i)
	}
}

func (q *leaseQueue) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			return
		}
		q.swap(i, parent)
		i = parent
	}
}

// down moves the lease at i down to its place and reports whether it
// moved.
func (q *leaseQueue) down(i int) bool {
	start := i
	for {
		c := 2*i + 1
		if c >= len(q.ls) {
			break
		}
		if r := c + 1; r < len(q.ls) && q.less(r, c) {
			c = r
		}
		if !q.less(c, i) {
			break
		}
		q.swap(i, c)
		i = c
	}
	return i > start
}
