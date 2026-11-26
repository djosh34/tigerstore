package wl

// Both fetch transports spend the one allowance in sizes.worst. The
// legacy cache is an owner too, including its indices and high-water
// containers. Returning a job's result is not permission to reuse its
// credit before the loop acknowledges it.
const (
	fetchCoreWork   = 80 << 10 // one worker stack and common control
	fetchCursorWork = 384 << 10
)

type fetchWorkBook struct {
	limit  int64
	used   int64
	next   uint64
	active *fetchWorkLease
}

type fetchWorkLease struct {
	book      *fetchWorkBook
	id        uint64
	bytes     int64
	running   bool
	cancelled bool
	settled   bool
}

func newFetchWorkBook(budget int64) fetchWorkBook {
	return fetchWorkBook{limit: fetchedMax(budget), used: fetchCoreWork}
}

// reserve is called before allocating the job/cache storage it covers.
// Callers include rounded full capacities, not only live payload lengths.
func (b *fetchWorkBook) reserve(bytes int64) *fetchWorkLease {
	if bytes < 0 || bytes > b.limit || b.used > b.limit-bytes {
		return nil
	}
	if b.next == ^uint64(0) {
		return nil
	}
	b.next++
	l := &fetchWorkLease{book: b, id: b.next, bytes: bytes}
	b.used += bytes
	return l
}

// start is the single-worker gate. A retained cache lease need not start
// a worker; its live allocation still participates in the same bound.
func (l *fetchWorkLease) start() bool {
	if l.settled || l.cancelled || l.running || l.book.active != nil {
		return false
	}
	l.book.active = l
	l.running = true
	return true
}

// cancel quarantines running work. The owner can drop queued work now,
// but cannot recycle a buffer still being written by a worker.
func (l *fetchWorkLease) cancel() {
	if l.settled {
		return
	}
	l.cancelled = true
	if !l.running {
		l.release()
	}
}

// acknowledge is idempotent and is tied to this lease, not to the stream
// now occupying the old request's slot. It never clears another job.
func (l *fetchWorkLease) acknowledge() bool {
	if l.settled || !l.running || l.book.active != l {
		return false
	}
	l.book.active = nil
	l.running = false
	if l.cancelled {
		l.release()
	}
	return true
}

// release also serves retained-cache eviction. It must not be used to
// release an active worker's buffers: that requires its acknowledgement.
func (l *fetchWorkLease) release() bool {
	if l.settled || l.running {
		return false
	}
	l.book.used -= l.bytes
	l.settled = true
	return true
}
