package wl

// A task's payload/descriptor reservation belongs to its caller until
// completion is acknowledged. Only the loop submits tasks and runs done;
// the persistent worker sees work and immutable, leased source plans.
type fetchTask struct {
	work func() error
	done func(error) error
}

type fetchCompletion struct {
	task *fetchTask
	err  error
}

type fetchWorker struct {
	jobs    chan *fetchTask
	results chan fetchCompletion
	exited  chan struct{}
	busy    bool // loop-owned, includes the posted completion
	stopped bool
}

// startFetchWorker starts one goroutine, not one goroutine per piece.
// The caller has already reserved the common 80-KiB stack/control cost.
func (l *loop) startFetchWorker() {
	w := &fetchWorker{
		jobs: make(chan *fetchTask, 1), results: make(chan fetchCompletion, 1),
		exited: make(chan struct{}),
	}
	l.fetchWorker = w
	strays.Add(1)
	go func() {
		defer strays.Add(-1)
		defer close(w.exited)
		for task := range w.jobs {
			err := task.work()
			// Do not retain the previous arena on this idle worker's
			// stack after its loop acknowledgement returns the credit.
			task.work = nil
			w.results <- fetchCompletion{task: task, err: err}
			l.mu.Lock()
			l.wake()
			l.mu.Unlock()
		}
	}()
}

// queueFetchTask never blocks the loop. busy stays true until the result
// is acknowledged, so neither a second I/O nor an uncharged completion
// can pass the single-worker gate.
func (l *loop) queueFetchTask(task *fetchTask) bool {
	if l.fetchWorker == nil {
		l.startFetchWorker()
	}
	w := l.fetchWorker
	if w.busy || w.stopped {
		return false
	}
	select {
	case w.jobs <- task:
		w.busy = true
		return true
	default:
		return false
	}
}

func (l *loop) runFetchCompletion() error {
	w := l.fetchWorker
	if w == nil {
		return nil
	}
	select {
	case result := <-w.results:
		w.busy = false
		done := result.task.done
		result.task.done = nil
		return done(result.err)
	default:
		return nil
	}
}

// stop is called only after request cancellation on the stopped loop,
// before WAL/checkpoint sources close. At most one result can be queued;
// the worker can therefore finish without a callback deadlock.
func (w *fetchWorker) stop() {
	if w.stopped {
		return
	}
	w.stopped = true
	close(w.jobs)
	<-w.exited
}

func (a *fillArena) fetchShape() (values int, bytes int64) {
	cold := false
	for i := range a.n {
		p := &a.spans[i]
		if p.flags&fillContinue == 0 {
			cold = false
		}
		if p.flags&fillReady != 0 {
			continue
		}
		bytes += int64(len(p.dst))
		if !cold {
			values++
			cold = true
		}
	}
	return
}

func (b *legacyBatch) fetchShape() (values int, bytes int64) {
	last := ^uint32(0)
	for i := range b.planN {
		p := &b.plan[i]
		if p.source == ^uint32(1) {
			continue
		}
		bytes += int64(p.len)
		if last != p.entry {
			values++
			last = p.entry
		}
	}
	return
}

func (n *node) recordFetch(values int, bytes int64) {
	n.stats.fetches++
	n.stats.fetched += values
	n.stats.fetchMost = max(n.stats.fetchMost, bytes)
}

// Test shells execute queued closures deterministically rather than
// starting real workers. The production shell always takes this branch.
func (n *node) queueFetchTask(task *fetchTask) bool {
	if s, ok := n.shell.(interface{ queueFetchTask(*fetchTask) bool }); ok {
		return s.queueFetchTask(task)
	}
	if n.shell == nil {
		return false
	}
	n.shell.offLoop(func() func() error {
		err := task.work()
		task.work = nil
		return func() error {
			done := task.done
			task.done = nil
			return done(err)
		}
	})
	return true
}
