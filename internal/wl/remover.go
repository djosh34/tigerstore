package wl

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
)

// remover removes files off the loop's thread. Unlinking a checkpoint of
// hundreds of MB, or a log segment, waits for the file system's journal,
// which, while checkpoints were written back, held the loop 215 to 555 ms
// (#704). A start removes what a crash left before the remover got to it:
// the checkpoints but the newest (cleanCkptDir) and the log's segments
// before its cut (wal.reopen).
type remover struct {
	sys    sysEnv
	paths  chan string
	done   chan struct{}
	failed atomic.Pointer[error]
	wake   func()
}

// removerQueue is how many removals may wait: a checkpoint and the
// segments a cut frees, far fewer than this.
const removerQueue = 1024

func newRemover(sys sysEnv, wake func()) *remover {
	r := &remover{sys: sys, paths: make(chan string, removerQueue), done: make(chan struct{}), wake: wake}
	go r.run()
	return r
}

func (r *remover) run() {
	defer close(r.done)
	for p := range r.paths {
		err := r.sys.remove(p)
		if err == nil || errors.Is(err, os.ErrNotExist) || r.failed.Load() != nil {
			continue
		}
		err = fmt.Errorf("remove %s: %w", p, err)
		r.failed.Store(&err)
		r.wake()
	}
}

// remove has path removed.
func (r *remover) remove(path string) { r.paths <- path }

// close waits for the removals asked for.
func (r *remover) close() {
	close(r.paths)
	<-r.done
}

// removeAll has paths removed, off the loop.
func (n *node) removeAll(paths []string) {
	for _, p := range paths {
		n.rm.remove(p)
	}
}
