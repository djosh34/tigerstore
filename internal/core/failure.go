package core

import (
	"errors"
	"fmt"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// ErrDisk marks a failed write or sync of the node's own disk, or a
// failed read of an entry it holds. The file is then in an unknown state,
// so the node stops and never retries it; damage (wal.ErrCorrupt) also
// fences it.
var ErrDisk = errors.New("core: the disk failed")

// ErrFetch marks a snapshot install that failed for any reason but the
// node's own disk: the leader, the link or the stream. Raft cannot take
// the snapshot back, so the driver starts a new core from its log, as a
// restart does, and Raft sends another snapshot.
var ErrFetch = errors.New("core: the snapshot fetch failed")

// ErrInstalled ends a core whose snapshot install is durable in its log.
// The driver starts a new core from the log, as a restart does, which
// loads the snapshot's checkpoint and replays the entries after it.
var ErrInstalled = errors.New("core: installed a snapshot")

// FetchFailed is the error of a snapshot install that failed with err, in
// both drivers. A failed write or sync of the node's own disk (ErrDisk)
// stays as it is. Otherwise sync writes out what the stream wrote, as a
// crash after a sync could keep it, and the install reloads (ErrFetch),
// unless that sync failed too.
func FetchFailed(err error, sync func() error) error {
	if errors.Is(err, ErrDisk) {
		return err
	}
	if serr := sync(); serr != nil {
		return errors.Join(err, fmt.Errorf("%w: %w", ErrDisk, serr))
	}
	return fmt.Errorf("%w: %w", ErrFetch, err)
}

// ErrRaft stops a node whose Raft found one of its own rules broken, as a
// heartbeat whose commit index is past the end of the node's log, which
// only a log that lost acknowledged entries gives (#493). Raft panics then.
// The core's Raft logger panics with its own value, and RecoverRaft turns
// that into this error, with Raft's message, so the node stops as for any
// other failure, and a process serving it ends with an error instead of
// a crash.
var ErrRaft = errors.New("core: Raft found its state broken")

// raftPanic is what the core's Raft logger panics with. It prints Raft's
// message if nothing recovers it.
type raftPanic struct{ msg string }

func (p raftPanic) Error() string { return "raft: " + p.msg }

// RecoverRaft, deferred by a function that calls Raft, sets *err to
// ErrRaft with Raft's message if Raft panicked. Any other panic goes on.
func RecoverRaft(err *error) {
	r := recover() //nolint:forbidigo // Raft reports a broken rule only by panicking through its logger
	if r == nil {
		return
	}
	p, ok := r.(raftPanic)
	if !ok {
		panic(r) //nolint:forbidigo // a panic that is not Raft's goes on as it was
	}
	*err = errors.Join(*err, fmt.Errorf("%w: %s", ErrRaft, p.msg))
}

// Failure is what a driver does once its node failed. The node driver acts
// on FailureOf, so it treats every error alike.
type Failure int

const (
	// Stop ends the process; a new one recovers from the disk. This is
	// the effect of a failed disk and any error with no
	// other effect.
	Stop Failure = iota
	// Reload ends the run, and a new core starts from the log in the same
	// process.
	Reload
	// Fence stops the node because its own log or values are damaged. A
	// start on that disk refuses it too, until it is replaced.
	Fence
	// Leave stops a node the cluster removed, for good.
	Leave
)

// FailureOf is the effect of err, from the core, the disk or a snapshot
// install.
func FailureOf(err error) Failure {
	switch {
	case errors.Is(err, ErrFetch), errors.Is(err, ErrInstalled):
		return Reload
	case errors.Is(err, ErrRemoved):
		return Leave
	case errors.Is(err, wal.ErrCorrupt), errors.Is(err, mvcc.ErrBadValue):
		return Fence
	}
	return Stop
}
