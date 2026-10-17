package core

import (
	"errors"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// ErrDisk marks a failed write or sync of the node's own disk. The file
// is then in an unknown state, so the node stops and never retries it.
var ErrDisk = errors.New("core: the disk failed")

// ErrFetch marks a snapshot install that failed for any reason but the
// node's own disk: the leader, the link or the stream. Raft cannot take
// the snapshot back, so the driver starts a new core from its log, as a
// restart does, and Raft sends another snapshot.
var ErrFetch = errors.New("core: the snapshot fetch failed")

// Failure is what a driver does once its node failed. The real node and
// the simulator both act on FailureOf, so they treat every error alike.
type Failure int

const (
	// Stop ends the process; a new one recovers from the disk. This is
	// the effect of a failed disk, a stalled commit and any error with no
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
	case errors.Is(err, ErrFetch):
		return Reload
	case errors.Is(err, ErrRemoved):
		return Leave
	case errors.Is(err, wal.ErrCorrupt), errors.Is(err, mvcc.ErrBadValue):
		return Fence
	}
	return Stop
}
