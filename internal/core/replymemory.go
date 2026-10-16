package core

import (
	"errors"

	"github.com/djosh34/tigerstore/internal/mvcc"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errReplyBusy = errors.New("core: answers occupy the memory budget")

func (n *Node) replyReservation(held *int64) func(int64) error {
	if n.cfg.MemoryBytes <= 0 {
		return nil
	}
	return func(bytes int64) error {
		if err := n.resizeReply(*held, bytes); err != nil {
			return err
		}
		*held = bytes
		return nil
	}
}

// ReleaseReplyBytes reports reservations the transport no longer needs.
// The driver supplies this event before Process, just as it supplies
// ticks and disk completions. It never changes the core from a writer.
func (n *Node) ReleaseReplyBytes(bytes int64) { n.replyBytes -= bytes }

// SetReplyBytes restores the reservations still on client connections
// when the driver reloads the core after a snapshot install failure.
func (n *Node) SetReplyBytes(bytes int64) { n.replyBytes = bytes }

func (n *Node) reserveReply(bytes int64) error {
	return n.resizeReply(0, bytes)
}

// resizeReply changes one answer's reservation. Its own earlier charge
// does not make it wait for itself while building candidate metadata.
func (n *Node) resizeReply(held, bytes int64) error {
	if n.cfg.MemoryBytes <= 0 {
		return nil
	}
	if bytes > n.cfg.MemoryBytes {
		return status.Error(codes.ResourceExhausted, "tigerstore: answer exceeds the memory budget; use a smaller limit")
	}
	// An answer waits only for other answers to go out. With none out,
	// it goes even when the index and the entries leave less room, so
	// that a full index does not refuse every read.
	available := n.cfg.MemoryBytes - n.kv.Bytes() - n.EntryBytes()
	if n.replyBytes > held && bytes > available-(n.replyBytes-held) {
		return errReplyBusy
	}
	n.replyBytes += bytes - held
	// Evict before allocating values, not at the next Process call.
	n.fitCache()
	return nil
}

func (n *Node) newComparisons() *mvcc.CompareProgress {
	budget := int64(1 << 20)
	if n.cfg.MemoryBytes > 0 {
		budget = min(budget, n.cfg.MemoryBytes)
	}
	return &mvcc.CompareProgress{Budget: budget}
}

// reserveComparison admits one bounded input batch. A single value is
// the minimum read unit. Even a committed transaction must make progress
// when that value exceeds a tiny configured budget, but never alongside
// other reservations. Recovery reads inline, before a driver can release
// answers, and may also need this one bounded batch to finish installing.
func (n *Node) reserveComparison(cold *mvcc.ColdError) (int64, error) {
	if n.cfg.MemoryBytes <= 0 {
		return 0, nil
	}
	bytes := mvcc.CompareBytes(cold.Need)
	if n.readInline || (len(cold.Need) == 1 && bytes > n.cfg.MemoryBytes && n.replyBytes == 0) {
		n.replyBytes += bytes
		n.fitCache()
		return bytes, nil
	}
	if bytes > n.cfg.MemoryBytes {
		return 0, errReplyBusy
	}
	if err := n.reserveReply(bytes); err != nil {
		return 0, err
	}
	return bytes, nil
}

// retryReplies retries reads whose answers did not fit. No values were
// allocated for them. Reads keep their normal deadline and authorization
// checks, and run against the state at their eventual execution.
func (n *Node) retryReplies() {
	waiting := n.replyWait
	n.replyWait = nil
	for _, p := range waiting {
		if n.now >= p.deadline {
			n.finish(p, rpctypes.ErrGRPCTimeout)
		} else {
			n.serveRead(p)
		}
	}
}
