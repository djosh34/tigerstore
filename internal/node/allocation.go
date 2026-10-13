package node

import (
	"context"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// ManagedBatch is an immutable linked handoff owned by the experimental
// transport. The links and wire bytes stay pinned through completion.
type ManagedBatch interface {
	ManagedRequest() (core.Request, any)
	NextManagedRequest() ManagedBatch
}

// DirectCompleter avoids a freshly allocated completion slice. CompleteOne
// must only copy into bounded storage, never encode or wait for socket output.
type DirectCompleter interface{ CompleteOne(any, core.Reply) }

func (n *Node) SubmitManagedBatch(ctx context.Context, head ManagedBatch, count int, to DirectCompleter) error {
	n.managedMu.RLock()
	defer n.managedMu.RUnlock()
	select {
	case <-n.managedStop:
		return rpctypes.ErrGRPCStopped
	default:
	}
	first := n.nextID.Add(uint64(count)) - uint64(count) + 1
	select {
	case n.inbox <- input{managed: head, managedFirst: first, direct: to}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.managedStop:
		return rpctypes.ErrGRPCStopped
	}
}

func completeManaged(head *core.Workspace, waiters map[uint64]waiter) {
	for head != nil {
		w := head
		head = w.Next
		r := w.Reply
		w.Clear() // callback may immediately recycle the enclosing slot
		if to, ok := waiters[r.ID]; ok {
			delete(waiters, r.ID)
			to.direct.CompleteOne(to.tag, r)
		}
	}
}

// shutdownManaged retires core and inbox owners before making any slot
// reusable. Closing managedStop unblocks a submitter holding the read lock;
// the write lock then excludes every possible late accepted inbox send.
func (n *Node) shutdownManaged(waiters map[uint64]waiter) {
	close(n.managedStop)
	n.managedMu.Lock()
	defer n.managedMu.Unlock()
	n.core.AbandonManaged()
	for id, w := range waiters {
		if w.direct != nil {
			delete(waiters, id)
			w.direct.CompleteOne(w.tag, core.Reply{ID: id, Err: rpctypes.ErrGRPCStopped})
		}
	}
	for {
		select {
		case in := <-n.inbox:
			for b := in.managed; b != nil; {
				next := b.NextManagedRequest() // callback may recycle this link
				_, tag := b.ManagedRequest()
				in.direct.CompleteOne(tag, core.Reply{Err: rpctypes.ErrGRPCStopped})
				b = next
			}
		default:
			return
		}
	}
}
