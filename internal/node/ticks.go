package node

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// AdjustTicks is etcd's adjustTicks, which it runs as it starts: a node
// alone in its cluster moves its election clock to one tick before its
// timeout, so that it leads at once; with initial, a node of a larger
// cluster waits up to wait for connections to more than one peer and
// then moves it to two ticks before, so that a cluster that starts
// together elects a leader sooner. activePeers returns the peers
// connected and a channel closed once that may have changed; etcd looks
// every 50 ms instead.
func (n *Node) AdjustTicks(ctx context.Context, activePeers func() (int, <-chan struct{}), initial bool, wait time.Duration) error {
	m, err := n.Metrics(ctx)
	if err != nil {
		return err
	}
	election := n.cfg.Core.ElectionTicks
	if len(m.Members) == 1 {
		n.logf("started as single-node; fast-forwarding %d election ticks", election-1)
		return n.advance(ctx, election-1)
	}
	if !initial {
		n.logf("skipping initial election tick advance")
		return nil
	}
	timeout := time.After(wait)
	for {
		active, moved := activePeers()
		if active > 1 {
			n.logf("initialized peer connections; fast-forwarding %d election ticks", election-2)
			return n.advance(ctx, election-2)
		}
		select {
		case <-moved:
		case <-timeout:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (n *Node) advance(ctx context.Context, ticks int) error {
	if ticks <= 0 {
		return nil
	}
	select {
	case n.inbox <- input{advance: ticks}:
		n.wake()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
}
