//go:build planted

package wl

import "github.com/djosh34/tigerstore/internal/planted"

// The cursor rebuilds live events from changes, not watches.events. Carry
// the EXACT live delivery sequence already mutated by B06a/B06b across
// that handoff. Do not mutate changes (other readers must not be affected),
// plant errors in checksums/framing, or broaden the bugs to old catch-up.
// This state exists only in planted builds, follows watcher incarnation
// and bigWatch copies, and retires at queue commit/cancel/store replacement.
type watchDeliveryPlant struct {
	st     *store
	events []change
}

func (p *watchDeliveryPlant) capture(n *node, c *client, w *watches, first int32, size int) {
	if !planted.On("B06a") && !planted.On("B06b") {
		return
	}
	p.reset()
	// Main discards the mutated live list when it must catch up rather
	// than deliver it. Preserve that scope without reserving any credit.
	past := n.roomNeeded(c, int64(5+size))
	if c.slow() || past > 0 && n.reserved+n.coveredRoom+past > n.sizes.reserve() {
		return
	}
	count := 0
	for e := first; e >= 0; e = w.events[e].next {
		count++
	}
	p.st = n.st
	p.events = make([]change, 0, count)
	for e := first; e >= 0; e = w.events[e].next {
		ev := &w.events[e]
		p.events = append(p.events, change{rev: ev.rev, key: ev.key, del: ev.del})
	}
}

func (p *watchDeliveryPlant) reset() { p.st, p.events = nil, nil }

func (p *watchDeliveryPlant) retire(next int64) {
	if len(p.events) > 0 && (next == 0 || next > p.events[len(p.events)-1].rev) {
		p.reset()
	}
}

func (p *watchDeliveryPlant) each(n *node, st *store, x *watcher, j, from int, f func(int, bigEv) bool) (bool, error) {
	if p.st != st {
		p.reset()
		return false, nil
	}
	if len(p.events) == 0 || j >= len(st.changes) {
		return false, nil
	}
	rev := st.changes[j].rev
	if rev < p.events[0].rev || rev > p.events[len(p.events)-1].rev {
		return false, nil
	}
	ordinal := 0
	for _, wanted := range p.events {
		if wanted.rev != rev {
			continue
		}
		off := ordinal
		ordinal++
		if off < from {
			continue
		}
		found, more := false, true
		err := n.eachUnmutatedRevEvent(st, x, j, 0, func(_ int, ev bigEv) bool {
			if ev.ch != wanted {
				return true
			}
			found = true
			more = f(off, ev)
			return false
		})
		if err != nil {
			return true, err
		}
		if !found {
			return true, errPrevCompacted
		}
		if !more {
			return true, nil
		}
	}
	return true, nil
}
