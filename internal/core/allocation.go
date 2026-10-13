package core

// Workspace is transient request state supplied by the allocation experiment.
// It must remain owned by the driver until its reply is consumed. Entry.Data is
// deliberately never stored here. Only wire Puts use this optional API.
type Workspace struct {
	p     pending
	Reply Reply
	Next  *Workspace
}

func (n *Node) answerRequest(r Request, reply Reply) {
	if r.Workspace == nil {
		n.out.Replies = append(n.out.Replies, reply)
		return
	}
	w := r.Workspace
	w.Reply = reply
	w.Next = n.out.ManagedReplies
	n.out.ManagedReplies = w
}

// retireManaged removes ALL transient core references before the driver can
// return a workspace. In particular settle alone does not retire timeouts or
// a proposal that expired before it was encoded.
// EnableAllocationWorkspace reserves descriptor-list/map capacity before Run.
// Retained entry encoding, effects for WAL/peers and MVCC history stay owned by
// their original subsystems, not this transient allocator.
func (n *Node) EnableAllocationWorkspace(limit int) {
	n.allocationWorkspace = true
	n.proposals = make([]*pending, 0, limit)
	n.timeouts = make([]*pending, 0, limit)
	n.waiting = make(map[uint64]*pending, limit)
}

func (n *Node) retireManaged() {
	if !n.allocationWorkspace {
		return
	}
	n.proposals = filterManaged(n.proposals, false)
	n.timeouts = filterManaged(n.timeouts, false)
}

func filterManaged(ps []*pending, all bool) []*pending {
	kept := ps[:0]
	for _, p := range ps {
		if p.req.Workspace != nil && (all || p.done) {
			// Do not reset done until all lists have been filtered.
			continue
		}
		kept = append(kept, p)
	}
	clear(ps[len(kept):])
	return kept
}

// AbandonManaged is called only after the driver has stopped using the core.
// The driver then fails accepted requests, including ones left in its inbox.
func (n *Node) AbandonManaged() {
	n.proposals = filterManaged(n.proposals, true)
	n.timeouts = filterManaged(n.timeouts, true)
	for id, p := range n.waiting {
		if p.req.Workspace != nil {
			delete(n.waiting, id)
		}
	}
	n.out.ManagedReplies = nil
}

// Clear removes aliases after all core holders have retired, before a reply
// callback can make its containing slot reusable.
func (w *Workspace) Clear() { *w = Workspace{} }
