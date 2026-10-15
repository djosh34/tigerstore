package core

import (
	"bytes"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/planted"
)

// The rules below follow etcd v3.7.2's watch server
// (server/etcdserver/api/v3rpc/watch.go and server/storage/mvcc).

// WatchResponse is a response on one watch stream.
type WatchResponse struct {
	Stream uint64
	Resp   *pb.WatchResponse
}

// invalidWatchID marks responses that belong to no single watcher.
const invalidWatchID = -1

// catchUpRevs caps the revisions in one response while a watcher catches
// up on history, like etcd's watchBatchMaxRevs. A revision is never split
// across responses, and each response has the current revision as its
// header, as in etcd.
const catchUpRevs = 1000

const (
	errDuplicateWatchID = "mvcc: duplicate watch ID provided on the WatchStream"
	errEmptyWatchRange  = "mvcc: watcher range is empty"
)

type watcher struct {
	stream   *watchStream
	id       int64
	key, end []byte
	prevKV   bool
	noPut    bool
	noDelete bool
	progress bool
	// start is the first revision the watcher delivers. Changes are
	// matched after it is created, so later revisions are always newer;
	// start only holds back a watcher that asked for a future revision.
	start int64
	// asked is the start revision the client asked for, 0 for "from now".
	asked   int64
	pending []*mvccpb.Event
	// waits are the value reads the pending events wait for.
	waits []*ValueRead
	// sent is set when the watcher got events since the last progress
	// tick; etcd then skips its progress notification.
	sent bool
	dead bool
}

// ahead reports whether the watcher asked to start above the current
// revision. etcd sends no progress for such a watcher, because a client
// resumes from progress revision + 1 and would go back.
func (w *watcher) ahead(rev int64) bool { return rev < w.asked }

type watchStream struct {
	id     uint64
	nextID int64
	byID   map[int64]*watcher
	// held are responses that wait for value reads, their own or those of
	// a response before them.
	held []heldWatch
}

// heldWatch is a watch response and the value reads it waits for.
type heldWatch struct {
	resp  WatchResponse
	waits []*ValueRead
}

func (n *Node) watchHeader() *pb.ResponseHeader {
	h := &pb.ResponseHeader{Revision: n.kv.Rev()}
	n.fillHeader(h)
	return h
}

// watchReply sends a response on a stream once the value reads in waits
// are done and every response before it went out.
func (n *Node) watchReply(s *watchStream, r *pb.WatchResponse, waits []*ValueRead) {
	r.Header = n.watchHeader()
	resp := WatchResponse{Stream: s.id, Resp: r}
	waits = slices.DeleteFunc(waits, func(v *ValueRead) bool { return v.done })
	if len(s.held) == 0 && len(waits) == 0 {
		n.out.Watch = append(n.out.Watch, resp)
		return
	}
	s.held = append(s.held, heldWatch{resp: resp, waits: waits})
	for _, v := range waits {
		if k := len(v.streams); k == 0 || v.streams[k-1] != s {
			v.streams = append(v.streams, s)
		}
	}
}

// releaseWatches sends a stream's held responses whose reads are done, in
// order, up to the first that still waits.
func (n *Node) releaseWatches(s *watchStream) {
	if n.streams[s.id] != s {
		return
	}
	i := 0
	for ; i < len(s.held) && allRead(s.held[i].waits); i++ {
		n.out.Watch = append(n.out.Watch, s.held[i].resp)
	}
	clear(s.held[:i])
	s.held = s.held[i:]
}

func allRead(rs []*ValueRead) bool {
	for _, r := range rs {
		if !r.done {
			return false
		}
	}
	return true
}

// watchEvent takes the value fills of an event just built and returns the
// read that brings them, or nil if it has none. The events of one turn
// share one read.
func (n *Node) watchEvent(ev *mvccpb.Event) *ValueRead {
	fills := n.kv.TakeFills()
	if ev == nil || len(fills) == 0 {
		return nil
	}
	if n.watchRead == nil {
		n.watchRead = n.readValues(nil)
	}
	n.watchRead.fills = append(n.watchRead.fills, fills...)
	return n.watchRead
}

// addWait adds r to waits unless it is nil or already the last.
func addWait(waits []*ValueRead, r *ValueRead) []*ValueRead {
	if r == nil || (len(waits) > 0 && waits[len(waits)-1] == r) {
		return waits
	}
	return append(waits, r)
}

// Watch handles one request on a watch stream. The stream exists from its
// first request until CloseWatchStream.
func (n *Node) Watch(stream uint64, r *pb.WatchRequest) {
	if n.err != nil {
		return
	}
	s, ok := n.streams[stream]
	if !ok {
		s = &watchStream{id: stream, byID: map[int64]*watcher{}}
		n.streams[stream] = s
	}
	switch {
	case r.GetCreateRequest() != nil:
		n.createWatch(s, r.GetCreateRequest())
	case r.GetCancelRequest() != nil:
		id := r.GetCancelRequest().WatchId
		if w, ok := s.byID[id]; ok {
			n.removeWatch(w)
			n.watchReply(s, &pb.WatchResponse{WatchId: id, Canceled: true}, nil)
		}
	case r.GetProgressRequest() != nil:
		// Every watcher is synced, because catching up happens inside one
		// call. Like etcd, answer once for the stream, unless it has no
		// watchers or one of them starts above the current revision.
		n.flushWatches()
		rev := n.kv.Rev()
		if len(s.byID) == 0 {
			return
		}
		for _, w := range n.watchers {
			if !w.dead && w.stream == s && w.ahead(rev) {
				return
			}
		}
		n.watchReply(s, &pb.WatchResponse{WatchId: invalidWatchID}, nil)
	}
}

// WatcherCount is the number of live watchers.
func (n *Node) WatcherCount() int {
	c := 0
	for _, w := range n.watchers {
		if !w.dead {
			c++
		}
	}
	return c
}

// CloseWatchStream drops a stream and its watchers.
func (n *Node) CloseWatchStream(stream uint64) {
	s, ok := n.streams[stream]
	if !ok {
		return
	}
	if !planted.On("B18") {
		for _, w := range s.byID {
			w.dead = true
		}
	}
	delete(n.streams, stream)
	n.pruneWatchers()
}

func (n *Node) removeWatch(w *watcher) {
	w.dead = true
	delete(w.stream.byID, w.id)
	n.pruneWatchers()
}

// pruneWatchers drops dead watchers once they are half of the list.
func (n *Node) pruneWatchers() {
	dead := 0
	for _, w := range n.watchers {
		if w.dead {
			dead++
		}
	}
	if dead*2 < len(n.watchers) {
		return
	}
	live := n.watchers[:0]
	for _, w := range n.watchers {
		if !w.dead {
			live = append(live, w)
		}
	}
	clear(n.watchers[len(live):])
	n.watchers = live
}

func (n *Node) createWatch(s *watchStream, c *pb.WatchCreateRequest) {
	created := &pb.WatchResponse{Created: true}
	if c.StartRevision < 0 {
		created.WatchId, created.Canceled = invalidWatchID, true
		created.CancelReason = rpctypes.ErrCompacted.Error()
		n.watchReply(s, created, nil)
		return
	}
	key := c.Key
	if len(key) == 0 {
		key = []byte{0}
	}
	fromKey := len(c.RangeEnd) == 1 && c.RangeEnd[0] == 0
	if len(c.RangeEnd) != 0 && !fromKey && bytes.Compare(key, c.RangeEnd) >= 0 {
		created.WatchId, created.Canceled, created.CancelReason = invalidWatchID, true, errEmptyWatchRange
		n.watchReply(s, created, nil)
		return
	}
	id := c.WatchId
	if id == 0 {
		for s.byID[s.nextID] != nil {
			s.nextID++
		}
		id = s.nextID
		s.nextID++
	} else if s.byID[id] != nil {
		created.WatchId, created.Canceled, created.CancelReason = invalidWatchID, true, errDuplicateWatchID
		n.watchReply(s, created, nil)
		return
	}
	w := &watcher{stream: s, id: id, key: key, end: c.RangeEnd, prevKV: c.PrevKv, progress: c.ProgressNotify, start: c.StartRevision, asked: c.StartRevision}
	if planted.On("B07") && w.start > 1 {
		w.start--
	}
	for _, f := range c.Filters {
		switch f {
		case pb.WatchCreateRequest_NOPUT:
			w.noPut = true
		case pb.WatchCreateRequest_NODELETE:
			w.noDelete = true
		}
	}
	// Events already applied in this turn are not sent yet; send them
	// first, so the new watcher's history starts after them.
	n.flushWatches()
	rev := n.kv.Rev()
	if w.start == 0 {
		w.start = rev + 1
	}
	created.WatchId = id
	n.watchReply(s, created, nil)
	if w.start <= rev {
		// Like etcd, a watch from below the compaction is canceled. A
		// watch with prev_kv from the compaction revision itself is too:
		// the versions before it are gone, and etcd would send its events
		// without previous values, which Kubernetes rejects.
		if c := n.kv.CompactRev(); w.start < c || (w.prevKV && w.start == c) {
			r := &pb.WatchResponse{WatchId: id, CompactRevision: c, Canceled: true}
			n.watchReply(s, r, nil)
			// etcd's compaction cancel carries header revision 0.
			r.Header.Revision = 0
			return
		}
		var evs []*mvccpb.Event
		var waits []*ValueRead
		revs, last := 0, int64(0)
		for _, ch := range n.kv.History(w.key, w.end, w.start) {
			ev, err := w.event(ch)
			if err != nil {
				n.fail(err)
				return
			}
			read := n.watchEvent(ev)
			if ev == nil {
				continue
			}
			if r := ev.Kv.ModRevision; r != last {
				if revs == catchUpRevs {
					n.watchReply(s, &pb.WatchResponse{WatchId: id, Events: evs}, waits)
					evs, waits, revs = nil, nil, 0
				}
				revs, last = revs+1, r
			}
			evs = append(evs, ev)
			waits = addWait(waits, read)
		}
		if len(evs) > 0 {
			n.watchReply(s, &pb.WatchResponse{WatchId: id, Events: evs}, waits)
		}
	}
	s.byID[id] = w
	n.watchers = append(n.watchers, w)
}

// event returns the watch event of a change for this watcher, or nil if
// the watcher filters it out. An error is a value that could not be read,
// which stops the node. Values only the log has come as fills.
func (w *watcher) event(c mvcc.Change) (*mvccpb.Event, error) {
	ev, err := c.Event(w.withPrev())
	if err != nil || w.filters(ev) {
		return nil, err
	}
	return ev, nil
}

// withPrev reports whether the watcher's events carry previous values.
func (w *watcher) withPrev() bool { return w.prevKV && !planted.On("B06c") }

// filters reports whether the watcher leaves out an event.
func (w *watcher) filters(ev *mvccpb.Event) bool {
	return (ev.Type == mvccpb.Event_PUT && w.noPut) || (ev.Type == mvccpb.Event_DELETE && w.noDelete)
}

// matchWatches queues the changes of the write just applied for every
// watcher that covers them. Each change's event is built once with and
// once without the previous value, at most, and the watchers share it.
func (n *Node) matchWatches() {
	changes := n.state.Changes()
	if len(changes) == 0 || len(n.watchers) == 0 {
		return
	}
	for i, c := range changes {
		if planted.On("B06a") && len(changes) > 1 && i == len(changes)-1 {
			continue
		}
		var built [2]*mvccpb.Event
		var reads [2]*ValueRead
		for _, w := range n.watchers {
			if w.dead || c.ModRev() < w.start || !mvcc.InRange(c.Key(), w.key, w.end) {
				continue
			}
			k := 0
			if w.withPrev() {
				k = 1
			}
			if built[k] == nil {
				ev, err := c.Event(k == 1)
				if err != nil {
					n.fail(err)
					return
				}
				built[k], reads[k] = ev, n.watchEvent(ev)
			}
			if w.filters(built[k]) {
				continue
			}
			w.pending = append(w.pending, built[k])
			w.waits = addWait(w.waits, reads[k])
			if planted.On("B06b") && i == 0 {
				w.pending = append(w.pending, built[k])
			}
		}
	}
}

// flushWatches sends every watcher's queued events in one response with
// the current revision.
func (n *Node) flushWatches() {
	for _, w := range n.watchers {
		if w.dead || len(w.pending) == 0 {
			continue
		}
		n.watchReply(w.stream, &pb.WatchResponse{WatchId: w.id, Events: w.pending}, w.waits)
		w.pending, w.waits = nil, nil
		w.sent = true
	}
}

// progressNotify sends a progress notification to every watcher that
// asked for them, unless it got events since the last notification or it
// starts above the current revision, as etcd does.
func (n *Node) progressNotify() {
	n.flushWatches()
	rev := n.kv.Rev()
	for _, w := range n.watchers {
		if !w.dead && w.progress && !w.sent && !w.ahead(rev) {
			r := &pb.WatchResponse{WatchId: w.id}
			n.watchReply(w.stream, r, nil)
		}
		w.sent = false
	}
}
