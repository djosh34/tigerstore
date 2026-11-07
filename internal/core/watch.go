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

// WatchResponse is a response on one watch stream, or a part of one.
// Resp.Fragment marks the response of a watcher created with fragment
// set: the server splits it as etcd does when it is large, once its
// values are read, and clears the mark.
//
// A response whose events for a watcher pass a piece, a catch-up of many
// revisions or one large revision, is built in parts, as etcd sends it in
// one message (#428): its first part has EventBytes, the size of all its
// events as the message encodes them; every part but the last has More,
// and every part but the first has Cont and only events. To a watcher
// that asked for fragments it goes as etcd's fragments instead: responses
// of about a piece each, all with Fragmented set, and Resp.Fragment set
// on all but the last. Either way the stream sends nothing else between
// them.
type WatchResponse struct {
	Stream     uint64
	Resp       *pb.WatchResponse
	EventBytes int64
	More, Cont bool
	Fragmented bool
	// Broken says that the stream cannot finish a response in parts: a
	// compaction passed its revision, or its watcher was cancelled,
	// before its last part. The driver ends the stream.
	Broken bool
	// part marks a part of a response in parts, fragments included.
	part bool
}

// invalidWatchID marks responses that belong to no single watcher.
const invalidWatchID = -1

// catchUpRevs caps the revisions in one response, like etcd's
// watchBatchMaxRevs. A revision is never split across responses, and each
// response has the current revision as its header, as in etcd.
const catchUpRevs = 1000

// catchUpScan caps the changes a catch-up response reads from the history
// for one watcher, whether they match it or not, and catchUpBytes its
// events, half of gRPC's largest message: past either, the next revision
// goes in the next response.
const (
	catchUpScan  = 1 << 16
	catchUpBytes = 1 << 30
)

const (
	errDuplicateWatchID = "mvcc: duplicate watch ID provided on the WatchStream"
	errEmptyWatchRange  = "mvcc: watcher range is empty"
)

// A watcher whose stream has room gets the events of each write as it
// applies (matchWatches). One whose stream is full falls behind: it drops
// what it held and catches up from its position in the store's history,
// a piece at a time, as its stream has room again (catchUp), as etcd's
// unsynced watchers do. A stream is never closed for being slow; a
// watcher that a compaction passes is cancelled with its revision.
type watcher struct {
	stream   *watchStream
	id       int64
	key, end []byte
	prevKV   bool
	noPut    bool
	noDelete bool
	progress bool
	fragment bool
	// start is the first revision the watcher delivers. Changes are
	// matched after it is created, so later revisions are always newer;
	// start only holds back a watcher that asked for a future revision.
	start int64
	// asked is the start revision the client asked for, 0 for "from now".
	asked int64
	// pending are the events matched since the last response, the first
	// at history position pendFrom, of pendRevs revisions and pendBytes
	// bytes, the last at revision pendLast.
	pending   []*mvccpb.Event
	pendFrom  int64
	pendBytes int
	pendRevs  int
	pendLast  int64
	// waits are the value reads the pending events wait for.
	waits []*ValueRead
	// behind says the watcher catches up from history position next, and
	// big is the revision it sends in parts.
	behind bool
	next   int64
	big    *bigResponse
	// sent is set when the watcher got events since the last progress
	// tick; etcd then skips its progress notification.
	sent bool
	dead bool
}

// bigResponse is a response whose events for a watcher pass a piece,
// which goes out in parts: its changes are at history positions pos up to
// end, whole revisions, and its events take bytes.
type bigResponse struct {
	pos, end int64
	bytes    int64
	started  bool
}

// ahead reports whether the watcher asked to start above the current
// revision. etcd sends no progress for such a watcher, because a client
// resumes from progress revision + 1 and would go back.
func (w *watcher) ahead(rev int64) bool { return rev < w.asked }

type watchStream struct {
	id uint64
	// caller is the client of the stream, whose watches need its read
	// permission while auth is enabled.
	caller Caller
	nextID int64
	byID   map[int64]*watcher
	// held are responses that wait for value reads, their own or those of
	// a response before them.
	held []heldWatch
	// room and slots are the bytes and responses the stream may still
	// take, as its driver last said (WatchRoom), less those sent since.
	room, slots int64
	// big is the watcher whose response in parts the stream is sending,
	// and after the responses that wait for its last part: a gRPC message
	// takes nothing else in its middle.
	big   *watcher
	after []heldWatch
}

// hasRoom reports whether the stream may take another response with
// events.
func (s *watchStream) hasRoom() bool { return s.room > 0 && s.slots > 0 }

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

// replyBytes is what a response counts against its stream's room besides
// its events.
const replyBytes = 64

// watchReply sends a response on a stream once the value reads in waits
// are done and every response before it went out. events is the size of
// its events, which with the rest of it counts against the stream's room.
func (n *Node) watchReply(s *watchStream, r *pb.WatchResponse, waits []*ValueRead, events int) {
	n.watchPart(s, WatchResponse{Stream: s.id, Resp: r}, waits, events, false)
}

// watchPart sends a response, or a part of one, as watchReply does; more
// says that parts follow it. Every part but the first goes without a
// header. While a stream sends a response in parts, the rest of its
// output waits for the last part.
func (n *Node) watchPart(s *watchStream, resp WatchResponse, waits []*ValueRead, events int, more bool) {
	if !resp.Cont {
		resp.Resp.Header = n.watchHeader()
	}
	n.counters.Events += int64(len(resp.Resp.Events))
	s.room -= int64(events + replyBytes)
	s.slots--
	if s.big != nil && !resp.part {
		s.after = append(s.after, heldWatch{resp: resp, waits: waits})
		return
	}
	n.queueWatch(s, resp, waits)
	if resp.part && !more {
		// The last part: what waited for it goes now, in order.
		after := s.after
		s.big, s.after = nil, nil
		for _, h := range after {
			n.queueWatch(s, h.resp, h.waits)
		}
	}
}

// queueWatch sends a response once the reads in waits are done and every
// response before it went out.
func (n *Node) queueWatch(s *watchStream, resp WatchResponse, waits []*ValueRead) {
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

// WatchRoom tells the node how many more bytes and responses with events
// a watch stream may take: what its driver holds for the client counts
// against both. Without room, the stream's watchers fall behind and catch
// up once it has room again; responses without events go out anyway.
func (n *Node) WatchRoom(stream uint64, bytes, responses int64) {
	if s, ok := n.streams[stream]; ok {
		s.room, s.slots = bytes, responses
	}
}

// Watch handles one request on a watch stream. The stream exists from its
// first request until CloseWatchStream.
func (n *Node) Watch(stream uint64, c Caller, r *pb.WatchRequest) {
	if n.err != nil {
		return
	}
	s, ok := n.streams[stream]
	if !ok {
		s = &watchStream{id: stream, byID: map[int64]*watcher{}, caller: c}
		n.streams[stream] = s
	}
	switch {
	case r.GetCreateRequest() != nil:
		n.createWatch(s, r.GetCreateRequest())
	case r.GetCancelRequest() != nil:
		id := r.GetCancelRequest().WatchId
		if w, ok := s.byID[id]; ok {
			n.removeWatch(w)
			n.watchReply(s, &pb.WatchResponse{WatchId: id, Canceled: true}, nil, 0)
		}
	case r.GetProgressRequest() != nil:
		// Like etcd, answer once for the stream, unless it has no
		// watchers, one of them starts above the current revision, or one
		// is still catching up: its events below the revision are not
		// sent yet.
		n.flushWatches()
		rev := n.kv.Rev()
		if len(s.byID) == 0 {
			return
		}
		for _, w := range n.watchers {
			if !w.dead && w.stream == s && (w.ahead(rev) || w.behind) {
				return
			}
		}
		n.watchReply(s, &pb.WatchResponse{WatchId: invalidWatchID}, nil, 0)
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
	if s := w.stream; s.big == w && n.streams[s.id] == s {
		// Its response in parts is half out, and no other message can
		// follow on the stream: the stream breaks, and its client
		// resumes.
		n.out.Watch = append(n.out.Watch, WatchResponse{Stream: s.id, Broken: true})
		n.CloseWatchStream(s.id)
	}
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
		n.watchReply(s, created, nil, 0)
		return
	}
	key := c.Key
	if len(key) == 0 {
		key = []byte{0}
	}
	if err := n.watchAllowed(s.caller, key, c.RangeEnd); err != nil {
		created.WatchId, created.Canceled, created.CancelReason = invalidWatchID, true, err.Error()
		n.watchReply(s, created, nil, 0)
		return
	}
	fromKey := len(c.RangeEnd) == 1 && c.RangeEnd[0] == 0
	if len(c.RangeEnd) != 0 && !fromKey && bytes.Compare(key, c.RangeEnd) >= 0 {
		created.WatchId, created.Canceled, created.CancelReason = invalidWatchID, true, errEmptyWatchRange
		n.watchReply(s, created, nil, 0)
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
		n.watchReply(s, created, nil, 0)
		return
	}
	w := &watcher{stream: s, id: id, key: key, end: c.RangeEnd, prevKV: c.PrevKv, progress: c.ProgressNotify, fragment: c.Fragment, start: c.StartRevision, asked: c.StartRevision}
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
	n.watchReply(s, created, nil, 0)
	if w.start <= rev || n.revoke != nil {
		// Like etcd, a watch from below the compaction is canceled. A
		// watch with prev_kv from the compaction revision itself is too:
		// the versions before it are gone, and etcd would send its events
		// without previous values, which Kubernetes rejects.
		if c := n.kv.CompactRev(); w.start < c || (w.prevKV && w.start == c) {
			n.compacted(w, c)
			return
		}
		// It catches up from the history (catchUp), a piece at a time;
		// during a revoke in steps it does, too, as the revoke's revision
		// is not whole yet.
		w.behind, w.next = true, n.kv.HistoryFrom(w.start)
	}
	s.byID[id] = w
	n.watchers = append(n.watchers, w)
}

// compacted cancels a watcher whose start a compaction at rev passed.
func (n *Node) compacted(w *watcher, rev int64) {
	r := &pb.WatchResponse{WatchId: w.id, CompactRevision: rev, Canceled: true}
	n.watchReply(w.stream, r, nil, 0)
	// etcd's compaction cancel carries header revision 0.
	r.Header.Revision = 0
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

// takes reports whether the watcher delivers the change at revision rev
// of key, filters aside.
func (w *watcher) takes(key []byte, rev int64) bool {
	return rev >= w.start && mvcc.InRange(key, w.key, w.end)
}

// wants reports whether the watcher delivers change c, its filters
// included, without building its event. A change a compaction removed
// goes to no watcher.
func (w *watcher) wants(c mvcc.Change) bool {
	if c.Gone() || !w.takes(c.Key(), c.ModRev()) {
		return false
	}
	if c.Deletes() {
		return !w.noDelete
	}
	return !w.noPut
}

// matchWatches queues the changes of the write just applied for every
// watcher that is not behind and covers them. Each change's event is
// built once with and once without the previous value, at most, and the
// watchers share it. A watcher's queue holds at most catchUpRevs
// revisions and about PieceBytes of events: past that it sends them, or
// falls behind if its stream has no room.
func (n *Node) matchWatches() {
	changes := n.state.Changes()
	if len(changes) == 0 || len(n.watchers) == 0 {
		return
	}
	from := n.kv.ChangesFrom()
	for i, c := range changes {
		if planted.On("B06a") && len(changes) > 1 && i == len(changes)-1 {
			continue
		}
		pos := from + int64(i)
		var built [2]*mvccpb.Event
		var reads [2]*ValueRead
		var sizes [2]int
		for _, w := range n.watchers {
			if w.dead || w.behind || !w.takes(c.Key(), c.ModRev()) {
				continue
			}
			if n.revoke != nil {
				// A revoke in steps: its revision is not whole yet, so
				// the watcher reads it from the history once it is.
				n.fallBehind(w)
				continue
			}
			if c.ModRev() != w.pendLast && len(w.pending) > 0 && (w.pendRevs >= catchUpRevs || w.pendBytes >= n.cfg.PieceBytes) {
				if n.flushWatcher(w); w.behind {
					continue
				}
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
				built[k], reads[k], sizes[k] = ev, n.watchEvent(ev), c.EventSize(k == 1)
			}
			if w.filters(built[k]) {
				continue
			}
			if len(w.pending) == 0 {
				w.pendFrom = pos
			}
			if c.ModRev() != w.pendLast {
				w.pendRevs++
				w.pendLast = c.ModRev()
			}
			w.pending = append(w.pending, built[k])
			w.pendBytes += sizes[k]
			w.waits = addWait(w.waits, reads[k])
			if planted.On("B06b") && i == 0 {
				w.pending = append(w.pending, built[k])
			}
			if w.pendBytes > 2*n.cfg.PieceBytes {
				// One revision past a piece goes out in parts; the
				// watcher reads it from the history.
				n.fallBehind(w)
			}
		}
	}
}

// flushWatches sends every watcher's queued events, or makes it fall
// behind if its stream has no room.
func (n *Node) flushWatches() {
	for _, w := range n.watchers {
		if !w.dead && !w.behind && len(w.pending) > 0 {
			n.flushWatcher(w)
		}
	}
}

// flushWatcher sends a watcher's queued events in one response with the
// current revision, if its stream has room and sends no response in
// parts; else it falls behind.
func (n *Node) flushWatcher(w *watcher) {
	if !w.stream.hasRoom() {
		n.counters.FellBehind++
		n.fallBehind(w)
		return
	}
	if b := w.stream.big; b != nil && b != w {
		// The stream sends another watcher's response in parts, and what
		// comes after it would wait with no room counted: the watcher
		// reads its events from the history once the parts are out.
		n.fallBehind(w)
		return
	}
	r := &pb.WatchResponse{WatchId: w.id, Events: w.pending, Fragment: w.fragment && len(w.pending) > 1}
	n.watchReply(w.stream, r, w.waits, w.pendBytes)
	w.sent = true
	w.clearPending()
}

func (w *watcher) clearPending() {
	w.pending, w.waits = nil, nil
	w.pendBytes, w.pendRevs, w.pendLast = 0, 0, 0
}

// fallBehind drops a watcher's queued events: it reads them, and what
// follows, from the history once its stream has room.
func (n *Node) fallBehind(w *watcher) {
	if len(w.pending) > 0 {
		w.next = w.pendFrom
	} else {
		w.next = n.kv.HistoryEnd()
	}
	w.behind = true
	w.clearPending()
}

// FellBehind counts the times a watcher fell behind because its stream
// had no room, as Metrics does.
func (n *Node) FellBehind() int64 { return n.counters.FellBehind }

// CatchingUp reports whether a watcher that is behind has room to take
// its next piece, so the driver gives the node another turn at once.
func (n *Node) CatchingUp() bool {
	for _, w := range n.watchers {
		if !w.dead && w.behind && w.stream.hasRoom() {
			return true
		}
	}
	return false
}

// catchUp sends one piece of history to each watcher that is behind and
// whose stream has room: the events of up to catchUpRevs revisions and
// about PieceBytes, or a part of one revision larger than that. A
// watcher that reaches the end of the history takes the next writes as
// they apply.
func (n *Node) catchUp() {
	var gone []*watcher
	var broken []*watchStream
	for _, w := range n.watchers {
		if w.dead || !w.behind || !w.stream.hasRoom() {
			continue
		}
		if b := w.stream.big; b != nil && b != w {
			// The stream sends another watcher's response in parts.
			continue
		}
		if w.big != nil && n.passedBig(w) {
			broken = append(broken, w.stream)
			continue
		}
		if w.big == nil && n.passed(w) {
			gone = append(gone, w)
			continue
		}
		n.catchUpOne(w)
		if n.err != nil {
			return
		}
	}
	for _, w := range gone {
		// A compaction passed the revision the watcher was at, as in
		// etcd, where it is cancelled with the compaction revision.
		n.removeWatch(w)
		n.compacted(w, n.kv.CompactRev())
	}
	for _, s := range broken {
		if n.streams[s.id] == s {
			n.out.Watch = append(n.out.Watch, WatchResponse{Stream: s.id, Broken: true})
			n.CloseWatchStream(s.id)
		}
	}
}

// passed reports whether a compaction passed the position of a watcher
// that is behind: a revision it has yet to send is gone, or, with
// prev_kv, the compaction is at it, so the previous values are gone.
func (n *Node) passed(w *watcher) bool {
	if w.next < n.kv.HistoryStart() {
		return true
	}
	return w.prevKV && w.next < n.kv.HistoryEnd() && n.kv.Changed(w.next).ModRev() <= n.kv.CompactRev()
}

// passedBig reports whether a compaction reached the next change of the
// response a watcher sends in parts, which then cannot go on as its first
// part said: the change is gone, or a compaction at its revision may have
// removed it, or, with prev_kv, its previous value.
func (n *Node) passedBig(w *watcher) bool {
	b := w.big
	return b.pos < n.kv.HistoryStart() || n.kv.Changed(b.pos).ModRev() <= n.kv.CompactRev()
}

// catchUpOne sends a watcher its next response of history: up to
// catchUpRevs revisions, as etcd groups them. One whose events pass a
// piece goes in parts.
func (n *Node) catchUpOne(w *watcher) {
	if w.big != nil {
		n.bigPart(w)
		return
	}
	end := n.kv.HistoryEnd()
	stop, size, revs, last := w.next, 0, 0, int64(0)
	for scanned := 0; stop < end; stop++ {
		c := n.kv.Changed(stop)
		if r := c.ModRev(); r != last {
			if revs == catchUpRevs || scanned >= catchUpScan || size >= catchUpBytes {
				break
			}
			revs, last = revs+1, r
		}
		scanned++
		if w.wants(c) {
			size += c.EventSize(w.withPrev())
		}
	}
	if size > n.cfg.PieceBytes {
		w.big = &bigResponse{pos: w.next, end: stop, bytes: int64(size)}
		n.bigPart(w)
		return
	}
	var evs []*mvccpb.Event
	bytes := 0
	for pos := w.next; pos < stop; pos++ {
		c := n.kv.Changed(pos)
		if !w.wants(c) {
			continue
		}
		ev, err := w.event(c)
		if err != nil {
			n.fail(err)
			return
		}
		if ev != nil {
			evs = append(evs, ev)
			bytes += c.EventSize(w.withPrev())
		}
	}
	w.next = stop
	if len(evs) > 0 {
		var waits []*ValueRead
		if fills := n.kv.TakeFills(); len(fills) > 0 {
			waits = append(waits, n.readValues(fills))
		}
		n.watchReply(w.stream, &pb.WatchResponse{WatchId: w.id, Events: evs, Fragment: w.fragment && len(evs) > 1}, waits, bytes)
		w.sent = true
	}
	if w.next == end && n.revoke == nil {
		w.behind = false
	}
}

// bigPart sends the next part of the revision a watcher sends in parts:
// events of about PieceBytes.
func (n *Node) bigPart(w *watcher) {
	b := w.big
	var evs []*mvccpb.Event
	bytes := 0
	for ; b.pos < b.end && bytes < n.cfg.PieceBytes; b.pos++ {
		c := n.kv.Changed(b.pos)
		if !w.wants(c) {
			continue
		}
		ev, err := w.event(c)
		if err != nil {
			n.fail(err)
			return
		}
		if ev != nil {
			evs = append(evs, ev)
			bytes += c.EventSize(w.withPrev())
		}
	}
	var waits []*ValueRead
	if fills := n.kv.TakeFills(); len(fills) > 0 {
		waits = append(waits, n.readValues(fills))
	}
	more := b.pos < b.end
	part := WatchResponse{Stream: w.stream.id, Resp: &pb.WatchResponse{WatchId: w.id, Events: evs}, More: more, Cont: b.started, part: true}
	switch {
	case w.fragment:
		part.Resp.Fragment, part.Fragmented, part.More, part.Cont = more, true, false, false
	case !b.started:
		part.EventBytes = b.bytes
	}
	if !b.started && more {
		// The stream sends nothing else until the last part.
		w.stream.big = w
	}
	b.started = true
	n.watchPart(w.stream, part, waits, bytes, more)
	w.sent = true
	if b.pos == b.end {
		w.big, w.next = nil, b.end
		if w.next == n.kv.HistoryEnd() && n.revoke == nil {
			w.behind = false
		}
	}
}

// progressNotify sends a progress notification to every watcher that
// asked for them, unless it got events since the last notification, it
// starts above the current revision, or it is still catching up, as etcd
// does.
func (n *Node) progressNotify() {
	n.flushWatches()
	rev := n.kv.Rev()
	for _, w := range n.watchers {
		if !w.dead && w.progress && !w.sent && !w.ahead(rev) && !w.behind {
			r := &pb.WatchResponse{WatchId: w.id}
			n.watchReply(w.stream, r, nil, 0)
		}
		w.sent = false
	}
}
