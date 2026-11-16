package core

import (
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

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

// catchUpRevs caps the revisions in one response, like etcd's
// watchBatchMaxRevs. A revision is never split across responses, and each
// response has the current revision as its header, as in etcd.
const catchUpRevs = 1000

// catchUpScan caps the changes a catch-up response reads from the history
// for one watcher, whether they match it or not, and catchUpBytes its
// events, half of gRPC's largest message: past either, the next revision
// goes in the next response.
const ()

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

	fragment bool
	// start is the first revision the watcher delivers. Changes are
	// matched after it is created, so later revisions are always newer;
	// start only holds back a watcher that asked for a future revision.
	start int64

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

	dead bool
}

type watchStream struct {
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
	n.watchPart(s, WatchResponse{Resp: r}, waits, events, false)
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
