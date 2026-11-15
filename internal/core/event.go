package core

import (
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/mvcc"
)

// Event is a watch event, encoded once as an mvccpb.Event message and
// shared by every response that carries it (part 4 of docs/wire-loop.md).
// The store writes it from its own bytes (mvcc.Change.AppendEvent). An
// event that needs a value only the log has is built as a message whose
// value a helper reads, and is encoded once the read is in, before its
// response leaves the node (seal).
type Event struct {
	b   []byte
	msg *mvccpb.Event
	del bool
}

// Bytes is the event as an mvccpb.Event message, without a tag or length.
// Nobody may change it.
func (e *Event) Bytes() []byte { return e.b }

// Deletes reports whether the event deletes its key.
func (e *Event) Deletes() bool { return e.del }

// seal encodes an event built as a message, whose values are read now.
func (e *Event) seal() error {
	if e.msg == nil {
		return nil
	}
	b, err := proto.Marshal(e.msg)
	if err != nil {
		return fmt.Errorf("core: encode a watch event: %w", err)
	}
	e.b, e.msg = b, nil
	return nil
}

// eventChunk is the room the events of a turn share, so that most take
// no allocation of their own.
const eventChunk = 64 << 10

// event builds the event of change c, with its previous version if
// withPrev is set. One whose values are all in memory is encoded at once,
// into room the events of a turn share; one that needs a value from the
// log is a message whose values come as fills (mvcc.Store.TakeFills).
func (n *Node) event(c mvcc.Change, withPrev bool) (*Event, error) {
	size := c.EventSize(withPrev)
	if cap(n.events)-len(n.events) < size {
		n.events = make([]byte, 0, max(eventChunk, size))
	}
	start := len(n.events)
	b, ok, err := c.AppendEvent(n.events, withPrev)
	if err != nil {
		return nil, err
	}
	if ok {
		n.events = b
		return &Event{b: b[start:len(b):len(b)], del: c.Deletes()}, nil
	}
	ev, err := c.Event(withPrev)
	if err != nil {
		return nil, err
	}
	return &Event{msg: ev, del: c.Deletes()}, nil
}

// Message returns the response as etcd's message, with its events
// decoded: for clients of the node that take messages, not bytes. A
// Broken response has none.
func (r WatchResponse) Message() (*pb.WatchResponse, error) {
	if r.Resp == nil {
		return nil, nil
	}
	m := &pb.WatchResponse{
		Header: r.Resp.Header, WatchId: r.Resp.WatchId, Created: r.Resp.Created, Canceled: r.Resp.Canceled,
		CompactRevision: r.Resp.CompactRevision, CancelReason: r.Resp.CancelReason, Fragment: r.Resp.Fragment,
	}
	if len(r.Events) > 0 {
		m.Events = make([]*mvccpb.Event, len(r.Events))
	}
	for i, e := range r.Events {
		ev := &mvccpb.Event{}
		if err := proto.Unmarshal(e.b, ev); err != nil {
			return nil, fmt.Errorf("core: decode a watch event: %w", err)
		}
		m.Events[i] = ev
	}
	return m, nil
}

// Size is the size of the response as one WatchResponse message, or of
// its part.
func (r WatchResponse) Size() int {
	if r.Resp == nil {
		return 0
	}
	return proto.Size(r.Resp) + r.EventsSize()
}

// EventsSize is the size of the response's events as fields of a
// WatchResponse.
func (r WatchResponse) EventsSize() int {
	n := 0
	for _, e := range r.Events {
		n += eventField(len(e.b))
	}
	return n
}

// eventField is the size of an event of n bytes as a field of a
// WatchResponse: its tag, its length and the event.
func eventField(n int) int { return 1 + protowire.SizeBytes(n) }
