package wl

import "bytes"

// The reserve (#727, ruled: a shared reserve within the client pool).
// What waits to be sent to a client, in its send buffer and held for
// want of HTTP/2 window, stays within its share (sizes.connOut). A message
// that would take it past the share takes the room it passes it by from
// the reserve (sizes.reserve), shared by every client, before it is
// built, and the room goes back as the client's bytes leave for its
// socket (giveRoom). A message within its share takes no new step.
//
// When the reserve is spent:
//   - a range's reply waits unbuilt, its request alone kept, until room
//     comes back, as a client waits for the read half (roomWait);
//   - a live watch event waits as a slow watcher's do, and its watch
//     catches up later from the change index;
//   - a watch's catch-up waits for its next turn;
//   - a write's reply, which the apply cannot hold back, is refused with
//     etcd's "too many requests", the write applied, as #721's second
//     ruling refuses a reply past maxMsg.
//
// One message is at most maxMsg, which the reserve holds, so none can
// pass a share by more than the whole reserve. Messages of up to
// smallMsg, headers, trailers and a few fields, take no room: each stream
// of the client has at most one of them under way.

// smallMsg is the largest message that takes no room of the reserve.
const smallMsg = 1 << 10

// msgRoom reports whether a message of m bytes may be built for client c
// now: within its share, or with the room the reserve gives it for what
// passes it, which it takes.
func (n *node) msgRoom(c *client, m int64) bool {
	past := c.waiting() + c.flow.bytes + m - n.connOut - c.roomHeld
	if past <= 0 || m <= smallMsg {
		return true
	}
	if n.reserved+past > n.sizes.reserve() {
		n.stats.roomWaits++
		n.roomShort = true
		return false
	}
	c.roomHeld += past
	n.reserved += past
	return true
}

// giveRoom gives back the room of the reserve that client c holds past
// what it holds over its share now, all of it once c is closed.
func (n *node) giveRoom(c *client, closed bool) {
	if c.roomHeld == 0 {
		return
	}
	keep := min(c.roomHeld, max(0, c.waiting()+c.flow.bytes-n.connOut))
	if closed {
		keep = 0
	}
	if keep < c.roomHeld {
		n.roomFreed = n.roomFreed || len(n.roomWait) > 0
		n.roomShort = false
	}
	n.reserved -= c.roomHeld - keep
	c.roomHeld = keep
}

// appliedRoom reports whether the reply of an applied write, of size
// bytes, may be built for stream s of client c. One that the reserve has
// no room for cannot wait, as the apply goes on: it is refused with
// etcd's "too many requests", the write applied.
func (n *node) appliedRoom(c *client, s *stream, size int) bool {
	if n.msgRoom(c, int64(5+size)) {
		return true
	}
	n.replyError(c, s.id, codeResourceExhausted, "etcdserver: too many requests")
	return false
}

// waitRoom keeps range request msg of handle h until the reserve has
// room for its reply.
func (n *node) waitRoom(h handle, msg []byte) {
	n.roomWait = append(n.roomWait, awayRead{h: h, msg: bytes.Clone(msg)})
}

// serveRoomWait serves again the ranges that waited for room, once some
// came back; one that finds none waits again.
func (n *node) serveRoomWait() {
	if !n.roomFreed {
		return
	}
	n.roomFreed = false
	reads := n.roomWait
	n.roomWait = nil
	for _, r := range reads {
		c, s := n.clientOf(r.h)
		if c == nil {
			continue
		}
		var req rangeReq
		if err := parseRange(r.msg, &req); err != nil {
			continue
		}
		n.serveRange(c, s, &req, r.msg)
	}
}
