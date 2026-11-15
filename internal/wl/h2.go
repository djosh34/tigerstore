package wl

import (
	"bytes"
	"encoding/binary"
	"errors"

	"golang.org/x/net/http2/hpack"
)

// The client side: HTTP/2 frames read in place from the opened records,
// gRPC messages handed to the node as slices of them.

const (
	kUnknown uint8 = iota
	kRange
	kRangeStream
	kPut
	kDel
	kTxn
	kCompact
	kLeaseGrant
	kLeaseRevoke
	kLeaseKeepAlive
	kLeaseTTL
	kWatch
	kStatus
	kMemberList
	kMoveLeader
	kLeaseLeases
	kAlarm
	kMemberAdd
	kMemberRemove
	kMemberUpdate
	kMemberPromote
	// kMemberAttrs is a member's name and client URLs, which it publishes
	// in a batch; no client sends it.
	kMemberAttrs
	kinds // how many kinds there are
)

var paths = map[string]uint8{
	"/etcdserverpb.KV/Range":               kRange,
	"/etcdserverpb.KV/RangeStream":         kRangeStream,
	"/etcdserverpb.KV/Put":                 kPut,
	"/etcdserverpb.KV/DeleteRange":         kDel,
	"/etcdserverpb.KV/Txn":                 kTxn,
	"/etcdserverpb.KV/Compact":             kCompact,
	"/etcdserverpb.Lease/LeaseGrant":       kLeaseGrant,
	"/etcdserverpb.Lease/LeaseRevoke":      kLeaseRevoke,
	"/etcdserverpb.Lease/LeaseKeepAlive":   kLeaseKeepAlive,
	"/etcdserverpb.Lease/LeaseTimeToLive":  kLeaseTTL,
	"/etcdserverpb.Watch/Watch":            kWatch,
	"/etcdserverpb.Maintenance/Status":     kStatus,
	"/etcdserverpb.Cluster/MemberList":     kMemberList,
	"/etcdserverpb.Maintenance/MoveLeader": kMoveLeader,
	"/etcdserverpb.Lease/LeaseLeases":      kLeaseLeases,
	"/etcdserverpb.Maintenance/Alarm":      kAlarm,
	"/etcdserverpb.Cluster/MemberAdd":      kMemberAdd,
	"/etcdserverpb.Cluster/MemberRemove":   kMemberRemove,
	"/etcdserverpb.Cluster/MemberUpdate":   kMemberUpdate,
	"/etcdserverpb.Cluster/MemberPromote":  kMemberPromote,
}

// stream is one HTTP/2 stream of a client.
type stream struct {
	id    uint32
	kind  uint8
	open  bool
	ended bool   // the client ended its side
	buf   []byte // a gRPC message across DATA frames
	watch bool
	wids  []int32 // the watchers of a watch stream
	fail  []byte  // a Txn's failure branch, kept here and not replicated
	win   int64   // the stream's send window
	holds bool    // held messages wait for window: the stream's window is in its client's flow
	// streaming: the stream's response headers are out, and it answers
	// each request with a message until the client ends its side.
	streaming bool
	// failAt is fail's absolute position in the side ring, -1 if it is on
	// the heap.
	failAt int64
}

const streamSlots = 1 << 13

// client is one client connection.
type client struct {
	sock
	preface  bool
	slots    []stream
	over     map[uint32]*stream
	dec      *hpack.Decoder
	hdrCache map[string]uint8
	hdrPath  string
	inBlock  bool
	blockSid uint32
	blockEnd bool
	block    []byte
	unacked  int
	node     *node
	flow     flow
}

func newClient(h *handoff, stats *counters) *client {
	c := &client{sock: *newSock(h, stats), slots: make([]stream, streamSlots), hdrCache: map[string]uint8{}, flow: newFlow()}
	c.dec = hpack.NewDecoder(4096, func(f hpack.HeaderField) {
		if f.Name == ":path" {
			c.hdrPath = f.Value
		}
	})
	return c
}

func (c *client) get(id uint32) *stream {
	s := &c.slots[id>>1&(streamSlots-1)]
	if s.open && s.id == id {
		return s
	}
	if c.over != nil {
		return c.over[id]
	}
	return nil
}

func (c *client) newStream(id uint32, kind uint8) *stream {
	s := &c.slots[id>>1&(streamSlots-1)]
	if s.open {
		if c.over == nil {
			c.over = map[uint32]*stream{}
		}
		s = &stream{}
		c.over[id] = s
	}
	*s = stream{id: id, kind: kind, open: true, buf: s.buf[:0], wids: s.wids[:0], fail: nil, win: c.flow.initial}
	return s
}

func (c *client) endStream(s *stream) {
	s.fail = nil // the side ring may take its bytes again
	c.node.putBuf(s.buf)
	s.buf = nil
	if c.over != nil {
		if o, ok := c.over[s.id]; ok && o == s {
			delete(c.over, s.id)
			return
		}
	}
	s.open = false
}

var clientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

// serverStart is the server's SETTINGS (no limit on streams, large stream
// windows) and a large connection window.
func (c *client) start() {
	var p [12]byte
	binary.BigEndian.PutUint16(p[0:], 0x4) // INITIAL_WINDOW_SIZE
	binary.BigEndian.PutUint32(p[2:], 1<<30)
	binary.BigEndian.PutUint16(p[6:], 0x3) // MAX_CONCURRENT_STREAMS
	binary.BigEndian.PutUint32(p[8:], 1<<20)
	c.writeFrame(frSettings, 0, p[:])
	var w [4]byte
	binary.BigEndian.PutUint32(w[:], 1<<30)
	c.writeFrame(frWindowUpdate, 0, w[:])
}

var errProtocol = errors.New("HTTP/2 protocol error")

// frames reads the frames in pt and hands complete gRPC messages to n.
func (c *client) frames(n *node) error {
	if !c.preface {
		if c.pw-c.pr < len(clientPreface) {
			return nil
		}
		if !bytes.Equal(c.pt[c.pr:c.pr+len(clientPreface)], clientPreface) {
			return errProtocol
		}
		c.pr += len(clientPreface)
		c.preface = true
		c.start()
		n.markDirty(&c.sock)
	}
	for {
		avail := c.pw - c.pr
		if avail < 9 {
			return nil
		}
		h := c.pt[c.pr : c.pr+9]
		l := int(h[0])<<16 | int(h[1])<<8 | int(h[2])
		if avail < 9+l {
			if l > 1<<24 {
				return errProtocol
			}
			return nil
		}
		typ, flags := h[3], h[4]
		sid := binary.BigEndian.Uint32(h[5:]) & (1<<31 - 1)
		p := c.pt[c.pr+9 : c.pr+9+l]
		c.pr += 9 + l
		if c.inBlock && typ != frContinuation {
			return errProtocol
		}
		switch typ {
		case frData:
			c.unacked += l
			if c.unacked >= 1<<29 {
				var w [4]byte
				binary.BigEndian.PutUint32(w[:], uint32(c.unacked))
				c.writeFrame(frWindowUpdate, 0, w[:])
				c.unacked = 0
				n.markDirty(&c.sock)
			}
			if flags&flPadded != 0 {
				if len(p) == 0 || int(p[0]) >= len(p) {
					return errProtocol
				}
				p = p[1 : len(p)-int(p[0])]
			}
			s := c.get(sid)
			if s == nil {
				continue
			}
			if err := c.data(n, s, p); err != nil {
				return err
			}
			if flags&flEndStream != 0 {
				s.ended = true
				if s.watch {
					// The client closed its side of a watch stream.
					n.closeWatchStream(c, s)
					c.endStream(s)
				} else if s.streaming {
					n.endStreaming(c, s)
				}
			}
		case frHeaders:
			if flags&flPadded != 0 {
				if len(p) == 0 || int(p[0]) >= len(p) {
					return errProtocol
				}
				p = p[1 : len(p)-int(p[0])]
			}
			if flags&flPriority != 0 {
				if len(p) < 5 {
					return errProtocol
				}
				p = p[5:]
			}
			c.blockSid, c.blockEnd = sid, flags&flEndStream != 0
			if flags&flEndHeaders == 0 {
				c.inBlock = true
				c.block = append(c.block[:0], p...)
				continue
			}
			if err := c.headers(n, p, false); err != nil {
				return err
			}
		case frContinuation:
			if !c.inBlock || sid != c.blockSid {
				return errProtocol
			}
			c.block = append(c.block, p...)
			if flags&flEndHeaders != 0 {
				c.inBlock = false
				if err := c.headers(n, c.block, true); err != nil {
					return err
				}
			}
		case frRST:
			c.dropHeld(sid)
			if s := c.get(sid); s != nil {
				if s.watch {
					n.closeWatchStream(c, s)
				}
				c.endStream(s)
			}
		case frSettings:
			if flags&flAck == 0 {
				if err := c.settings(p); err != nil {
					return err
				}
				c.writeFrame(frSettings, flAck, nil)
				n.markDirty(&c.sock)
			}
		case frWindowUpdate:
			if len(p) != 4 {
				return errProtocol
			}
			c.windowUpdate(sid, int64(binary.BigEndian.Uint32(p)&(1<<31-1)))
		case frPing:
			if flags&flAck == 0 && len(p) == 8 {
				c.writeFrame(frPing, flAck, p)
				n.markDirty(&c.sock)
			}
		case frGoAway:
			return errClosed
		}
	}
}

// headers takes a whole header block that opens stream sid.
func (c *client) headers(n *node, block []byte, continued bool) error {
	sid := c.blockSid
	if s := c.get(sid); s != nil {
		// Trailers of the client: nothing to do but the end of its side.
		if c.blockEnd {
			s.ended = true
		}
		return nil
	}
	kind, ok := uint8(0), false
	if !continued {
		kind, ok = c.hdrCache[string(block)]
	}
	if !ok {
		c.hdrPath = ""
		if _, err := c.dec.Write(block); err != nil {
			return err
		}
		if err := c.dec.Close(); err != nil {
			return err
		}
		kind = paths[c.hdrPath]
		if keepsTable(block) {
			if len(c.hdrCache) < 256 {
				c.hdrCache[string(block)] = kind
			}
		} else {
			clear(c.hdrCache)
		}
	}
	s := c.newStream(sid, kind)
	if kind == kUnknown {
		n.replyError(c, sid, 12, "unknown method")
		c.endStream(s)
		return nil
	}
	if c.blockEnd {
		s.ended = true
	}
	return nil
}

// keepsTable reports whether an HPACK block leaves the dynamic table as
// it is: only indexed fields and literals without indexing.
func keepsTable(b []byte) bool {
	for len(b) > 0 {
		first := b[0]
		switch {
		case first&0x80 != 0:
			_, rest, ok := hpackInt(b, 7)
			if !ok {
				return false
			}
			b = rest
		case first&0xc0 == 0x40, first&0xe0 == 0x20:
			return false
		default:
			idx, rest, ok := hpackInt(b, 4)
			if !ok {
				return false
			}
			b = rest
			if idx == 0 {
				if b, ok = hpackStr(b); !ok {
					return false
				}
			}
			if b, ok = hpackStr(b); !ok {
				return false
			}
		}
	}
	return true
}

func hpackInt(b []byte, n uint) (uint64, []byte, bool) {
	if len(b) == 0 {
		return 0, nil, false
	}
	limit := uint64(1)<<n - 1
	v := uint64(b[0]) & limit
	b = b[1:]
	if v < limit {
		return v, b, true
	}
	for shift := uint(0); len(b) > 0 && shift < 63; shift += 7 {
		v += uint64(b[0]&0x7f) << shift
		last := b[0]&0x80 == 0
		b = b[1:]
		if last {
			return v, b, true
		}
	}
	return 0, nil, false
}

func hpackStr(b []byte) ([]byte, bool) {
	n, b, ok := hpackInt(b, 7)
	if !ok || n > uint64(len(b)) {
		return nil, false
	}
	return b[n:], true
}

// data takes the bytes of a DATA frame of stream s.
// A message that spans DATA frames waits in its stream's buffer. Streams
// that end give their buffers back to the node, which keeps a few of them
// for the next: a stream slot that kept its own grew to the largest
// message it ever had, in each of a client's 8,192 slots, and the k8s
// bench's nodes held hundreds of megabytes of them (#602).
const (
	keptBufs   = 64
	keptBufMax = 256 << 10
)

// takeBuf returns an empty buffer that a stream gave back, or nil.
func (n *node) takeBuf() []byte {
	k := len(n.bufs)
	if k == 0 {
		return nil
	}
	b := n.bufs[k-1]
	n.bufs[k-1] = nil
	n.bufs = n.bufs[:k-1]
	return b[:0]
}

// putBuf keeps b for another stream, unless the node keeps enough or b
// is large.
func (n *node) putBuf(b []byte) {
	if cap(b) > 0 && cap(b) <= keptBufMax && len(n.bufs) < keptBufs {
		n.bufs = append(n.bufs, b)
	}
}

func (c *client) data(n *node, s *stream, p []byte) error {
	if len(s.buf) > 0 {
		s.buf = append(s.buf, p...)
		p = s.buf
	}
	for len(p) >= 5 {
		l := int(binary.BigEndian.Uint32(p[1:5]))
		if len(p)-5 < l {
			break
		}
		if p[0] != 0 {
			return errors.New("compressed gRPC message")
		}
		msg := p[5 : 5+l]
		p = p[5+l:]
		if err := n.request(c, s, msg); err != nil {
			return err
		}
		if !s.open {
			p = nil
			break
		}
	}
	if len(p) > 0 {
		if len(s.buf) > 0 {
			s.buf = append(s.buf[:0], p...) // overlapping copy of the tail is fine
		} else {
			s.buf = append(n.takeBuf(), p...)
		}
	} else {
		s.buf = s.buf[:0]
	}
	return nil
}

// Header blocks the server sends: :status 200 and the gRPC content type;
// trailers with grpc-status 0.
var (
	respHeaders  = append([]byte{0x88, 0x0f, 0x10, 16}, "application/grpc"...)
	respTrailers = append(append([]byte{0x00, 11}, "grpc-status"...), 1, '0')
)

// handle packs a client stream into a number that names it in batches and
// queues: socket index, socket generation and stream id.
type handle uint64

// noHandle is an op's handle that no client waits on.
const noHandle = ^handle(0)

func mkHandle(c *client, sid uint32) handle {
	return handle(uint64(c.idx)<<48 | uint64(c.gen)<<32 | uint64(sid))
}

func (h handle) parts() (idx int, gen uint16, sid uint32) {
	return int(h >> 48), uint16(h >> 32), uint32(h)
}
