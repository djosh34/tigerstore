package wl

import (
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// sock is one TLS connection the loop owns: its ciphertext and plaintext
// buffers, and its send buffer of records. Frames are written straight
// into the plaintext of the open record at the end of out; a record is
// sealed in place when it is full or at the end of the turn, and the
// whole buffer goes out in one write. A plain socket, a client of a
// listener without TLS (h2c), has no records: in is the bytes as they
// came, and out the bytes to send.
type sock struct {
	fd     int
	idx    int
	gen    uint16
	rd, wr aeadDir
	plain  bool // no TLS: the bytes as they come, no records
	dead   bool

	in     []byte // ciphertext read and not yet opened
	ir, iw int
	pt     []byte // opened plaintext not yet taken
	pr, pw int

	out             []byte // sealed records, then the open record
	ow              int    // bytes of out written already
	rec             int    // offset of the open record's header, -1 for none
	wantOut         bool   // registered for EPOLLOUT
	noIn            bool   // registered without EPOLLIN: a paused client
	controlCapacity int64  // unchanged inherited watch-control socket growth, #912
	controlOwner    *client
	outHold         *int // retain backing while protected output owns it
	dropOut         bool // release an empty protected socket without a shrink-copy
	outBase         int  // the send buffer's size as it started
	dirty           bool // in the loop's list of sockets to flush
	stats           *counters
	// sent, if any, counts the bytes written: a peer connection's.
	sent *atomic.Uint64
	// broken is set when a reply came out another length than it said:
	// the stream is corrupt, and the connection is closed at its flush.
	broken bool
	// pool and outPool, a client's, count the bytes it holds in its
	// node's client buffers (sizes.clientBufs): what it reads in pool, and
	// what waits to be sent in outPool; nil for a peer's. loose counts
	// what pool took with no check of its room (growLoose).
	pool, outPool, loose       *int64
	creditWake                 *bool // loop-owned physical client-credit progress
	promiseLocal, promiseTotal *int64
}

// A socket's buffers: the ciphertext read, the plaintext opened and the
// send buffer. A peer's are made with it. A client's are taken from the
// client buffers only while it reads or writes, at the client sizes, and
// given back once it is idle (client.takeBufs, client.idle).
const (
	inBuf        = 512 << 10
	ptBuf        = 256 << 10
	outBuf       = 1 << 20
	clientInBuf  = 64 << 10
	clientPtBuf  = 64 << 10
	clientOutBuf = 64 << 10
)

func newSock(h *handoff, stats *counters) *sock {
	s := &sock{
		fd: h.fd, rd: h.rd, wr: h.wr, plain: h.plain, rec: -1, stats: stats,
		in: make([]byte, max(inBuf, len(h.left))), pt: make([]byte, ptBuf), out: make([]byte, 0, outBuf), outBase: outBuf,
	}
	s.iw = copy(s.in, h.left)
	return s
}

// grow counts n more bytes of what the socket reads in its pool, and
// growOut of what waits to be sent.
func (s *sock) grow(n int) {
	if n < 0 && s.creditWake != nil {
		*s.creditWake = true
	}
	if s.pool != nil {
		*s.pool += int64(n)
	}
}

// growLoose grows what the socket counts in its pool by n with no check
// of the pool's room, and counts it in loose too: a turn of the read half
// (partwait.go) keeps its room past it.
func (s *sock) growLoose(n int) {
	s.grow(n)
	if s.loose != nil && n > 0 {
		*s.loose += int64(n)
	}
}

func (s *sock) growOut(n int) {
	if n > 0 {
		s.takeOutputPromise(int64(n))
	}
	if n < 0 && s.creditWake != nil {
		*s.creditWake = true
	}
	if s.outPool != nil {
		*s.outPool += int64(n)
	}
}

func (s *sock) takeOutputPromise(bytes int64) {
	if s.promiseLocal != nil {
		take := min(bytes, *s.promiseLocal)
		*s.promiseLocal -= take
		*s.promiseTotal -= take
	}
}

var errClosed = errors.New("connection closed")

// readSock reads once from the socket into in. It reports whether it
// read anything.
func (s *sock) readSock() (bool, error) {
	got, err := s.readSockOnce()
	if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
		return false, nil
	}
	return got, err
}

// readSockBlocking reads from a socket with a receive timeout. EINTR
// repeats that blocking read; only EAGAIN means the timeout expired.
// The loop's non-blocking readSock still yields after an interrupt.
func (s *sock) readSockBlocking() (bool, error) {
	for {
		got, err := s.readSockOnce()
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return got, err
	}
}

// readSockOnce leaves interruptions and timeouts to its caller, which
// knows whether the socket blocks or belongs to the loop.
func (s *sock) readSockOnce() (bool, error) {
	if s.ir == s.iw {
		s.ir, s.iw = 0, 0
	} else if len(s.in)-s.iw < len(s.in)/4 {
		s.iw = copy(s.in, s.in[s.ir:s.iw])
		s.ir = 0
	}
	if s.iw == len(s.in) {
		// A record bigger than the buffer cannot happen; the buffer is
		// full of whole records not yet opened.
		return false, nil
	}
	s.stats.reads++
	n, err := sysRead(s.fd, s.in[s.iw:])
	if n > 0 {
		s.iw += n
		return true, nil
	}
	if err == nil {
		return false, io.EOF
	}
	return false, err
}

// open opens every whole record in in into pt; a plain socket's bytes
// move as they are.
func (s *sock) open() error {
	if s.plain {
		return s.takePlain()
	}
	for {
		avail := s.iw - s.ir
		if avail < recHdr {
			return nil
		}
		hdr := s.in[s.ir : s.ir+recHdr]
		n := int(binary.BigEndian.Uint16(hdr[3:]))
		if n > maxCipher || n < tagLen+1 {
			return errRecord
		}
		if avail < recHdr+n {
			return nil
		}
		if hdr[0] != typeAppData {
			if hdr[0] == typeAlert {
				return errClosed
			}
			return errRecord
		}
		if len(s.pt)-s.pw < n {
			if s.pr > 0 {
				s.pw = copy(s.pt, s.pt[s.pr:s.pw])
				s.pr = 0
			}
			if len(s.pt)-s.pw < n {
				np := make([]byte, 2*len(s.pt))
				s.growLoose(len(np) - len(s.pt))
				s.pw = copy(np, s.pt[s.pr:s.pw])
				s.pr = 0
				s.pt = np
			}
		}
		body := s.in[s.ir+recHdr : s.ir+recHdr+n]
		if s.rd.tls12 {
			plain, err := s.rd.open12(s.pt[s.pw:s.pw], body, hdr)
			if err != nil {
				return err
			}
			s.ir += recHdr + n
			s.pw += len(plain)
			continue
		}
		plain, err := s.rd.aead.Open(s.pt[s.pw:s.pw], s.rd.next(), body, hdr)
		if err != nil {
			return err
		}
		s.ir += recHdr + n
		i := len(plain) - 1
		for i >= 0 && plain[i] == 0 {
			i--
		}
		if i < 0 {
			return errRecord
		}
		switch plain[i] {
		case typeAppData:
			s.pw += i
		case typeHandshake:
			// Session tickets are dropped; nothing else is expected.
			if err := postHandshake(plain[:i]); err != nil {
				return err
			}
		case typeAlert:
			return errClosed
		default:
			return errRecord
		}
	}
}

func postHandshake(m []byte) error {
	for len(m) > 0 {
		if len(m) < 4 {
			return errRecord
		}
		n := 4 + (int(m[1])<<16 | int(m[2])<<8 | int(m[3]))
		if n > len(m) {
			return errRecord
		}
		if m[0] != 4 { // NewSessionTicket
			return errors.New("TLS handshake message after the handshake, such as a key update")
		}
		m = m[n:]
	}
	return nil
}

// takePlain moves what a plain socket read into pt.
func (s *sock) takePlain() error {
	n := s.iw - s.ir
	if len(s.pt)-s.pw < n {
		if s.pr > 0 {
			s.pw = copy(s.pt, s.pt[s.pr:s.pw])
			s.pr = 0
		}
		if len(s.pt)-s.pw < n {
			np := make([]byte, max(2*len(s.pt), s.pw+n))
			s.growLoose(len(np) - len(s.pt))
			s.pw = copy(np, s.pt[s.pr:s.pw])
			s.pr = 0
			s.pt = np
		}
	}
	s.pw += copy(s.pt[s.pw:], s.in[s.ir:s.iw])
	s.ir = s.iw
	return nil
}

// room is the plaintext bytes the open record still takes.
func (s *sock) room() int {
	if s.rec < 0 {
		return 0
	}
	if s.plain {
		return maxPlain - (len(s.out) - s.rec)
	}
	return maxPlain - (len(s.out) - s.rec - recHdr - s.wr.extra())
}

// openRec opens a record at the end of out. When out has no room for it,
// the bytes already written go first, if they are at least half of out:
// out is emptied only once a flush writes everything, and a peer that
// never quite catches up would otherwise keep every byte ever sent to it
// (#696). Else out doubles.
func (s *sock) openRec() {
	const need = maxPlain + recOverhead + 8
	if cap(s.out)-len(s.out) < need && s.ow > 0 && 2*s.ow >= len(s.out) {
		s.out = s.out[:copy(s.out, s.out[s.ow:])]
		s.ow = 0
	}
	if cap(s.out)-len(s.out) < need {
		n := make([]byte, len(s.out), 2*cap(s.out)+need)
		s.growOut(cap(n) - cap(s.out))
		copy(n, s.out)
		s.out = n
	}
	s.rec = len(s.out)
	if s.plain {
		return
	}
	s.out = append(s.out, typeAppData, 3, 3, 0, 0)
	if s.wr.explicit {
		s.out = s.out[:len(s.out)+8] // TLS 1.2 AES-GCM's explicit nonce, written at the seal
	}
}

// growMsg is the least message a send buffer grows for at once
// (growFor). A smaller one appends, and the buffer doubles: grown to fit
// each message of a few KiB exactly, a buffer was allocated and copied
// again for the next.
const growMsg = 64 << 10

// growFor makes room in out for a message of m bytes as DATA frames in
// records, with one allocation of what it takes rather than doublings
// that may leave twice its size: what waits to be sent counts in bytes
// against a client's share and the reserve (reserve.go).
func (s *sock) growFor(m int) {
	need := m + (m/maxFrame+2)*9 + (m/maxPlain+2)*(recOverhead+8) + maxPlain + recOverhead + 8
	if cap(s.out)-len(s.out) >= need {
		return
	}
	if s.ow > 0 {
		// The bytes written already go first, as openRec drops them: a
		// client that never quite drains kept every byte sent to it, and
		// each large message grew out by all of them (a send buffer of
		// 34 MiB with 580 KiB unsent, TestALargeRangeAndCatchUpStay-
		// WithinTheBudget under -race). An open record lies past them.
		if s.rec >= 0 {
			s.rec -= s.ow
		}
		s.out = s.out[:copy(s.out, s.out[s.ow:])]
		s.ow = 0
		if cap(s.out)-len(s.out) >= need {
			return
		}
	}
	n := make([]byte, len(s.out), len(s.out)+need)
	s.growOut(cap(n) - cap(s.out))
	copy(n, s.out)
	s.out = n
}

// seal closes the open record and seals it in place.
func (s *sock) seal() {
	if s.rec < 0 {
		return
	}
	if s.plain {
		s.rec = -1
		return
	}
	start := s.rec + recHdr
	end := len(s.out)
	if end == start+s.wr.extra() {
		// An empty record: take it back.
		s.out = s.out[:s.rec]
		s.rec = -1
		return
	}
	if s.wr.tls12 {
		s.seal12(start, end)
		return
	}
	s.out = append(s.out, typeAppData)
	l := end - start + 1 + tagLen
	hdr := s.out[s.rec:start]
	hdr[3], hdr[4] = byte(l>>8), byte(l)
	sealed := s.wr.aead.Seal(s.out[start:start], s.wr.next(), s.out[start:end+1], hdr)
	s.out = s.out[:start+len(sealed)]
	s.rec = -1
}

// seal12 seals the open record of a TLS 1.2 connection, whose
// plaintext is out[start+extra:end].
func (s *sock) seal12(start, end int) {
	pt := start + s.wr.extra()
	seq := s.wr.seq
	nonce := s.wr.next()
	if s.wr.explicit {
		copy(s.out[start:pt], nonce[4:])
	}
	hdr := s.out[s.rec:start]
	sealed := s.wr.aead.Seal(s.out[pt:pt], nonce, s.out[pt:end], s.wr.aad12(seq, hdr, end-pt))
	l := pt - start + len(sealed)
	hdr[3], hdr[4] = byte(l>>8), byte(l)
	s.out = s.out[:pt+len(sealed)]
	s.rec = -1
}

// frame returns n bytes at the end of the open record, opening a new one
// if this one has no room: the caller writes a whole frame there.
func (s *sock) frame(n int) []byte {
	if s.room() < n {
		s.seal()
		s.openRec()
	}
	l := len(s.out)
	s.out = s.out[:l+n]
	return s.out[l : l+n]
}

// flush seals the open record and writes what is not written yet. It
// reports whether everything went out.
func (s *sock) flush() (bool, error) {
	if s.broken {
		return false, errMsgSize
	}
	s.seal()
	for s.ow < len(s.out) {
		s.stats.writes++
		n, err := sysWrite(s.fd, s.out[s.ow:])
		if n > 0 {
			s.ow += n
			if s.sent != nil {
				s.sent.Add(uint64(n))
			}
			continue
		}
		if errors.Is(err, unix.EAGAIN) {
			return false, nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err == nil {
			err = io.ErrShortWrite
		}
		return false, err
	}
	s.out = s.out[:0]
	s.ow = 0
	if s.retireProtectedControlSocket() || s.outHold != nil && *s.outHold > 0 {
		return true, nil
	}
	if s.dropOut {
		s.shrinkControlCapacity(0)
		s.growOut(-cap(s.out))
		s.out = nil
		s.dropOut = false
		return true, nil
	}
	if cap(s.out) > 2*s.outBase {
		// A large reply grew it; it need not keep that much. Kept to
		// four times its base, what a client's share leaves past its
		// bytes waiting went past the client buffers' send half and the
		// reserve: 9.96 MB of 8.59 MB with 33 clients at 96 MiB (#689's
		// O re-check). Twice its base is what a record opens in it.
		s.shrinkControlCapacity(s.outBase)
		s.growOut(s.outBase - cap(s.out))
		s.out = make([]byte, 0, s.outBase)
	}
	return true, nil
}

// HTTP/2 frame types and flags (RFC 9113).
const (
	frData         = 0
	frHeaders      = 1
	frPriority     = 2
	frRST          = 3
	frSettings     = 4
	frPing         = 6
	frPushPromise  = 5
	frGoAway       = 7
	frWindowUpdate = 8
	frContinuation = 9

	flEndStream  = 0x1
	flAck        = 0x1
	flEndHeaders = 0x4
	flPadded     = 0x8
	flPriority   = 0x20

	// maxFrame is HTTP/2's initial SETTINGS_MAX_FRAME_SIZE.
	maxFrame = 1 << 14
)

func putFrameHeader(b []byte, n int, typ, flags byte, sid uint32) {
	b[0], b[1], b[2], b[3], b[4] = byte(n>>16), byte(n>>8), byte(n), typ, flags
	binary.BigEndian.PutUint32(b[5:], sid)
}

// writeFrame writes a whole frame of the connection, stream 0, with
// payload p.
func (s *sock) writeFrame(typ, flags byte, p []byte) {
	b := s.frame(9 + len(p))
	putFrameHeader(b, len(p), typ, flags, 0)
	copy(b[9:], p)
}

// msgW writes one gRPC message of a known length as DATA frames of a
// stream, straight into the records.
type msgW struct {
	control       bool // inherited watch-control output only (#912)
	controlClient *client
	controlCap    int
	stage         *replyStage
	s             *sock
	sid           uint32
	left          int // message bytes still to write, the 5-byte prefix included
	frameLeft     int
	// A message the windows do not take is held: its client and stream,
	// and its bytes.
	c    *client
	st   *stream
	held []byte
}

// begin starts a gRPC message of n bytes on stream s of client c.
func (w *msgW) begin(c *client, s *stream, n int) {
	*w = msgW{control: w.control, s: &c.sock, sid: s.id, left: 5 + n}
	if w.control {
		w.controlClient, w.controlCap = c, cap(c.out)
	}
	if g := c.node; g != nil {
		g.gs.calls[s.kind].sent++
		g.gs.sentBytes += int64(n)
	}
	switch {
	case c.replyStage != nil:
		w.stage = c.replyStage
	case !c.take(s, 5+n):
		w.c, w.st, w.held = c, s, make([]byte, 0, 5+n)
	case n >= growMsg:
		c.growFor(5 + n)
	}
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(n))
	w.write(p[:])
}

// newFrame opens the next DATA frame of the message. It reports false,
// and marks the connection broken, when the message has no bytes left
// for it: a writer wrote more than the length it began with.
func (w *msgW) newFrame() bool {
	s := w.s
	if w.left <= 0 {
		s.broken = true
		return false
	}
	if s.room() < 9+1 {
		s.seal()
		s.openRec()
	}
	n := min(w.left, s.room()-9)
	b := s.frame(9)
	putFrameHeader(b, n, frData, 0, w.sid)
	w.frameLeft = n
	return true
}

func (w *msgW) write(p []byte) {
	if w.stage != nil {
		w.stage.append(p)
		w.left -= len(p)
		return
	}
	if w.c != nil {
		w.held = append(w.held, p...)
		w.left -= len(p)
		return
	}
	for len(p) > 0 {
		if w.frameLeft == 0 && !w.newFrame() {
			return
		}
		n := min(len(p), w.frameLeft)
		w.s.out = append(w.s.out, p[:n]...)
		w.frameLeft -= n
		w.left -= n
		p = p[n:]
	}
}

func (w *msgW) varintField(num int, v uint64) {
	var b [20]byte
	n := putUvarint(b[:], uint64(num)<<3)
	n += putUvarint(b[n:], v)
	w.write(b[:n])
}

func (w *msgW) bytesField(num int, p []byte) {
	var b [20]byte
	n := putUvarint(b[:], uint64(num)<<3|2)
	n += putUvarint(b[n:], uint64(len(p)))
	w.write(b[:n])
	w.write(p)
}

// lenField writes the tag and length of a nested message of n bytes.
func (w *msgW) lenField(num int, n int) {
	var b [20]byte
	k := putUvarint(b[:], uint64(num)<<3|2)
	k += putUvarint(b[k:], uint64(n))
	w.write(b[:k])
}

var errMsgSize = errors.New("a gRPC message's length does not match what was written")

func (w *msgW) end() {
	if w.left != 0 {
		w.s.broken = true
	}
	if w.c != nil {
		w.c.holdOutput(w.st, w.held, nil, w.control)
	} else if w.controlClient != nil {
		w.controlClient.controlSocketGrew(w.controlCap)
	}
}
