package wl

import (
	"encoding/binary"
	"errors"
	"io"

	"golang.org/x/sys/unix"
)

// sock is one TLS connection the loop owns: its ciphertext and plaintext
// buffers, and its send buffer of records. Frames are written straight
// into the plaintext of the open record at the end of out; a record is
// sealed in place when it is full or at the end of the turn, and the
// whole buffer goes out in one write.
type sock struct {
	fd     int
	idx    int
	gen    uint16
	rd, wr aeadDir
	dead   bool

	in     []byte // ciphertext read and not yet opened
	ir, iw int
	pt     []byte // opened plaintext not yet taken
	pr, pw int

	out     []byte // sealed records, then the open record
	ow      int    // bytes of out written already
	rec     int    // offset of the open record's header, -1 for none
	wantOut bool   // registered for EPOLLOUT
	dirty   bool   // in the loop's list of sockets to flush
	// countSent adds what is written to the peer byte counter.
	countSent bool
}

const (
	inBuf  = 512 << 10
	ptBuf  = 1 << 20
	outBuf = 1 << 20
)

func newSock(h *handoff) *sock {
	s := &sock{fd: h.fd, rd: h.rd, wr: h.wr, rec: -1,
		in: make([]byte, inBuf), pt: make([]byte, ptBuf), out: make([]byte, 0, outBuf)}
	s.iw = copy(s.in, h.left)
	return s
}

var errClosed = errors.New("connection closed")

// readSock reads once from the socket into in. It reports whether it
// read anything.
func (s *sock) readSock() (bool, error) {
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
	n, err := unix.Read(s.fd, s.in[s.iw:])
	if n > 0 {
		s.iw += n
		return true, nil
	}
	if err == unix.EAGAIN || err == unix.EINTR {
		return false, nil
	}
	if err == nil {
		return false, io.EOF
	}
	return false, err
}

// open opens every whole record in in into pt.
func (s *sock) open() error {
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
				s.pw = copy(np, s.pt[s.pr:s.pw])
				s.pr = 0
				s.pt = np
			}
		}
		body := s.in[s.ir+recHdr : s.ir+recHdr+n]
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

// room is the plaintext bytes the open record still takes.
func (s *sock) room() int {
	if s.rec < 0 {
		return 0
	}
	return maxPlain - (len(s.out) - s.rec - recHdr)
}

func (s *sock) openRec() {
	if cap(s.out)-len(s.out) < maxPlain+recOverhead {
		n := make([]byte, len(s.out), 2*cap(s.out)+maxPlain+recOverhead)
		copy(n, s.out)
		s.out = n
	}
	s.rec = len(s.out)
	s.out = append(s.out, typeAppData, 3, 3, 0, 0)
}

// seal closes the open record and seals it in place.
func (s *sock) seal() {
	if s.rec < 0 {
		return
	}
	start := s.rec + recHdr
	end := len(s.out)
	if end == start {
		// An empty record: take it back.
		s.out = s.out[:s.rec]
		s.rec = -1
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
	s.seal()
	for s.ow < len(s.out) {
		n, err := unix.Write(s.fd, s.out[s.ow:])
		if n > 0 {
			s.ow += n
			if s.countSent {
				peerSentBytes.Add(uint64(n))
			}
			continue
		}
		if err == unix.EAGAIN {
			return false, nil
		}
		if err == unix.EINTR {
			continue
		}
		if err == nil {
			err = io.ErrShortWrite
		}
		return false, err
	}
	s.out = s.out[:0]
	s.ow = 0
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
	frGoAway       = 7
	frWindowUpdate = 8
	frContinuation = 9

	flEndStream  = 0x1
	flAck        = 0x1
	flEndHeaders = 0x4
	flPadded     = 0x8
	flPriority   = 0x20
)

func putFrameHeader(b []byte, n int, typ, flags byte, sid uint32) {
	b[0], b[1], b[2], b[3], b[4] = byte(n>>16), byte(n>>8), byte(n), typ, flags
	binary.BigEndian.PutUint32(b[5:], sid)
}

// writeFrame writes a whole frame with payload p.
func (s *sock) writeFrame(typ, flags byte, sid uint32, p []byte) {
	b := s.frame(9 + len(p))
	putFrameHeader(b, len(p), typ, flags, sid)
	copy(b[9:], p)
}

// msgW writes one gRPC message of a known length as DATA frames of a
// stream, straight into the records.
type msgW struct {
	s         *sock
	sid       uint32
	left      int // message bytes still to write, the 5-byte prefix included
	frameLeft int
}

// begin starts a gRPC message of n bytes on stream sid.
func (w *msgW) begin(s *sock, sid uint32, n int) {
	w.s, w.sid, w.left, w.frameLeft = s, sid, 5+n, 0
	var p [5]byte
	binary.BigEndian.PutUint32(p[1:], uint32(n))
	w.write(p[:])
}

func (w *msgW) newFrame() {
	s := w.s
	if s.room() < 9+1 {
		s.seal()
		s.openRec()
	}
	n := min(w.left, s.room()-9)
	b := s.frame(9)
	putFrameHeader(b, n, frData, 0, w.sid)
	w.frameLeft = n
}

func (w *msgW) write(p []byte) {
	for len(p) > 0 {
		if w.frameLeft == 0 {
			w.newFrame()
		}
		n := min(len(p), w.frameLeft)
		w.s.out = append(w.s.out, p[:n]...)
		w.frameLeft -= n
		w.left -= n
		p = p[n:]
	}
}

func (w *msgW) varint(v uint64) {
	var b [10]byte
	n := putUvarint(b[:], v)
	w.write(b[:n])
}

func (w *msgW) tag(num int, typ int) { w.varint(uint64(num)<<3 | uint64(typ)) }

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

func (w *msgW) end() {
	if w.left != 0 {
		panic("gRPC message length does not match what was written")
	}
}
