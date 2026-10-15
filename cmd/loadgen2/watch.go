package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

type wstream struct {
	id        uint32
	res       int
	prefix    []byte
	created   bool
	failed    bool
	last      int64
	buf       []byte
	unacked   int64
	gotHdr    bool
	log       []rec
	events    int64
	winEvents int64
}

// watcher owns one connection carrying several watch streams.
type watcher struct {
	c       *h2
	streams []*wstream
	tnow    int64
	t1, t2  int64
	errs    map[string]int
	s1, s2  []byte
	final   chan []int64 // expected events per resource, sent once at the end
	fatal   error
}

func newWatcher(c *h2) *watcher {
	return &watcher{c: c, errs: map[string]int{}, final: make(chan []int64, 1)}
}

func (w *watcher) fail(s string) { w.errs[s]++ }

// add opens a prefix watch with prev_kv and progress notifications from
// revision start.
func (w *watcher) add(res int, start int64) {
	id := uint32(2*len(w.streams) + 1)
	prefix := resPrefix[res]
	end := bytes.Clone(prefix)
	end[len(end)-1]++
	var cr []byte
	cr = appendBytesField(cr, 1<<3|2, prefix)
	cr = appendBytesField(cr, 2<<3|2, end)
	cr = append(cr, 3<<3)
	cr = protowire.AppendVarint(cr, uint64(start))
	cr = append(cr, 4<<3, 1, 6<<3, 1)
	msg := appendBytesField(nil, 1<<3|2, cr)
	blk := reqHeaders(w.c.tc.RemoteAddr().String(), "/etcdserverpb.Watch/Watch")
	out := appendFH(w.c.out, len(blk), fHeaders, flEndHdr, id)
	out = append(out, blk...)
	out = appendFH(out, 5+len(msg), fData, 0, id)
	out = append(out, 0)
	out = binary.BigEndian.AppendUint32(out, uint32(len(msg)))
	w.c.out = append(out, msg...)
	w.streams = append(w.streams, &wstream{id: id, res: res, prefix: prefix, last: start - 1})
}

func (w *watcher) allCreated() bool {
	for _, s := range w.streams {
		if !s.created {
			return false
		}
	}
	return true
}

func (w *watcher) waitCreated() error {
	if err := w.c.flush(); err != nil {
		return err
	}
	w.c.tc.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer w.c.tc.SetReadDeadline(time.Time{})
	for {
		if err := w.frames(); err != nil {
			return err
		}
		if w.allCreated() {
			return w.c.flush()
		}
		if err := w.c.flush(); err != nil {
			return err
		}
		if err := w.c.fill(); err != nil {
			return err
		}
	}
}

func (w *watcher) frames() error {
	c := w.c
	for {
		typ, flags, sid, p, ok, err := c.frame()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		handled, err := c.control(typ, flags, sid, p)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		if sid == 0 || int(sid/2) >= len(w.streams) {
			return fmt.Errorf("watch frame on unknown stream %d", sid)
		}
		s := w.streams[sid/2]
		switch typ {
		case fData:
			p, err = c.dataPayload(flags, p)
			if err != nil {
				return err
			}
			s.unacked += int64(len(p))
			if s.unacked >= recvStep {
				c.out = appendWinUpd(c.out, s.id, uint32(s.unacked))
				s.unacked = 0
			}
			w.data(s, p)
			if flags&flEnd != 0 {
				w.fail("watch stream ended")
			}
		case fHeaders:
			blk, err := headerBlock(flags, p)
			if err != nil {
				return err
			}
			if flags&flEnd != 0 {
				c.checkBlock(blk, true)
				w.fail("watch stream ended")
			} else {
				if err := c.checkBlock(blk, false); err != nil {
					w.fail("watch headers: " + err.Error())
				}
				s.gotHdr = true
			}
		case fRST:
			w.fail("watch stream reset")
		}
	}
}

func (w *watcher) data(s *wstream, p []byte) {
	if len(s.buf) == 0 && len(p) >= 5 && p[0] == 0 && 5+int(binary.BigEndian.Uint32(p[1:5])) == len(p) {
		w.message(s, p[5:])
		return
	}
	s.buf = append(s.buf, p...)
	b := s.buf
	for len(b) >= 5 {
		if b[0] != 0 {
			w.fail("compressed watch message")
			b = nil
			break
		}
		n := 5 + int(binary.BigEndian.Uint32(b[1:5]))
		if len(b) < n {
			break
		}
		w.message(s, b[5:n])
		b = b[n:]
	}
	s.buf = append(s.buf[:0], b...)
}

func (w *watcher) message(s *wstream, m []byte) {
	if s.failed {
		return
	}
	if err := w.response(s, m); err != nil {
		s.failed = true
		w.fail(fmt.Sprintf("watch %s: %v", s.prefix, err))
	}
}

func (w *watcher) response(s *wstream, m []byte) error {
	var hdr int64
	before := s.last
	inWin := w.tnow >= w.t1 && w.tnow < w.t2
	for len(m) > 0 {
		num, _, v, b, n := field(m)
		if n < 0 {
			return errProto
		}
		switch num {
		case wrHeader:
			r, err := headerRev(b)
			if err != nil {
				return err
			}
			hdr = r
		case wrCreated:
			if v == 1 {
				s.created = true
			}
		case wrCanceled, wrCompact:
			if v != 0 {
				return errors.New("canceled or compacted")
			}
		case wrEvents:
			if !s.created {
				return errors.New("event before created")
			}
			if err := w.event(s, b); err != nil {
				return err
			}
			if inWin {
				s.winEvents++
			}
		}
		m = m[n:]
	}
	if hdr == 0 {
		return errors.New("no header")
	}
	if s.last != before && s.last > hdr {
		return errors.New("event above header revision")
	}
	return nil
}

func (w *watcher) event(s *wstream, e []byte) error {
	var typ uint64
	var kv, prev []byte
	hasKV, hasPrev := false, false
	for len(e) > 0 {
		num, _, v, b, n := field(e)
		if n < 0 {
			return errProto
		}
		switch num {
		case evType:
			typ = v
		case evKV:
			kv, hasKV = b, true
		case evPrevKV:
			prev, hasPrev = b, true
		}
		e = e[n:]
	}
	if !hasKV {
		return errors.New("event without kv")
	}
	ckv, key, mod, s1, err := canonKV(kv, w.s1)
	w.s1 = s1
	if err != nil {
		return err
	}
	var cprev []byte
	if hasPrev {
		cprev, _, _, w.s2, err = canonKV(prev, w.s2)
		if err != nil {
			return err
		}
	}
	if int64(mod) <= s.last {
		return errors.New("event revision not increasing")
	}
	if !bytes.HasPrefix(key, s.prefix) {
		return errors.New("event key outside prefix")
	}
	s.last = int64(mod)
	if fault == "drop" && s.events == 500 {
		s.events++
		return nil
	}
	var hp uint64
	if hasPrev {
		hp = kvHash(cprev)
	}
	s.log = append(s.log, rec{int64(mod), fingerprint(typ, kvHash(ckv), hp, hasPrev)})
	s.events++
	return nil
}

// run reads events until main sends the expected counts and they are
// reached (plus a short grace period for extra events), or 5 s pass.
func (w *watcher) run() {
	var expect []int64
	var hard time.Time
	grace := false
	for {
		if err := w.frames(); err != nil {
			w.fatal = err
			return
		}
		if err := w.c.flush(); err != nil {
			w.fatal = err
			return
		}
		err := w.c.fill()
		w.tnow = now()
		if err == nil {
			continue
		}
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			w.fatal = err
			return
		}
		if expect == nil {
			select {
			case expect = <-w.final:
				hard = time.Now().Add(5 * time.Second)
			default:
			}
		}
		if expect == nil {
			w.c.tc.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			continue
		}
		caught := true
		for _, s := range w.streams {
			if s.events < expect[s.res] {
				caught = false
			}
		}
		if caught && grace || time.Now().After(hard) {
			return
		}
		if caught {
			grace = true
			w.c.tc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		} else {
			w.c.tc.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		}
	}
}
