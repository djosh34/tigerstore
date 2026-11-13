package loop

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

// fuzzSecret keys both the records a fuzz input seals and the reader.
var fuzzSecret = bytes.Repeat([]byte{7}, 32)

// sealFor seals inner content typ of plain, then pad zeros, as one record
// under d. Its header's length is the body's, cut to 16 bits.
func sealFor(d *direction, typ byte, plain []byte, pad int) []byte {
	inner := append(append(bytes.Clone(plain), typ), make([]byte, pad)...)
	rec := []byte{typeAppData, 3, 3, 0, 0}
	rec = binary.BigEndian.AppendUint16(rec[:3], uint16(len(inner)+tagSize))
	return d.aead.Seal(rec, d.next(), inner, rec[:recordHeader])
}

// fuzzSocket returns its bytes in pieces of chunk, then ErrWouldBlock.
type fuzzSocket struct {
	data  []byte
	chunk int
}

func (s *fuzzSocket) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, ErrWouldBlock
	}
	n := copy(p[:min(len(p), s.chunk)], s.data)
	s.data = s.data[n:]
	return n, nil
}

func (s *fuzzSocket) Write(p []byte) (int, error) { return len(p), nil }
func (s *fuzzSocket) Close() error                { return nil }

// FuzzRecords feeds the record layer's open path what a client could
// send: raw bytes, a close_notify, a record sealed under the reader's own
// keys with any inner content type, padding and length, one bit of it
// flipped, and any bytes after. It must only ever fail, never panic or
// read out of bounds; every plaintext it hands out must be the start of
// the application data that a reader of the test's own opens under the
// keys (authentic); and a record whose body or tag changed, with nothing
// before it, must be refused for its tag.
func FuzzRecords(f *testing.F) {
	f.Add([]byte{}, []byte("hello"), byte(typeAppData), uint8(0), uint16(0), []byte{}, false)
	f.Add([]byte{}, []byte("hello"), byte(typeAppData), uint8(200), uint16(0), []byte{23, 3, 3, 0xff, 0xff}, false)
	f.Add([]byte{23, 3, 3, 0x41, 0x01}, []byte{}, byte(typeAppData), uint8(0), uint16(0), []byte{}, false)
	f.Add([]byte{}, []byte("after close"), byte(typeAppData), uint8(0), uint16(0), []byte{}, true)
	f.Add([]byte{}, []byte{1, 0}, byte(typeAlert), uint8(3), uint16(0), []byte{}, false)
	f.Add([]byte{}, []byte{msgKeyUpdate, 0, 0, 1, 1}, byte(typeHandshake), uint8(0), uint16(0), []byte{}, false)
	f.Add([]byte{}, []byte("flipped"), byte(typeAppData), uint8(0), uint16(0x0107), []byte{}, false)
	// "flipped" seals to 29 bytes: byte 5 is the body's first, 28 the tag's
	// last.
	f.Add([]byte{}, []byte("flipped"), byte(typeAppData), uint8(0), uint16(0x0105), []byte{}, false)
	f.Add([]byte{}, []byte("flipped"), byte(typeAppData), uint8(0), uint16(0x011c), []byte{}, false)
	f.Add([]byte{}, bytes.Repeat([]byte("x"), maxPlaintext+1), byte(typeAppData), uint8(0), uint16(0), []byte{}, false)
	// Raw bytes that are a record which opens under the keys: its data may
	// come out.
	if s, err := suiteOf(tls.TLS_AES_128_GCM_SHA256); err == nil {
		if w, err := s.keyed(fuzzSecret); err == nil {
			f.Add(sealFor(&w, typeAppData, []byte("in raw"), 0), []byte("x"), byte(typeAppData), uint8(0), uint16(0), []byte{}, false)
		}
	}
	// Raw bytes with a dropped alert, a ticket or a KeyUpdate before a data
	// record: the data may come out (#646's Astra fix check).
	if s, err := suiteOf(tls.TLS_AES_128_GCM_SHA256); err == nil {
		ticket := []byte{msgNewSessionTicket, 0, 0, 14, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 1, 7, 0, 0}
		for _, first := range []struct {
			typ     byte
			content []byte
		}{{typeAlert, []byte{1, alertUserCanceled}}, {typeHandshake, ticket}, {typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 0}}} {
			w, err := s.keyed(fuzzSecret)
			if err != nil {
				break
			}
			raw := sealFor(&w, first.typ, first.content, 0)
			if first.content[0] == msgKeyUpdate {
				if w, err = s.updated(&w); err != nil {
					break
				}
			}
			raw = append(raw, sealFor(&w, typeAppData, []byte("after control"), 0)...)
			f.Add(raw, []byte("x"), byte(typeAppData), uint8(0), uint16(0), []byte{}, false)
		}
	}
	f.Fuzz(func(t *testing.T, raw, plain []byte, typ byte, pad uint8, flip uint16, tail []byte, closeFirst bool) {
		if len(plain) > 1<<17 {
			return
		}
		s, err := suiteOf(tls.TLS_AES_128_GCM_SHA256)
		if err != nil {
			t.Fatal(err)
		}
		w, err := s.keyed(fuzzSecret)
		if err != nil {
			t.Fatal(err)
		}
		rd, err := s.keyed(fuzzSecret)
		if err != nil {
			t.Fatal(err)
		}
		stream := bytes.Clone(raw)
		if closeFirst {
			stream = append(stream, sealFor(&w, typeAlert, []byte{1, alertCloseNotify}, 0)...)
		}
		rec := sealFor(&w, typ, plain, int(pad))
		flipAt := -1
		if flip != 0 {
			flipAt = int(flip) % len(rec)
			rec[flipAt] ^= byte(flip>>8) | 1
		}
		stream = append(stream, rec...)
		stream = append(stream, tail...)
		br := &Bridge{keepOut: maxIdleOut}
		br.Attach(&fuzzSocket{data: stream, chunk: 1 + (len(raw)+len(tail)+int(pad))%4096}, NewBuffers(32<<10, 2), nil)
		r := &Records{br: br, suite: s, rd: rd, wr: w}
		var got []byte
		var lastErr error
		buf := make([]byte, 4096)
		for range 1 << 12 {
			n, err := r.Read(buf)
			got = append(got, buf[:n]...)
			if err != nil && !errors.Is(err, ErrWouldBlock) {
				lastErr = err
			}
			if err != nil {
				if errors.Is(err, ErrWouldBlock) && len(br.buffered()) > 0 && n == 0 && r.err == nil {
					// A partial record waits for the rest, which never comes.
					break
				}
				if errors.Is(err, ErrWouldBlock) {
					continue
				}
				break
			}
		}
		// A record whose body or tag changed, with nothing before it, is
		// refused for its tag, whatever its inner content says: its header
		// still frames it, and it must not open.
		if flipAt >= recordHeader && len(raw) == 0 && !closeFirst && len(plain)+1+int(pad)+tagSize <= maxCiphertext {
			if lastErr == nil || !strings.Contains(lastErr.Error(), "does not open") {
				t.Fatalf("a record with byte %d of %d changed was not refused for its tag: %v", flipAt, len(rec), lastErr)
			}
		}
		want, err := authentic(s, stream)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(want, got) {
			t.Fatalf("plaintext %q came out, which is not the start of the application data that opens under the keys, %q", got, want)
		}
	})
}

// authentic opens stream's records in order with a reader of its own and
// returns their application data: all that Records may hand out of it. It
// follows KeyUpdates to the next key and reads on past dropped alerts and
// other handshake messages, and stops at the first record that does not
// open, a close_notify or fatal alert, or a record too long. It is looser
// than Records, which may stop sooner (a malformed ticket, too many
// records without data), so Records' data must be its start; but every
// byte it returns opened under the keys.
func authentic(s suite, stream []byte) ([]byte, error) {
	d, err := s.keyed(fuzzSecret)
	if err != nil {
		return nil, err
	}
	var out, post []byte
	for len(stream) >= recordHeader {
		n := int(binary.BigEndian.Uint16(stream[3:]))
		if stream[0] != typeAppData || n < 1+tagSize || n > maxCiphertext || len(stream) < recordHeader+n {
			break
		}
		pt, ok := opens(&d, stream[:recordHeader+n])
		if !ok || len(pt) > maxPlaintext+1 {
			break
		}
		stream = stream[recordHeader+n:]
		i := len(pt) - 1
		for i >= 0 && pt[i] == 0 {
			i--
		}
		if i < 0 {
			break
		}
		switch content := pt[:i]; pt[i] {
		case typeAppData:
			out = append(out, content...)
		case typeAlert:
			if len(content) != 2 || content[1] != alertUserCanceled {
				return out, nil
			}
		case typeHandshake:
			post = append(post, content...)
			update := false
			for len(post) >= 4 {
				m := int(post[1])<<16 | int(post[2])<<8 | int(post[3])
				if len(post) < 4+m {
					break
				}
				update = update || post[0] == msgKeyUpdate
				post = post[4+m:]
			}
			if update {
				if d, err = s.updated(&d); err != nil {
					return nil, err
				}
			}
		default:
			return out, nil
		}
	}
	return out, nil
}

// opens opens rec under d, and reports whether it did: one that does not
// ends the application data authentic returns.
func opens(d *direction, rec []byte) ([]byte, bool) {
	pt, err := d.aead.Open(nil, d.next(), rec[recordHeader:], rec[:recordHeader])
	return pt, err == nil
}
