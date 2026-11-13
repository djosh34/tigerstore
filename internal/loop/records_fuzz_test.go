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
// read out of bounds, and every plaintext it hands out must be the start
// of a record that opened under the keys: none after raw bytes, after a
// close_notify, or of a record with a flipped bit.
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
		if len(got) == 0 {
			return
		}
		if len(raw) > 0 || closeFirst || flip != 0 {
			t.Fatalf("%d bytes of plaintext came out of a stream whose record should not open (raw %d bytes, close first %v, flip %#x)", len(got), len(raw), closeFirst, flip)
		}
		if !bytes.HasPrefix(plain, got) {
			t.Fatalf("plaintext %q came out, which is not the start of the record sealed, %q", got, plain)
		}
	})
}
