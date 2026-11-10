package main

// TLS 1.3's record layer for the connections to the store (RFC 8446, 5).
// crypto/tls runs the handshake; then the generator seals and opens the
// records itself, with the traffic secrets crypto/tls logs. One read takes
// in as many records as the kernel holds, and each record opens straight
// into the buffer the HTTP/2 frames are read from, where crypto/tls would
// copy each byte twice more, into its own buffer and out to the reader's.
// One write sends every record of a flush, where crypto/tls writes each
// record on its own.

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"strings"
)

const (
	recordHeaderLen = 5
	maxPlaintext    = 1 << 14
	maxCiphertext   = maxPlaintext + 256 // RFC 8446, 5.2
	tagLen          = 16
	nonceLen        = 12

	typeAlert     = 21
	typeHandshake = 22
	typeAppData   = 23

	msgNewSessionTicket = 4
	msgKeyUpdate        = 24

	// recordBuf is the bytes of records one read may take in, and of
	// records opened and not yet read as frames.
	recordBuf = 256 << 10
)

var (
	errRecord    = errors.New("malformed TLS record")
	errKeyUpdate = errors.New("the server updated its TLS keys, which the record layer does not follow")
	errCipher    = errors.New("a TLS cipher suite the record layer does not have")
	errAlert     = errors.New("TLS alert from the server")
	errNoSecrets = errors.New("crypto/tls logged no traffic secrets")
)

// direction is the AEAD of one direction and the sequence number of its
// next record.
type direction struct {
	aead  cipher.AEAD
	iv    [nonceLen]byte
	seq   uint64
	nonce [nonceLen]byte
}

// next returns the nonce of the next record: the IV with the sequence
// number XORed into its last 8 bytes.
func (d *direction) next() []byte {
	d.nonce = d.iv
	for i := range 8 {
		d.nonce[nonceLen-1-i] ^= byte(d.seq >> (8 * i))
	}
	d.seq++
	return d.nonce[:]
}

// newDirection derives the key and IV of a traffic secret (RFC 8446, 7.3).
func newDirection(suite uint16, secret []byte) (direction, error) {
	var h func() hash.Hash
	var keyLen int
	//exhaustive:ignore // the record layer has AES-GCM only
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256:
		h, keyLen = sha256.New, 16
	case tls.TLS_AES_256_GCM_SHA384:
		h, keyLen = sha512.New384, 32
	default:
		return direction{}, fmt.Errorf("%w: %s", errCipher, tls.CipherSuiteName(suite))
	}
	key, err := expandLabel(h, secret, "key", keyLen)
	if err != nil {
		return direction{}, err
	}
	iv, err := expandLabel(h, secret, "iv", nonceLen)
	if err != nil {
		return direction{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return direction{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return direction{}, err
	}
	d := direction{aead: aead}
	copy(d.iv[:], iv)
	return d, nil
}

// expandLabel is HKDF-Expand-Label with an empty context (RFC 8446, 7.1).
func expandLabel(h func() hash.Hash, secret []byte, label string, n int) ([]byte, error) {
	full := "tls13 " + label
	info := binary.BigEndian.AppendUint16(nil, uint16(n))
	info = append(info, byte(len(full)))
	info = append(info, full...)
	info = append(info, 0)
	return hkdf.Expand(h, secret, string(info), n)
}

// keyLog keeps the application traffic secrets crypto/tls logs, one line
// a Write.
type keyLog struct {
	client, server []byte
}

func (k *keyLog) Write(p []byte) (int, error) {
	f := strings.Fields(string(p))
	if len(f) != 3 {
		return len(p), nil
	}
	var to *[]byte
	switch f[0] {
	case "CLIENT_TRAFFIC_SECRET_0":
		to = &k.client
	case "SERVER_TRAFFIC_SECRET_0":
		to = &k.server
	default:
		return len(p), nil
	}
	secret, err := hex.DecodeString(f[2])
	*to = secret
	return len(p), err
}

// recordConn reads and writes TLS 1.3 records on nc. One goroutine reads
// (Peek, Discard, Buffered) and one writes (Write).
type recordConn struct {
	nc net.Conn
	// in[ir:iw] are records read from nc and not opened yet; pt[pr:pw]
	// is what the opened ones held and the reader has not taken.
	in     []byte
	ir, iw int
	pt     []byte
	pr, pw int
	rd, wr direction
	out    []byte // the records of one Write
	hdr    [recordHeaderLen]byte
	// err ended the records: a record that failed, a key update, an alert
	// or the end of nc. Every later open returns it, so Peek returns it
	// once the bytes before it are taken.
	err error
}

// clientHandshake runs crypto/tls's handshake on nc and returns the record
// layer of the connection. crypto/tls reads the handshake through
// handshakeReader, which never reads past the record crypto/tls asked for,
// so the records after the handshake stay for the record layer.
func clientHandshake(nc net.Conn, addr string) (*recordConn, error) {
	cfg := clientTLS.Clone()
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Join(err, nc.Close())
		}
		cfg.ServerName = host
	}
	keys := &keyLog{}
	cfg.KeyLogWriter = keys
	rc := &recordConn{nc: nc, in: make([]byte, recordBuf), pt: make([]byte, recordBuf)}
	hr := &handshakeReader{Conn: nc, rc: rc}
	tc := tls.Client(hr, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(err, nc.Close())
	}
	st := tc.ConnectionState()
	var err error
	switch {
	case st.NegotiatedProtocol != "h2":
		err = fmt.Errorf("%s: the server did not negotiate h2", addr)
	case st.Version != tls.VersionTLS13:
		err = fmt.Errorf("%s: TLS %s, not 1.3", addr, tls.VersionName(st.Version))
	case keys.client == nil || keys.server == nil:
		err = errNoSecrets
	case hr.left != 0:
		err = fmt.Errorf("%w: the handshake ended inside a record", errRecord)
	}
	if err == nil {
		rc.wr, err = newDirection(st.CipherSuite, keys.client)
	}
	if err == nil {
		rc.rd, err = newDirection(st.CipherSuite, keys.server)
	}
	if err != nil {
		return nil, errors.Join(err, nc.Close())
	}
	return rc, nil
}

// handshakeReader hands crypto/tls the handshake's records, never more
// than the rest of the record it reads.
type handshakeReader struct {
	net.Conn
	rc   *recordConn
	left int // bytes of the record crypto/tls reads still to hand over
}

func (h *handshakeReader) Read(p []byte) (int, error) {
	rc := h.rc
	if h.left == 0 {
		if err := rc.fill(recordHeaderLen); err != nil {
			return 0, err
		}
		h.left = recordHeaderLen + int(binary.BigEndian.Uint16(rc.in[rc.ir+3:]))
	}
	if err := rc.fill(1); err != nil {
		return 0, err
	}
	n := copy(p[:min(len(p), h.left)], rc.in[rc.ir:rc.iw])
	rc.ir += n
	h.left -= n
	return n, nil
}

// fill reads from nc until in holds n bytes not opened yet.
func (rc *recordConn) fill(n int) error {
	if rc.iw-rc.ir >= n {
		return nil
	}
	// Each read gets at least half the buffer: a read into what is left
	// at its end took in little, and doubled the reads.
	if len(rc.in)-rc.iw < len(rc.in)/2 || len(rc.in)-rc.ir < n {
		rc.iw = copy(rc.in, rc.in[rc.ir:rc.iw])
		rc.ir = 0
	}
	for rc.iw-rc.ir < n {
		m, err := rc.nc.Read(rc.in[rc.iw:])
		rc.iw += m
		if err != nil && rc.iw-rc.ir < n {
			return err
		}
	}
	return nil
}

// whole reports whether in holds the whole next record.
func (rc *recordConn) whole() bool {
	return rc.iw-rc.ir >= recordHeaderLen && rc.iw-rc.ir >= recordHeaderLen+int(binary.BigEndian.Uint16(rc.in[rc.ir+3:]))
}

// open opens the next record into pt, reading it from nc first if it is
// not all there. Once one fails, open returns that error every time.
func (rc *recordConn) open() error {
	if rc.err == nil {
		rc.err = rc.openRecord()
	}
	return rc.err
}

// openRecord opens the next record, as open does, and returns why it
// could not.
func (rc *recordConn) openRecord() error {
	if err := rc.fill(recordHeaderLen); err != nil {
		return err
	}
	hdr := rc.in[rc.ir : rc.ir+recordHeaderLen]
	n := int(binary.BigEndian.Uint16(hdr[3:]))
	if hdr[0] != typeAppData || n < tagLen+1 || n > maxCiphertext {
		return fmt.Errorf("%w: type %d, %d bytes", errRecord, hdr[0], n)
	}
	if err := rc.fill(recordHeaderLen + n); err != nil {
		return err
	}
	if len(rc.pt)-rc.pw < n {
		rc.pw = copy(rc.pt, rc.pt[rc.pr:rc.pw])
		rc.pr = 0
	}
	hdr = rc.in[rc.ir : rc.ir+recordHeaderLen]
	body := rc.in[rc.ir+recordHeaderLen : rc.ir+recordHeaderLen+n]
	plain, err := rc.rd.aead.Open(rc.pt[rc.pw:rc.pw], rc.rd.next(), body, hdr)
	if err != nil {
		return err
	}
	rc.ir += recordHeaderLen + n
	// The record's content type is its last byte other than padding.
	i := len(plain) - 1
	for i >= 0 && plain[i] == 0 {
		i--
	}
	if i < 0 {
		return fmt.Errorf("%w: no content type", errRecord)
	}
	switch plain[i] {
	case typeAppData:
		rc.pw += i
		return nil
	case typeHandshake:
		return postHandshake(plain[:i])
	case typeAlert:
		if i == 2 && plain[1] == 0 {
			return io.EOF // close_notify
		}
		if i == 2 {
			return fmt.Errorf("%w: %d", errAlert, plain[1])
		}
	}
	return fmt.Errorf("%w: content type %d", errRecord, plain[i])
}

// postHandshake takes in the handshake messages of a record after the
// handshake: it drops session tickets, which the generator never resumes
// with, and refuses a key update.
func postHandshake(m []byte) error {
	for len(m) > 0 {
		if len(m) < 4 {
			return fmt.Errorf("%w: a handshake message cut short", errRecord)
		}
		n := 4 + (int(m[1])<<16 | int(m[2])<<8 | int(m[3]))
		if n > len(m) {
			return fmt.Errorf("%w: a handshake message across records", errRecord)
		}
		switch m[0] {
		case msgNewSessionTicket:
		case msgKeyUpdate:
			return errKeyUpdate
		default:
			return fmt.Errorf("%w: handshake message %d after the handshake", errRecord, m[0])
		}
		m = m[n:]
	}
	return nil
}

// Peek returns the next n bytes the records held, without taking them;
// they stay valid until the next call.
func (rc *recordConn) Peek(n int) ([]byte, error) {
	for rc.pw-rc.pr < n {
		if err := rc.open(); err != nil {
			return nil, err
		}
	}
	return rc.pt[rc.pr : rc.pr+n], nil
}

// Discard takes the next n bytes, which Peek returned.
func (rc *recordConn) Discard(n int) (int, error) {
	if n > rc.pw-rc.pr {
		return 0, fmt.Errorf("%w: discard of %d bytes past the %d opened", errRecord, n, rc.pw-rc.pr)
	}
	rc.pr += n
	return n, nil
}

// Buffered opens every whole record already read and returns the bytes
// Peek can return without reading from nc.
func (rc *recordConn) Buffered() int {
	// An error stays for Peek, after the bytes before it (open).
	for rc.whole() && rc.open() == nil {
	}
	return rc.pw - rc.pr
}

// Write seals p into records and writes them all at once. p must have
// room for one byte more than its length: each record's content type goes
// after its bytes, in place, for as long as the record is sealed.
func (rc *recordConn) Write(p []byte) (int, error) {
	rc.out = rc.out[:0]
	for done := 0; done < len(p); {
		n := min(len(p)-done, maxPlaintext)
		chunk := p[done : done+n : done+n+1]
		end := chunk[:n+1]
		saved := end[n]
		end[n] = typeAppData
		l := n + 1 + tagLen
		rc.hdr = [recordHeaderLen]byte{typeAppData, 3, 3, byte(l >> 8), byte(l)}
		rc.out = append(rc.out, rc.hdr[:]...)
		rc.out = rc.wr.aead.Seal(rc.out, rc.wr.next(), end, rc.hdr[:])
		end[n] = saved
		done += n
	}
	if _, err := rc.nc.Write(rc.out); err != nil {
		return 0, err
	}
	return len(p), nil
}
