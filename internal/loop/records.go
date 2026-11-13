package loop

import (
	"bytes"
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
	"math"
	"net"
	"sync"
	"time"
)

// TLS 1.3's record layer (RFC 8446, section 5), after crypto/tls's
// handshake. crypto/tls runs the handshake over a Bridge and logs the
// traffic secrets it derives (Config.KeyLogWriter); from there on Records
// seals and opens the connection's records itself. It opens a record in
// place in the bridge's read buffer and hands out its plaintext from
// there, and it writes plaintext straight into the body of a record at
// the end of the bridge's write buffer, which it seals there in place: one
// record for each 16 KiB that a turn writes, where crypto/tls copies the
// plaintext into a record buffer of its own, seals it and copies it on.
const (
	recordHeader = 5
	// maxPlaintext is the most a record carries, and maxCiphertext the
	// most a record's body may hold on the wire with its content type,
	// padding and tag.
	maxPlaintext  = 1 << 14
	maxCiphertext = maxPlaintext + 256
	tagSize       = 16
	nonceSize     = 12

	typeAlert     = 21
	typeHandshake = 22
	typeAppData   = 23

	alertCloseNotify  = 0
	alertUnexpected   = 10
	alertBadRecordMAC = 20
	alertDecodeError  = 50

	msgNewSessionTicket = 4
	msgKeyUpdate        = 24
	// maxPostHandshake bounds the handshake messages a peer may send after
	// the handshake that Records holds while they arrive in pieces.
	maxPostHandshake = 1 << 16
	// alertWait is how long an alert waits for a peer that does not read,
	// before Attach, as crypto/tls's close_notify does.
	alertWait = 5 * time.Second
)

// keyUpdateAfter is how many records a write key seals before Records
// sends a KeyUpdate and goes on under the next one: RFC 8446 (5.5) puts the
// safe use of an AES-GCM key at about 2^24.5 full records, which a busy
// peer connection reaches in a few hundred GB; crypto/tls never updates
// on its own. So a write sequence number stays far from wrapping, and a
// KeyUpdate starts the next key at 0 with a new key and IV, so no nonce
// repeats under a key. A test lowers it.
var keyUpdateAfter uint64 = 1 << 24

// appHeader starts a record whose length seal sets, and sealRoom is the
// room a record needs past its content: the content type and the tag.
var (
	appHeader = [recordHeader]byte{typeAppData, 3, 3, 0, 0}
	sealRoom  [1 + tagSize]byte
)

// ErrNotTLS13 is the error of a handshake that negotiated a version below
// TLS 1.3, whose records Records cannot carry.
var ErrNotTLS13 = errors.New("loop: the connection is not TLS 1.3")

// direction is the keys and the sequence number of one direction of a
// connection's records.
type direction struct {
	secret []byte
	aead   cipher.AEAD
	iv     [nonceSize]byte
	seq    uint64
	nonce  [nonceSize]byte
}

// next returns the nonce of the next record and counts it.
func (d *direction) next() []byte {
	d.nonce = d.iv
	for i := range 8 {
		d.nonce[nonceSize-1-i] ^= byte(d.seq >> (8 * i))
	}
	d.seq++
	return d.nonce[:]
}

// suite is a TLS 1.3 cipher suite's hash and AES key size.
type suite struct {
	hash   func() hash.Hash
	keyLen int
}

func suiteOf(id uint16) (suite, error) {
	switch id {
	case tls.TLS_AES_128_GCM_SHA256:
		return suite{sha256.New, 16}, nil
	case tls.TLS_AES_256_GCM_SHA384:
		return suite{sha512.New384, 32}, nil
	}
	return suite{}, fmt.Errorf("loop: cipher suite %s is not supported", tls.CipherSuiteName(id))
}

// keyed makes the keys of a direction from its traffic secret.
func (s suite) keyed(secret []byte) (direction, error) {
	key, err := expandLabel(s.hash, secret, "key", s.keyLen)
	if err != nil {
		return direction{}, err
	}
	iv, err := expandLabel(s.hash, secret, "iv", nonceSize)
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
	d := direction{secret: secret, aead: aead}
	copy(d.iv[:], iv)
	return d, nil
}

// updated makes the keys of a direction after a KeyUpdate (RFC 8446,
// 7.2).
func (s suite) updated(d *direction) (direction, error) {
	secret, err := expandLabel(s.hash, d.secret, "traffic upd", s.hash().Size())
	if err != nil {
		return direction{}, err
	}
	return s.keyed(secret)
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

// secrets keeps the application traffic secrets crypto/tls logs during a
// handshake.
type secrets struct {
	mu             sync.Mutex
	client, server []byte
}

func (k *secrets) Write(p []byte) (int, error) {
	f := bytes.Fields(p)
	if len(f) != 3 {
		return len(p), nil
	}
	var to *[]byte
	switch string(f[0]) {
	case "CLIENT_TRAFFIC_SECRET_0":
		to = &k.client
	case "SERVER_TRAFFIC_SECRET_0":
		to = &k.server
	default:
		return len(p), nil
	}
	s, err := hex.DecodeString(string(f[2]))
	if err != nil {
		return 0, err
	}
	k.mu.Lock()
	*to = s
	k.mu.Unlock()
	return len(p), nil
}

// keyLog passes each line crypto/tls logs to the configuration's own key
// log, if it has one, and to the handshake's secrets.
type keyLog struct {
	next io.Writer
	k    *secrets
}

func (l keyLog) Write(p []byte) (int, error) {
	if l.next != nil {
		if _, err := l.next.Write(p); err != nil {
			return 0, err
		}
	}
	return l.k.Write(p)
}

// keyLogging returns a copy of cfg that logs its traffic secrets to k,
// sends no session tickets, so that nothing follows the handshake under
// the new keys before Records takes over, and does the same for the
// configuration its GetConfigForClient returns.
func keyLogging(cfg *tls.Config, k *secrets) *tls.Config {
	cfg = cfg.Clone()
	cfg.KeyLogWriter = keyLog{next: cfg.KeyLogWriter, k: k}
	cfg.SessionTicketsDisabled = true
	if get := cfg.GetConfigForClient; get != nil {
		cfg.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			c, err := get(hi)
			if c == nil || err != nil {
				return c, err
			}
			return keyLogging(c, k), nil
		}
	}
	return cfg
}

// Handshake runs crypto/tls's handshake over br, as the server or the
// client, within ctx. A TLS 1.3 connection's records then go through the
// returned Records; a connection of an older version, which it cannot
// carry, comes back as the crypto/tls connection, with ErrNotTLS13. On
// any other error the raw connection is left to the caller to close.
func Handshake(ctx context.Context, br *Bridge, cfg *tls.Config, server bool) (*Records, *tls.Conn, error) {
	k := &secrets{}
	cfg = keyLogging(cfg, k)
	br.byRecord = true
	defer func() { br.byRecord = false }()
	var tc *tls.Conn
	if server {
		tc = tls.Server(br, cfg)
	} else {
		tc = tls.Client(br, cfg)
	}
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, nil, err
	}
	state := tc.ConnectionState()
	if state.Version != tls.VersionTLS13 {
		return nil, tc, ErrNotTLS13
	}
	if br.recLeft != 0 {
		return nil, nil, errors.New("loop: crypto/tls's handshake ended within a record")
	}
	k.mu.Lock()
	client, server13 := k.client, k.server
	k.mu.Unlock()
	if client == nil || server13 == nil {
		return nil, nil, errors.New("loop: crypto/tls logged no traffic secrets")
	}
	s, err := suiteOf(state.CipherSuite)
	if err != nil {
		return nil, nil, err
	}
	rdSecret, wrSecret := client, server13
	if !server {
		rdSecret, wrSecret = server13, client
	}
	r := &Records{br: br, state: state, suite: s}
	if r.rd, err = s.keyed(rdSecret); err != nil {
		return nil, nil, err
	}
	if r.wr, err = s.keyed(wrSecret); err != nil {
		return nil, nil, err
	}
	return r, nil, nil
}

// Records carries a TLS 1.3 connection's records after its handshake
// (Handshake). Before the bridge is attached it reads and writes with
// blocking calls on the raw connection, as a net.Conn; after Attach, on
// the loop, a read fails with ErrWouldBlock until a whole record has
// arrived, and a write only goes into the bridge's write buffer, which
// Flush seals and sends.
type Records struct {
	br    *Bridge
	state tls.ConnectionState
	suite suite
	rd    direction
	wr    direction
	// plain is the plaintext of the record that was opened last and that
	// Read has not taken; it lies in the bridge's read buffer, whose held
	// bytes the record took up, and which stay until plain is taken.
	plain []byte
	held  int
	// post holds handshake messages after the handshake while they arrive
	// in pieces.
	post []byte
	// open is how many bytes the record being written takes at the end of
	// the bridge's write buffer, its header included; 0 for none.
	open int
	// scratch holds a record that is sealed and written at once, before
	// Attach.
	scratch []byte
	// err is the error that ended reading: io.EOF after close_notify, and
	// werr the one that ended writing.
	err       error
	werr      error
	closeSent bool
	// rotating is set while a KeyUpdate of keyUpdateAfter goes out.
	rotating bool
}

var _ net.Conn = (*Records)(nil)

// ConnectionState is the handshake's connection state.
func (r *Records) ConnectionState() tls.ConnectionState { return r.state }

// NetConn is the bridge under the records.
func (r *Records) NetConn() *Bridge { return r.br }

// Read reads plaintext. After Attach it fails with ErrWouldBlock while
// no whole record waits.
func (r *Records) Read(p []byte) (int, error) {
	for len(r.plain) == 0 {
		if r.held > 0 {
			r.br.consume(r.held)
			r.held = 0
		}
		if r.err != nil {
			return 0, r.err
		}
		if err := r.openNext(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.plain)
	r.plain = r.plain[n:]
	if len(r.plain) == 0 {
		// An empty slice into the buffer would keep the buffer.
		r.plain = nil
	}
	return n, nil
}

// openNext opens the next record in place and takes its content.
func (r *Records) openNext() error {
	if err := r.br.fill(recordHeader); err != nil {
		return err
	}
	hdr := r.br.buffered()[:recordHeader]
	n := int(binary.BigEndian.Uint16(hdr[3:]))
	if hdr[0] != typeAppData || n < 1+tagSize || n > maxCiphertext {
		return r.fail(alertUnexpected, fmt.Errorf("loop: a TLS record of type %d and %d bytes after the handshake", hdr[0], n))
	}
	if err := r.br.fill(recordHeader + n); err != nil {
		return err
	}
	rec := r.br.buffered()[:recordHeader+n]
	if r.rd.seq == math.MaxUint64 {
		// The peer must have updated its key long before: the next nonce
		// would repeat the first.
		return r.fail(alertUnexpected, errors.New("loop: a TLS read key used for 2^64 records"))
	}
	pt, err := r.rd.aead.Open(rec[recordHeader:recordHeader], r.rd.next(), rec[recordHeader:], rec[:recordHeader])
	if err != nil {
		return r.fail(alertBadRecordMAC, fmt.Errorf("loop: a TLS record does not open: %w", err))
	}
	r.held = recordHeader + n
	// The content type is the last byte that is not padding.
	i := len(pt) - 1
	for i >= 0 && pt[i] == 0 {
		i--
	}
	if i < 0 {
		return r.fail(alertUnexpected, errors.New("loop: a TLS record without a content type"))
	}
	content := pt[:i]
	if len(content) > maxPlaintext {
		return r.fail(alertUnexpected, errors.New("loop: a TLS record over 16 KiB of plaintext"))
	}
	switch pt[i] {
	case typeAppData:
		r.plain = content
		return nil
	case typeAlert:
		return r.alert(content)
	case typeHandshake:
		return r.postHandshake(content)
	}
	return r.fail(alertUnexpected, fmt.Errorf("loop: a TLS record of inner type %d", pt[i]))
}

// alert takes an alert: close_notify ends the stream, and every other one
// ends the connection with an error, as crypto/tls does in TLS 1.3.
func (r *Records) alert(b []byte) error {
	if len(b) != 2 {
		return r.fail(alertDecodeError, errors.New("loop: a malformed TLS alert"))
	}
	if b[1] == alertCloseNotify {
		r.err = io.EOF
		return nil
	}
	r.err = &net.OpError{Op: "remote error", Err: tls.AlertError(b[1])}
	return nil
}

// postHandshake takes the handshake messages after the handshake: a
// KeyUpdate, which it answers if asked, and a NewSessionTicket, which it
// ignores, as a client that keeps no sessions.
func (r *Records) postHandshake(b []byte) error {
	r.post = append(r.post, b...)
	for len(r.post) >= 4 {
		n := int(r.post[1])<<16 | int(r.post[2])<<8 | int(r.post[3])
		if n > maxPostHandshake {
			return r.fail(alertUnexpected, errors.New("loop: a TLS handshake message over 64 KiB after the handshake"))
		}
		if len(r.post) < 4+n {
			break
		}
		typ, body := r.post[0], r.post[4:4+n]
		switch typ {
		case msgNewSessionTicket:
		case msgKeyUpdate:
			if len(body) != 1 || body[0] > 1 {
				return r.fail(alertDecodeError, errors.New("loop: a malformed TLS KeyUpdate"))
			}
			rd, err := r.suite.updated(&r.rd)
			if err != nil {
				return err
			}
			r.rd = rd
			if body[0] == 1 {
				if err := r.updateWrite(); err != nil {
					return err
				}
			}
		default:
			return r.fail(alertUnexpected, fmt.Errorf("loop: a TLS handshake message of type %d after the handshake", typ))
		}
		r.post = r.post[4+n:]
	}
	if len(r.post) == 0 {
		r.post = nil
	}
	return nil
}

// updateWrite sends a KeyUpdate that asks for none back, and writes on
// under the next keys.
func (r *Records) updateWrite() error {
	if err := r.record(typeHandshake, []byte{msgKeyUpdate, 0, 0, 1, 0}); err != nil {
		return err
	}
	wr, err := r.suite.updated(&r.wr)
	if err != nil {
		return err
	}
	r.wr = wr
	return nil
}

// fail sends the peer alert desc, best effort, and ends reading with err.
func (r *Records) fail(desc byte, err error) error {
	if !r.closeSent {
		r.closeSent = true
		if !r.br.Attached() {
			_ = r.br.Conn.SetWriteDeadline(time.Now().Add(alertWait))
		}
		_ = r.record(typeAlert, []byte{2, desc})
		if r.br.Attached() {
			_ = r.br.Flush()
		}
	}
	r.err = err
	return err
}

// Write writes plaintext: before Attach it seals and sends it at once,
// after it into the open record at the end of the bridge's write buffer.
func (r *Records) Write(p []byte) (int, error) {
	if r.closeSent {
		return 0, net.ErrClosed
	}
	if r.werr != nil {
		return 0, r.werr
	}
	if !r.br.Attached() {
		for done := 0; done < len(p); {
			k := min(len(p)-done, maxPlaintext)
			if err := r.record(typeAppData, p[done:done+k]); err != nil {
				return done, err
			}
			done += k
		}
		return len(p), nil
	}
	for done := 0; done < len(p); {
		if r.open == 0 {
			// The length is set when the record is sealed.
			if _, err := r.br.Write(appHeader[:]); err != nil {
				return done, err
			}
			r.open = recordHeader
		}
		k := min(len(p)-done, recordHeader+maxPlaintext-r.open)
		if _, err := r.br.Write(p[done : done+k]); err != nil {
			return done, err
		}
		r.open += k
		done += k
		if r.open == recordHeader+maxPlaintext {
			r.seal(typeAppData)
		}
	}
	return len(p), nil
}

// seal seals the open record in place, as inner type typ.
func (r *Records) seal(typ byte) {
	// Room for the content type and the tag, then the record again.
	_, _ = r.br.Write(sealRoom[:])
	rec := r.br.tail(r.open + 1 + tagSize)
	body := rec[recordHeader : r.open+1]
	body[len(body)-1] = typ
	binary.BigEndian.PutUint16(rec[3:recordHeader], uint16(len(body)+tagSize))
	r.wr.aead.Seal(body[:0], r.wr.next(), body, rec[:recordHeader])
	r.open = 0
	r.rotate()
}

// rotate sends a KeyUpdate once the write key sealed keyUpdateAfter
// records.
func (r *Records) rotate() {
	if r.wr.seq < keyUpdateAfter || r.rotating || r.werr != nil {
		return
	}
	r.rotating = true
	if err := r.updateWrite(); err != nil {
		r.werr = err
	}
	r.rotating = false
}

// record writes content as one record of inner type typ: sealed and sent
// at once before Attach, and after the open record at the end of the
// bridge's write buffer after it.
func (r *Records) record(typ byte, content []byte) error {
	if !r.br.Attached() {
		n := recordHeader + len(content) + 1 + tagSize
		if cap(r.scratch) < n {
			r.scratch = make([]byte, n)
		}
		rec := r.scratch[:n]
		rec[0], rec[1], rec[2] = typeAppData, 3, 3
		binary.BigEndian.PutUint16(rec[3:], uint16(n-recordHeader))
		body := rec[recordHeader : recordHeader+len(content)+1]
		copy(body, content)
		body[len(content)] = typ
		r.wr.aead.Seal(body[:0], r.wr.next(), body, rec[:recordHeader])
		if _, err := r.br.Conn.Write(rec); err != nil {
			return err
		}
		r.rotate()
		return r.werr
	}
	if r.open > 0 {
		r.seal(typeAppData)
	}
	if _, err := r.br.Write(appHeader[:]); err != nil {
		return err
	}
	if _, err := r.br.Write(content); err != nil {
		return err
	}
	r.open = recordHeader + len(content)
	r.seal(typ)
	return nil
}

// Flush seals the open record and sends what the bridge holds, as far as
// the socket takes it now (Bridge.Flush).
func (r *Records) Flush() error {
	if r.open > 0 {
		r.seal(typeAppData)
	}
	return r.br.Flush()
}

// Close sends close_notify, as crypto/tls does, and closes the connection.
// Before Attach the alert waits at most for the write deadline set; after
// it, it goes only as far as the socket takes it at once.
func (r *Records) Close() error {
	if !r.closeSent {
		r.closeSent = true
		if !r.br.Attached() {
			_ = r.br.Conn.SetWriteDeadline(time.Now().Add(alertWait))
		}
		if err := r.record(typeAlert, []byte{1, alertCloseNotify}); err == nil && r.br.Attached() {
			_ = r.br.Flush()
		}
	}
	return r.br.Close()
}

func (r *Records) LocalAddr() net.Addr                { return r.br.LocalAddr() }
func (r *Records) RemoteAddr() net.Addr               { return r.br.RemoteAddr() }
func (r *Records) SetDeadline(t time.Time) error      { return r.br.SetDeadline(t) }
func (r *Records) SetReadDeadline(t time.Time) error  { return r.br.SetReadDeadline(t) }
func (r *Records) SetWriteDeadline(t time.Time) error { return r.br.SetWriteDeadline(t) }
