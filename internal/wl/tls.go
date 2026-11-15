package wl

// TLS 1.3 after crypto/tls's handshake (RFC 8446, 5). crypto/tls runs the
// handshake on a blocking connection in a goroutine; then the loop takes
// the socket and seals and opens the records itself, with the traffic
// secrets crypto/tls logs. Replies are written straight into the record
// plaintext of a connection's send buffer and sealed there in place.

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
	"net"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	recHdr        = 5
	maxPlain      = 1 << 14
	maxCipher     = maxPlain + 256
	tagLen        = 16
	nonceLen      = 12
	recOverhead   = recHdr + 1 + tagLen // header, content type, tag
	typeAlert     = 21
	typeHandshake = 22
	typeAppData   = 23
)

var (
	errRecord    = errors.New("malformed TLS record")
	errNoSecrets = errors.New("crypto/tls logged no traffic secrets")
)

// aeadDir is the AEAD of one direction and the sequence number of its next
// record.
type aeadDir struct {
	aead  cipher.AEAD
	iv    [nonceLen]byte
	seq   uint64
	nonce [nonceLen]byte
}

func (d *aeadDir) next() []byte {
	d.nonce = d.iv
	for i := range 8 {
		d.nonce[nonceLen-1-i] ^= byte(d.seq >> (8 * i))
	}
	d.seq++
	return d.nonce[:]
}

func newDir(suite uint16, secret []byte) (aeadDir, error) {
	var h func() hash.Hash
	var keyLen int
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256:
		h, keyLen = sha256.New, 16
	case tls.TLS_AES_256_GCM_SHA384:
		h, keyLen = sha512.New384, 32
	default:
		return aeadDir{}, fmt.Errorf("cipher suite %s not supported", tls.CipherSuiteName(suite))
	}
	key, err := expandLabel(h, secret, "key", keyLen)
	if err != nil {
		return aeadDir{}, err
	}
	iv, err := expandLabel(h, secret, "iv", nonceLen)
	if err != nil {
		return aeadDir{}, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return aeadDir{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return aeadDir{}, err
	}
	d := aeadDir{aead: aead}
	copy(d.iv[:], iv)
	return d, nil
}

func expandLabel(h func() hash.Hash, secret []byte, label string, n int) ([]byte, error) {
	full := "tls13 " + label
	info := binary.BigEndian.AppendUint16(nil, uint16(n))
	info = append(info, byte(len(full)))
	info = append(info, full...)
	info = append(info, 0)
	return hkdf.Expand(h, secret, string(info), n)
}

type keyLog struct{ client, server []byte }

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
	s, err := hex.DecodeString(f[2])
	*to = s
	return len(p), err
}

// handoff is a connection after its handshake, for the loop to take.
type handoff struct {
	fd     int
	rd, wr aeadDir
	left   []byte // ciphertext read past the handshake
	alpn   string
	kind   uint8
	peer   uint64 // the node an incoming or outgoing peer connection is with
	cn     string
}

const (
	kindClient uint8 = iota
	kindPeerIn
	kindPeerOut
)

// hsReader hands crypto/tls the handshake's records and never more than
// the record it reads, so the records after the handshake stay for the
// loop.
type hsReader struct {
	net.Conn
	buf      []byte
	r, w     int
	leftInRe int
}

func (h *hsReader) fill(n int) error {
	for h.w-h.r < n {
		if len(h.buf)-h.r < n {
			h.w = copy(h.buf, h.buf[h.r:h.w])
			h.r = 0
		}
		m, err := h.Conn.Read(h.buf[h.w:])
		h.w += m
		if err != nil && h.w-h.r < n {
			return err
		}
	}
	return nil
}

func (h *hsReader) Read(p []byte) (int, error) {
	if h.leftInRe == 0 {
		if err := h.fill(recHdr); err != nil {
			return 0, err
		}
		h.leftInRe = recHdr + int(binary.BigEndian.Uint16(h.buf[h.r+3:]))
	}
	if err := h.fill(1); err != nil {
		return 0, err
	}
	n := copy(p[:min(len(p), h.leftInRe)], h.buf[h.r:h.w])
	h.r += n
	h.leftInRe -= n
	return n, nil
}

// handshake runs crypto/tls's handshake on nc, as the server or as the
// client, and hands the socket over: the returned fd is a non-blocking
// duplicate, and nc is closed.
func handshake(nc net.Conn, cfg *tls.Config, server bool) (*handoff, error) {
	cfg = cfg.Clone()
	keys := &keyLog{}
	cfg.KeyLogWriter = keys
	cfg.SessionTicketsDisabled = true
	cfg.MinVersion = tls.VersionTLS13
	if get := cfg.GetConfigForClient; get != nil {
		cfg.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			c, err := get(hi)
			if c == nil || err != nil {
				return c, err
			}
			c = c.Clone()
			c.KeyLogWriter, c.SessionTicketsDisabled, c.MinVersion = keys, true, tls.VersionTLS13
			return c, nil
		}
	}
	hr := &hsReader{Conn: nc, buf: make([]byte, 64<<10)}
	var tc *tls.Conn
	if server {
		tc = tls.Server(hr, cfg)
	} else {
		tc = tls.Client(hr, cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = nc.Close()
		return nil, err
	}
	st := tc.ConnectionState()
	if keys.client == nil || keys.server == nil {
		_ = nc.Close()
		return nil, errNoSecrets
	}
	if hr.leftInRe != 0 {
		_ = nc.Close()
		return nil, errRecord
	}
	h := &handoff{alpn: st.NegotiatedProtocol}
	if len(st.PeerCertificates) > 0 {
		h.cn = st.PeerCertificates[0].Subject.CommonName
	}
	rdSecret, wrSecret := keys.client, keys.server
	if !server {
		rdSecret, wrSecret = keys.server, keys.client
	}
	var err error
	if h.rd, err = newDir(st.CipherSuite, rdSecret); err != nil {
		_ = nc.Close()
		return nil, err
	}
	if h.wr, err = newDir(st.CipherSuite, wrSecret); err != nil {
		_ = nc.Close()
		return nil, err
	}
	h.left = append([]byte(nil), hr.buf[hr.r:hr.w]...)
	fd, err := takeFD(nc)
	if err != nil {
		return nil, err
	}
	h.fd = fd
	return h, nil
}

// takeFD duplicates nc's socket as a non-blocking fd and closes nc.
func takeFD(nc net.Conn) (int, error) {
	sc, ok := nc.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		_ = nc.Close()
		return -1, errors.New("not a socket")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		_ = nc.Close()
		return -1, err
	}
	nfd := -1
	var derr error
	if err := raw.Control(func(fd uintptr) { nfd, derr = unix.Dup(int(fd)) }); err != nil {
		_ = nc.Close()
		return -1, err
	}
	_ = nc.Close()
	if derr != nil {
		return -1, derr
	}
	if err := unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
		return -1, err
	}
	_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	return nfd, nil
}
