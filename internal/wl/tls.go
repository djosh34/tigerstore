package wl

// TLS after crypto/tls's handshake: 1.3 (RFC 8446, 5), and 1.2's AEAD
// suites for clients (RFC 5246, 6.2.3.3; RFC 7905), as etcd takes TLS 1.2
// by default. crypto/tls runs the handshake on a blocking connection in
// a goroutine; then the loop takes the socket and seals and opens the
// records itself, with the secrets crypto/tls logs: 1.3's traffic
// secrets, or 1.2's master secret, from which the key block comes with
// both randoms (the server's read from the ServerHello it wrote). Replies
// are written straight into the record plaintext of a connection's send
// buffer and sealed there in place.

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
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
	"time"

	"golang.org/x/crypto/chacha20poly1305"
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
	// tls12: a TLS 1.2 record, with no inner content type and the
	// sequence number and header in its additional data; explicit: its
	// 8-byte explicit nonce comes before the ciphertext (AES-GCM).
	tls12, explicit bool
	aad             [13]byte
}

// extra is what a record carries between its header and ciphertext: 1.2
// AES-GCM's explicit nonce.
func (d *aeadDir) extra() int {
	if d.explicit {
		return 8
	}
	return 0
}

// open12 opens a TLS 1.2 record of application data into dst: AES-GCM's
// nonce is the salt and the record's explicit nonce, ChaCha20's the IV and
// the sequence number.
func (d *aeadDir) open12(dst, body, hdr []byte) ([]byte, error) {
	seq := d.seq
	nonce := d.next()
	if d.explicit {
		if len(body) < 8+tagLen {
			return nil, errRecord
		}
		copy(nonce[4:], body[:8])
		body = body[8:]
	}
	if len(body) < tagLen {
		return nil, errRecord
	}
	return d.aead.Open(dst, nonce, body, d.aad12(seq, hdr, len(body)-tagLen))
}

// aad12 is TLS 1.2's additional data of record seq: the sequence number,
// the record's type and version, and the plaintext's length.
func (d *aeadDir) aad12(seq uint64, hdr []byte, n int) []byte {
	binary.BigEndian.PutUint64(d.aad[:], seq)
	copy(d.aad[8:11], hdr[:3])
	binary.BigEndian.PutUint16(d.aad[11:], uint16(n))
	return d.aad[:]
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
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		h, keyLen = sha256.New, 32
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
	aead, err := newAEAD(key, suite == tls.TLS_CHACHA20_POLY1305_SHA256)
	if err != nil {
		return aeadDir{}, err
	}
	d := aeadDir{aead: aead}
	copy(d.iv[:], iv)
	return d, nil
}

func newAEAD(key []byte, chacha bool) (cipher.AEAD, error) {
	if chacha {
		return chacha20poly1305.New(key)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newDirs12 is both directions of a TLS 1.2 connection with suite: the
// key block of the master secret and the randoms, from which the client
// writes with the first key and IV and the server with the second. The
// handshake's Finished took sequence number 0 in each.
func newDirs12(suite uint16, master, clientRandom, serverRandom []byte, server bool) (rd, wr aeadDir, err error) {
	h, keyLen, ivLen, chacha := sha256.New, 16, 4, false
	switch suite {
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:
		h, keyLen = sha512.New384, 32
	case tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:
		keyLen, ivLen, chacha = 32, 12, true
	default:
		return rd, wr, fmt.Errorf("cipher suite %s not supported", tls.CipherSuiteName(suite))
	}
	block := prf12(h, master, "key expansion", append(append([]byte(nil), serverRandom...), clientRandom...), 2*keyLen+2*ivLen)
	dir := func(key, iv []byte) (aeadDir, error) {
		aead, err := newAEAD(key, chacha)
		if err != nil {
			return aeadDir{}, err
		}
		d := aeadDir{aead: aead, seq: 1, tls12: true, explicit: !chacha}
		copy(d.iv[:], iv)
		return d, nil
	}
	cKey, sKey := block[:keyLen], block[keyLen:2*keyLen]
	cIV, sIV := block[2*keyLen:2*keyLen+ivLen], block[2*keyLen+ivLen:]
	if !server {
		cKey, sKey, cIV, sIV = sKey, cKey, sIV, cIV
	}
	if rd, err = dir(cKey, cIV); err != nil {
		return rd, wr, err
	}
	wr, err = dir(sKey, sIV)
	return rd, wr, err
}

// prf12 is TLS 1.2's PRF, P_hash with h (RFC 5246, 5).
func prf12(h func() hash.Hash, secret []byte, label string, seed []byte, n int) []byte {
	seed = append([]byte(label), seed...)
	mac := hmac.New(h, secret)
	a := seed
	var out []byte
	for len(out) < n {
		mac.Reset()
		mac.Write(a)
		a = mac.Sum(nil)
		mac.Reset()
		mac.Write(a)
		mac.Write(seed)
		out = mac.Sum(out)
	}
	return out[:n]
}

func expandLabel(h func() hash.Hash, secret []byte, label string, n int) ([]byte, error) {
	full := "tls13 " + label
	info := binary.BigEndian.AppendUint16(nil, uint16(n))
	info = append(info, byte(len(full)))
	info = append(info, full...)
	info = append(info, 0)
	return hkdf.Expand(h, secret, string(info), n)
}

type keyLog struct{ client, server, clientRandom, master []byte }

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
	case "CLIENT_RANDOM": // TLS 1.2: the client's random and the master secret
		r, err := hex.DecodeString(f[1])
		k.clientRandom = r
		if err != nil {
			return len(p), err
		}
		to = &k.master
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
	beat   bool   // a peer connection of heartbeats and votes (beatMark)
	addr   string // the peer address an incoming peer connection's hello names
	plain  bool   // a client of a listener without TLS: h2c (plainHandoff)
	cn     string
	pre    *sock    // an incoming peer connection's socket, its hello read
	h1     net.Conn // a client's connection that net/http serves, HTTP/1.1
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
	hello    []byte // the start of the first record written
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

// Write sends the handshake's records, and keeps the start of the first,
// the server's ServerHello, for its random.
func (h *hsReader) Write(p []byte) (int, error) {
	if k := min(len(p), serverHelloLen-len(h.hello)); k > 0 {
		h.hello = append(h.hello, p[:k]...)
	}
	return h.Conn.Write(p)
}

// serverHelloLen is a ServerHello record's length up to its random: the
// record header, the message's type and length, the version and the 32
// bytes of random.
const serverHelloLen = recHdr + 4 + 2 + 32

// serverRandom is the random of the ServerHello the server wrote.
func (h *hsReader) serverRandom() ([]byte, bool) {
	if len(h.hello) < serverHelloLen || h.hello[0] != typeHandshake || h.hello[recHdr] != 2 {
		return nil, false
	}
	return h.hello[serverHelloLen-32:], true
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

// handshake runs crypto/tls's handshake on nc, which is of kind: as the
// client for kindPeerOut, else as the server. It hands the socket over:
// the returned fd is a non-blocking duplicate, and nc is closed. A
// client of the client port that did not choose HTTP/2 keeps crypto/tls's
// connection instead, in h1.
func handshake(sys sysEnv, nc net.Conn, cfg *tls.Config, kind uint8) (*handoff, error) {
	server := kind != kindPeerOut
	cfg = cfg.Clone()
	keys := &keyLog{}
	cfg.KeyLogWriter = keys
	cfg.SessionTicketsDisabled = true
	// Clients may take TLS 1.2, as etcd's do unless --tls-min-version says
	// otherwise; peers take 1.3 only.
	minVersion := func(c *tls.Config) {
		if kind != kindClient {
			c.MinVersion = tls.VersionTLS13
		} else if c.MinVersion < tls.VersionTLS12 {
			c.MinVersion = tls.VersionTLS12
		}
	}
	minVersion(cfg)
	if get := cfg.GetConfigForClient; get != nil {
		cfg.GetConfigForClient = func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			c, err := get(hi)
			if c == nil || err != nil {
				return c, err
			}
			c = c.Clone()
			c.KeyLogWriter, c.SessionTicketsDisabled = keys, true
			minVersion(c)
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
	if kind == kindClient && st.NegotiatedProtocol != "h2" {
		return &handoff{alpn: st.NegotiatedProtocol, kind: kind, h1: tc}, nil
	}
	if hr.leftInRe != 0 {
		_ = nc.Close()
		return nil, errRecord
	}
	h := &handoff{alpn: st.NegotiatedProtocol, kind: kind}
	if len(st.PeerCertificates) > 0 {
		h.cn = st.PeerCertificates[0].Subject.CommonName
	}
	var err error
	if st.Version == tls.VersionTLS12 {
		// Only a client of the client port takes 1.2: the server here.
		random, ok := hr.serverRandom()
		if keys.master == nil || !ok {
			_ = nc.Close()
			return nil, errNoSecrets
		}
		if h.rd, h.wr, err = newDirs12(st.CipherSuite, keys.master, keys.clientRandom, random, server); err != nil {
			_ = nc.Close()
			return nil, err
		}
	} else {
		if keys.client == nil || keys.server == nil {
			_ = nc.Close()
			return nil, errNoSecrets
		}
		rdSecret, wrSecret := keys.client, keys.server
		if !server {
			rdSecret, wrSecret = keys.server, keys.client
		}
		if h.rd, err = newDir(st.CipherSuite, rdSecret); err != nil {
			_ = nc.Close()
			return nil, err
		}
		if h.wr, err = newDir(st.CipherSuite, wrSecret); err != nil {
			_ = nc.Close()
			return nil, err
		}
	}
	h.left = append([]byte(nil), hr.buf[hr.r:hr.w]...)
	fd, err := sys.connFD(nc)
	if err != nil {
		return nil, err
	}
	h.fd = fd
	return h, nil
}
