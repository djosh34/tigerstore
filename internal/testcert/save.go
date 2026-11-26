package testcert

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"time"
)

// A CA and the certificates it issued can be saved and loaded again in
// another process, as the simulator's images keep theirs for the forks
// that run from them in runs of their own (#794).

// ErrSaved means that saved bytes do not parse.
var ErrSaved = errors.New("testcert: malformed saved certificate")

// Save returns the CA's certificate, key and expiry.
func (ca *CA) Save() ([]byte, error) {
	key, err := x509.MarshalECPrivateKey(ca.key)
	if err != nil {
		return nil, err
	}
	b := appendBytes(nil, ca.cert.Raw)
	b = appendBytes(b, key)
	return binary.AppendVarint(b, ca.notAfter.UnixNano()), nil
}

// LoadCA is the CA that Save returned b of.
func LoadCA(b []byte) (*CA, error) {
	der, b, err := cutBytes(b)
	if err != nil {
		return nil, err
	}
	keyDER, b, err := cutBytes(b)
	if err != nil {
		return nil, err
	}
	notAfter, n := binary.Varint(b)
	if n <= 0 || n != len(b) {
		return nil, fmt.Errorf("%w: the CA's expiry", ErrSaved)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	key, err := x509.ParseECPrivateKey(keyDER)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{cert: cert, key: key, notAfter: time.Unix(0, notAfter), Pool: pool, PEM: pemOf(der)}, nil
}

// SaveCertificate returns c's chain and its ECDSA key.
func SaveCertificate(c tls.Certificate) ([]byte, error) {
	key, ok := c.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("testcert: a certificate's key of %T", c.PrivateKey)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	b := binary.AppendUvarint(nil, uint64(len(c.Certificate)))
	for _, der := range c.Certificate {
		b = appendBytes(b, der)
	}
	return appendBytes(b, keyDER), nil
}

// LoadCertificate is the certificate that SaveCertificate returned b of.
func LoadCertificate(b []byte) (tls.Certificate, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > 16 {
		return tls.Certificate{}, fmt.Errorf("%w: the chain's length", ErrSaved)
	}
	b = b[k:]
	var chain []byte
	for range n {
		der, rest, err := cutBytes(b)
		if err != nil {
			return tls.Certificate{}, err
		}
		chain, b = append(chain, pemOf(der)...), rest
	}
	keyDER, rest, err := cutBytes(b)
	if err != nil {
		return tls.Certificate{}, err
	}
	if len(rest) != 0 {
		return tls.Certificate{}, fmt.Errorf("%w: bytes past the key", ErrSaved)
	}
	// As IssueUntil makes it.
	return tls.X509KeyPair(chain, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

// pemOf is the certificate der in PEM form.
func pemOf(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func cutBytes(b []byte) (p, rest []byte, err error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) {
		return nil, nil, fmt.Errorf("%w: a length of %d", ErrSaved, n)
	}
	return b[k : k+int(n)], b[k+int(n):], nil
}
