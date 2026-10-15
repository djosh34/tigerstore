// Package testcert makes a throwaway CA and certificates for tests. The
// certificates are valid for 127.0.0.1 and localhost, for server and
// client use.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// CA signs certificates.
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// Pool holds the CA certificate.
	Pool *x509.CertPool
	// PEM is the CA certificate in PEM form.
	PEM []byte
}

// NewCA makes a CA.
func NewCA() (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tigerstore test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{cert: cert, key: key, Pool: pool, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// Issue makes a certificate for name, valid as server and client for
// 127.0.0.1 and localhost. It returns the TLS certificate and its PEM
// certificate and key.
func (ca *CA) Issue(name string, serial int64) (tls.Certificate, []byte, []byte, error) {
	return ca.IssueFor(name, serial, []net.IP{net.IPv4(127, 0, 0, 1)}, []string{"localhost"})
}

// IssueFor is Issue with the SANs ips and dns.
func (ca *CA) IssueFor(name string, serial int64, ips []net.IP, dns []string) (tls.Certificate, []byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  ips,
		DNSNames:     dns,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	return c, certPEM, keyPEM, err
}

// CRL is a DER certificate revocation list of the CA, the form etcd
// reads, that revokes the certificates with the serials.
func (ca *CA) CRL(serials ...int64) ([]byte, error) {
	var revoked []x509.RevocationListEntry
	for _, s := range serials {
		revoked = append(revoked, x509.RevocationListEntry{SerialNumber: big.NewInt(s), RevocationTime: time.Now()})
	}
	return x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: time.Now(), NextUpdate: time.Now().Add(time.Hour), RevokedCertificateEntries: revoked,
	}, ca.cert, ca.key)
}
