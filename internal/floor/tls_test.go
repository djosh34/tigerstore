package floor

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

type memConn struct {
	net.Conn
	in, out *bytes.Buffer
	memory  bool
}

func (c *memConn) Read(p []byte) (int, error) {
	if c.memory {
		return c.in.Read(p)
	}
	return c.Conn.Read(p)
}
func (c *memConn) Write(p []byte) (int, error) {
	if c.memory {
		return c.out.Write(p)
	}
	return c.Conn.Write(p)
}
func BenchmarkFloorTLS(b *testing.B) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		b.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	for _, n := range []int{80, 1103, 1221, 16384} {
		b.Run(fmt.Sprintf("seal_open/n%d", n), func(b *testing.B) {
			a, z := net.Pipe()
			defer a.Close()
			defer z.Close()
			ab, ba := new(bytes.Buffer), new(bytes.Buffer)
			mc := &memConn{Conn: a, in: ba, out: ab}
			ms := &memConn{Conn: z, in: ab, out: ba}
			c := tls.Client(mc, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13, DynamicRecordSizingDisabled: true})
			s := tls.Server(ms, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13, DynamicRecordSizingDisabled: true})
			done := make(chan error, 1)
			go func() { done <- s.Handshake() }()
			if err := c.Handshake(); err != nil {
				b.Fatal(err)
			}
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			if c.ConnectionState().CipherSuite != tls.TLS_AES_128_GCM_SHA256 {
				b.Fatalf("unexpected cipher %x", c.ConnectionState().CipherSuite)
			}
			mc.memory, ms.memory = true, true
			p, out := make([]byte, n), make([]byte, n)
			b.ResetTimer()
			for b.Loop() {
				if _, err := c.Write(p); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(s, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
