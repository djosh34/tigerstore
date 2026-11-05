package testcert

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"
)

// These probes record existing behavior for the plan review, not fixes.
func TestPlan5ReviewClockJump(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("review", 2)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: ca.Pool, DNSName: "localhost", CurrentTime: time.Now().Add(25 * time.Hour)})
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.Expired {
		t.Fatalf("expected expiry after the jump, got %v", err)
	}
	t.Log("the simulator's certificate helper expires both CA and leaf after 24 hours")
}

func TestPlan5ReviewTLSCipher(t *testing.T) {
	ca, err := NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("review", 2)
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	server := tls.Server(a, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	client := tls.Client(b, &tls.Config{RootCAs: ca.Pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.HandshakeContext(ctx) }()
	if err := client.HandshakeContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Log(tls.CipherSuiteName(client.ConnectionState().CipherSuite))
}
