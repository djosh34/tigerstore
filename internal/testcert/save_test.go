package testcert

import (
	"bytes"
	"crypto/x509"
	"testing"
	"time"
)

// A saved CA and certificate load as they were: the loaded certificate
// verifies against the loaded CA, and the loaded CA issues certificates
// the saved one's pool trusts (#794).
func TestASavedCALoadsAsItWas(t *testing.T) {
	ca, err := NewCAValid(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.IssueUntil("n", 2, nil, []string{"node1"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ca.Save()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCA(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded.PEM, ca.PEM) || !loaded.notAfter.Equal(ca.notAfter) {
		t.Fatal("the loaded CA has another certificate or expiry")
	}
	cb, err := SaveCertificate(cert)
	if err != nil {
		t.Fatal(err)
	}
	lc, err := LoadCertificate(cb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(lc.Certificate[0], cert.Certificate[0]) || lc.Leaf == nil {
		t.Fatal("the loaded certificate differs")
	}
	if _, err := lc.Leaf.Verify(x509.VerifyOptions{Roots: loaded.Pool, DNSName: "node1", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatal(err)
	}
	other, _, _, err := loaded.IssueUntil("m", 3, nil, []string{"node2"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Leaf.Verify(x509.VerifyOptions{Roots: ca.Pool, DNSName: "node2", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("the saved CA's pool does not trust what the loaded CA issued: %v", err)
	}
	for cut := range len(b) - 1 {
		if _, err := LoadCA(b[:cut]); err == nil {
			t.Fatalf("a CA cut at %d of %d bytes loaded", cut, len(b))
		}
	}
}
