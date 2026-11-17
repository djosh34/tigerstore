package main

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// peerHandshake runs one handshake over loopback TCP between a node with
// dialer's peer settings and one with acceptor's, and reports whether
// both ends took it.
func peerHandshake(t *testing.T, dialer, acceptor peer.Config) bool {
	t.Helper()
	_, ccfg, err := wlPeerTLS(dialer)
	if err != nil {
		t.Fatal(err)
	}
	scfg, _, err := wlPeerTLS(acceptor)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer func() { _ = nc.Close() }()
		tc := tls.Server(nc, scfg)
		err = tc.Handshake()
		if err == nil {
			_, err = tc.Write([]byte{1})
		}
		accepted <- err
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ccfg = ccfg.Clone()
	ccfg.ServerName = "127.0.0.1"
	tc := tls.Client(nc, ccfg)
	derr := tc.Handshake()
	if derr == nil {
		// TLS 1.3's client ends its handshake before the server checks
		// the client's certificate: the server's byte, or its refusal,
		// tells.
		_, derr = tc.Read(make([]byte, 1))
	}
	_ = tc.Close()
	aerr := <-accepted
	return derr == nil && aerr == nil
}

func TestWLPeerTLSChecksTheOtherNode(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	// Both certificates are valid for 127.0.0.1 and localhost; node3's
	// is valid for another address only.
	certs := map[string]tls.Certificate{}
	for i, cn := range []string{"node1", "node2"} {
		if certs[cn], _, _, err = ca.Issue(cn, int64(i+2)); err != nil {
			t.Fatal(err)
		}
	}
	if certs["node3"], _, _, err = ca.IssueFor("node3", 4, []net.IP{net.ParseIP("10.9.9.9")}, nil); err != nil {
		t.Fatal(err)
	}
	crl, err := ca.CRL(2)
	if err != nil {
		t.Fatal(err)
	}
	crlFile := filepath.Join(t.TempDir(), "crl")
	if err := os.WriteFile(crlFile, crl, 0o600); err != nil {
		t.Fatal(err)
	}
	node := func(cert string, cns, hostnames []string) peer.Config {
		c := certs[cert]
		return peer.Config{
			GetCertificate: func() (*tls.Certificate, error) { return &c, nil },
			CA:             ca.Pool, AllowedCNs: cns, AllowedHostnames: hostnames,
		}
	}
	withCRL := node("node2", nil, nil)
	withCRL.CRLFile = crlFile
	withSAN := node("node2", nil, nil)
	withSAN.CheckClientSAN = true
	cases := []struct {
		name             string
		dialer, acceptor peer.Config
		ok               bool
	}{
		{"no lists", node("node1", nil, nil), node("node2", nil, nil), true},
		{"CNs allowed on both ends", node("node1", []string{"node2"}, nil), node("node2", []string{"node1", "node3"}, nil), true},
		{"acceptor refuses the CN", node("node1", nil, nil), node("node2", []string{"node3"}, nil), false},
		{"dialer refuses the CN", node("node1", []string{"node3"}, nil), node("node2", nil, nil), false},
		{"hostnames allowed on both ends", node("node1", nil, []string{"localhost"}), node("node2", nil, []string{"127.0.0.1"}), true},
		{"acceptor refuses the hostname", node("node1", nil, nil), node("node2", nil, []string{"db.example"}), false},
		{"dialer refuses the hostname", node("node1", nil, []string{"db.example"}), node("node2", nil, nil), false},
		{"acceptor revokes the dialer's certificate", node("node1", nil, nil), withCRL, false},
		{"acceptor's CRL leaves another certificate", node("node2", nil, nil), withCRL, true},
		{"dialer's certificate valid for its address", node("node1", nil, nil), withSAN, true},
		{"dialer's certificate not valid for its address", node("node3", nil, nil), withSAN, false},
	}
	for _, c := range cases {
		if got := peerHandshake(t, c.dialer, c.acceptor); got != c.ok {
			t.Errorf("%s: handshake taken = %v, want %v", c.name, got, c.ok)
		}
	}
	if _, _, err := wlPeerTLS(node("node1", []string{"node2"}, []string{"localhost"})); err == nil {
		t.Error("both allowed lists set: no error")
	}
}
