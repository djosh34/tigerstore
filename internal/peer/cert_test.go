package peer

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/testcert"
)

func TestVerifyPeer(t *testing.T) {
	leaf := func(cn string, dns []string, ips ...net.IP) [][]*x509.Certificate {
		return [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: cn}, DNSNames: dns, IPAddresses: ips}}}
	}
	cases := []struct {
		name           string
		cns, hostnames []string
		chains         [][]*x509.Certificate
		ok             bool
	}{
		{"CN in the list", []string{"node1", "node2"}, nil, leaf("node2", nil), true},
		{"CN not in the list", []string{"node1", "node2"}, nil, leaf("node3", []string{"node1"}), false},
		{"CN check ignores SANs", []string{"node1"}, nil, leaf("", []string{"node1"}), false},
		{"no verified chain", []string{"node1"}, nil, nil, false},
		{"SAN DNS name", nil, []string{"a.example", "b.example"}, leaf("x", []string{"b.example"}), true},
		{"SAN wildcard subdomain", nil, []string{"db.b.example"}, leaf("x", []string{"*.b.example"}), true},
		{"SAN IP", nil, []string{"10.0.0.1"}, leaf("x", nil, net.ParseIP("10.0.0.1")), true},
		{"hostname only in CN", nil, []string{"a.example"}, leaf("a.example", nil), false},
		{"hostname not in SANs", nil, []string{"a.example"}, leaf("x", []string{"c.example"}, net.ParseIP("10.0.0.1")), false},
	}
	for _, c := range cases {
		verify, err := verifyPeer(c.cns, c.hostnames)
		if err != nil || verify == nil {
			t.Fatalf("%s: verify set = %v, err = %v", c.name, verify != nil, err)
		}
		if err := verify(nil, c.chains); (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok = %v", c.name, err, c.ok)
		}
	}

	if verify, err := verifyPeer(nil, nil); verify != nil || err != nil {
		t.Errorf("no lists: verify set = %v, err = %v; want no check", verify != nil, err)
	}
	if _, err := verifyPeer([]string{"node1"}, []string{"a.example"}); err == nil {
		t.Error("both lists set: no error")
	}
	if _, err := Start(Config{AllowedCNs: []string{"node1"}, AllowedHostnames: []string{"a.example"}}); err == nil {
		t.Error("Start with both lists set: no error")
	}
}

// delivers starts node 1 with c1 and node 2 with c2, both trusting ca,
// and reports whether a message from node 1 reaches node 2 within wait.
// c1 and c2 carry the certificate and the allowed lists.
func delivers(t *testing.T, ca *x509.CertPool, c1, c2 Config, wait time.Duration) bool {
	t.Helper()
	addrs, lns := listeners(t, 1, 2)
	got := make(chan *raftpb.Message, 1000)
	var trs []*TCP
	defer func() {
		for _, tr := range trs {
			if err := tr.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for i, c := range []Config{c1, c2} {
		id, other := uint64(i+1), uint64(2-i)
		c.ID, c.Listeners, c.Peers, c.CA = id, []net.Listener{lns[id]}, map[uint64]string{other: addrs[other]}, ca
		c.Deliver = func(m *raftpb.Message) { got <- m }
		c.Unreachable = func(uint64) {}
		c.Logf = t.Logf
		tr, err := Start(c)
		if err != nil {
			t.Fatal(err)
		}
		trs = append(trs, tr)
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		trs[0].Send(msg(1, "x"))
		select {
		case <-got:
			return true
		case <-time.After(20 * time.Millisecond):
		}
	}
	return false
}

func TestAllowedCertificates(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	// Both certificates are valid for 127.0.0.1 and localhost.
	certs := map[string]tls.Certificate{}
	for i, cn := range []string{"node1", "node2"} {
		c, _, _, err := ca.Issue(cn, int64(i+2))
		if err != nil {
			t.Fatal(err)
		}
		certs[cn] = c
	}
	node := func(cert string, cns, hostnames []string) Config {
		return Config{Certificate: certs[cert], AllowedCNs: cns, AllowedHostnames: hostnames}
	}
	cases := []struct {
		name   string
		c1, c2 Config
		ok     bool
	}{
		{"no lists", node("node1", nil, nil), node("node2", nil, nil), true},
		{"CNs allowed on both ends", node("node1", []string{"node2"}, nil), node("node2", []string{"node1", "node3"}, nil), true},
		{"acceptor refuses the CN", node("node1", nil, nil), node("node2", []string{"node3"}, nil), false},
		{"dialer refuses the CN", node("node1", []string{"node3"}, nil), node("node2", nil, nil), false},
		{"hostnames allowed on both ends", node("node1", nil, []string{"localhost"}), node("node2", nil, []string{"127.0.0.1"}), true},
		{"acceptor refuses the hostname", node("node1", nil, nil), node("node2", nil, []string{"db.example"}), false},
		{"dialer refuses the hostname", node("node1", nil, []string{"db.example"}), node("node2", nil, nil), false},
	}
	for _, c := range cases {
		// A refusal is checked for one second; a delivery usually takes
		// milliseconds but gets longer on a loaded machine.
		wait := time.Second
		if c.ok {
			wait = 10 * time.Second
		}
		if got := delivers(t, ca.Pool, c.c1, c.c2, wait); got != c.ok {
			t.Errorf("%s: delivered = %v, want %v", c.name, got, c.ok)
		}
	}
}
