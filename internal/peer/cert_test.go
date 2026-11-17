package peer

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"testing"
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
		verify, err := VerifyPeer(c.cns, c.hostnames)
		if err != nil || verify == nil {
			t.Fatalf("%s: verify set = %v, err = %v", c.name, verify != nil, err)
		}
		if err := verify(nil, c.chains); (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok = %v", c.name, err, c.ok)
		}
	}

	if verify, err := VerifyPeer(nil, nil); verify != nil || err != nil {
		t.Errorf("no lists: verify set = %v, err = %v; want no check", verify != nil, err)
	}
	if _, err := VerifyPeer([]string{"node1"}, []string{"a.example"}); err == nil {
		t.Error("both lists set: no error")
	}
}
