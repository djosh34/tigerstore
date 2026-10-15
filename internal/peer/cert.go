package peer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
)

// verifyPeer returns the check of etcd's --peer-cert-allowed-cn and
// --peer-cert-allowed-hostname, or nil when both lists are empty. TLS
// runs it after the certificate chain is verified against the peer CA,
// on both ends of a connection. The other node's certificate passes when
// its CommonName is in cns, or when it is valid for one of hostnames by
// its SANs. As in etcd, the two lists cannot both be set.
func verifyPeer(cns, hostnames []string) (func([][]byte, [][]*x509.Certificate) error, error) {
	if len(cns) > 0 && len(hostnames) > 0 {
		return nil, fmt.Errorf("peer: allowed CNs %q and allowed hostnames %q cannot both be set", cns, hostnames)
	}
	if len(cns) == 0 && len(hostnames) == 0 {
		return nil, nil
	}
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		for _, chain := range chains {
			if len(chain) == 0 {
				continue
			}
			leaf := chain[0]
			if slices.Contains(cns, leaf.Subject.CommonName) {
				return nil
			}
			for _, h := range hostnames {
				if leaf.VerifyHostname(h) == nil {
					return nil
				}
			}
		}
		if len(cns) > 0 {
			return errors.New("peer certificate's CommonName is not in --peer-cert-allowed-cn")
		}
		return errors.New("peer certificate is not valid for any name in --peer-cert-allowed-hostname")
	}, nil
}

// certificates are the functions that give the certificate a handshake
// presents to a peer that dials in and to a peer this node dials.
func (cfg *Config) certificates() (server, client func() (*tls.Certificate, error)) {
	server = cfg.GetCertificate
	if server == nil {
		cert := cfg.Certificate
		server = func() (*tls.Certificate, error) { return &cert, nil }
	}
	client = cfg.GetClientCertificate
	if client == nil {
		client = server
	}
	return server, client
}

// checkCRL is etcd's check against a certificate revocation list: no
// certificate of the chain may have a revoked serial number. It reads the
// file at every handshake, as etcd does.
func checkCRL(path string, certs []*x509.Certificate) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	crl, err := x509.ParseRevocationList(b)
	if err != nil {
		return err
	}
	for _, c := range certs {
		for _, r := range crl.RevokedCertificateEntries {
			if r.SerialNumber.Cmp(c.SerialNumber) == 0 {
				return fmt.Errorf("transport: certificate serial %x revoked", c.SerialNumber.Bytes())
			}
		}
	}
	return nil
}

// CheckCertSAN is etcd's check of a dialing client's certificate against
// its address: a certificate without SANs passes; one with IP SANs
// passes when one is the address; one with DNS SANs when a reverse
// lookup of the address gives one of them, or a name under a wildcard.
func CheckCertSAN(ctx context.Context, cert *x509.Certificate, remoteAddr string) error {
	if len(cert.IPAddresses) == 0 && len(cert.DNSNames) == 0 {
		return nil
	}
	h, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return err
	}
	if len(cert.IPAddresses) > 0 {
		err := cert.VerifyHostname(h)
		if err == nil || len(cert.DNSNames) == 0 {
			return err
		}
	}
	names, lerr := net.DefaultResolver.LookupAddr(ctx, h)
	for _, name := range names {
		name = strings.TrimSuffix(name, ".")
		for _, dns := range cert.DNSNames {
			if name == dns || strings.HasPrefix(dns, "*.") && strings.HasSuffix(name, dns[1:]) {
				return nil
			}
		}
	}
	msg := ""
	if lerr != nil {
		msg = " (" + lerr.Error() + ")"
	}
	return fmt.Errorf("tls: %q does not match any of DNSNames %q%s", h, cert.DNSNames, msg)
}
