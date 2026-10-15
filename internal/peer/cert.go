package peer

import (
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
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
