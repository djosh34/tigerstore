// Package peer is the TLS of the connections between nodes, from etcd's
// peer flags: the certificates each end shows and the checks of the
// other's (cert.go). internal/wl carries Raft's messages on it.
package peer

import (
	"crypto/tls"
	"crypto/x509"
	"net"
)

// Config is the peer side of a node: its listeners and its TLS.
type Config struct {
	// Listeners accept peers, those of etcd's --listen-peer-urls.
	Listeners []net.Listener
	// CA is the pool that verifies other nodes' certificates. Both ends
	// require TLS 1.3 and verify each other.
	CA *x509.CertPool
	// GetCertificate gives this node's peer certificate for each
	// handshake, and GetClientCertificate, if set, the one this node
	// dials with, as etcd reads its certificate files again for every
	// handshake and dials with --peer-client-cert-file.
	GetCertificate, GetClientCertificate func() (*tls.Certificate, error)
	// CRLFile is etcd's --peer-crl-file: a peer that dials with a
	// certificate it revokes is refused.
	CRLFile string
	// CheckClientSAN is etcd's check, unless
	// --peer-skip-client-san-verification, that the certificate of a
	// peer that dials in is valid for its address: by an IP SAN, or else
	// by a DNS SAN that a reverse lookup of the address gives.
	CheckClientSAN bool
	// InsecureSkipVerify takes any certificate of the other node, as
	// etcd's --peer-auto-tls does with its self-signed ones; CA is not
	// used then.
	InsecureSkipVerify bool
	// AllowedCNs and AllowedHostnames are etcd's --peer-cert-allowed-cn
	// and --peer-cert-allowed-hostname: when one is set, the other node's
	// certificate must also have one of these CommonNames, or be valid
	// for one of these hostnames. At most one of them may be set.
	AllowedCNs       []string
	AllowedHostnames []string
}
