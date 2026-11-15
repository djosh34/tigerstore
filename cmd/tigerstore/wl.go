package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/wl"
)

// useWL reports whether the node runs internal/wl, prototype 6 made safe
// (#602): one event loop with its own client stack, store and
// replication. TIGERSTORE_MAIN asks for main's node instead, until that
// is deleted.
func useWL() bool { return os.Getenv("TIGERSTORE_MAIN") == "" }

var errWLPlainClients = errors.New("the client URLs must be https")

// runWL runs internal/wl with the node's identity, listeners, TLS and
// memory budget until a signal or an error stops it.
func runWL(ec *embed.Config, ident identity, pcfg peer.Config, ctls *tls.Config, clientLns []clientListener, budget int64, lg *zap.Logger) error {
	cfg := wl.Config{
		Name: ec.Name, ID: ident.id, ClusterID: ident.cluster, DataDir: ec.Dir,
		ClientTLS: ctls, PeerListeners: pcfg.Listeners, MemoryBudget: budget,
		Logger: log.New(os.Stderr, "tigerstore-wl ", log.LstdFlags|log.Lmicroseconds),
	}
	for _, l := range clientLns {
		if !l.TLS {
			closeListeners(clientLns, pcfg.Listeners, lg)
			return errWLPlainClients
		}
		cfg.ClientListeners = append(cfg.ClientListeners, l.Listener.Listener)
	}
	for _, m := range ident.members {
		mem := wl.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs}
		if m.ID == ident.id {
			mem.ClientURLs = nil
			for _, u := range ec.AdvertiseClientUrls {
				mem.ClientURLs = append(mem.ClientURLs, u.String())
			}
		}
		cfg.Members = append(cfg.Members, mem)
	}
	cfg.PeerServerTLS, cfg.PeerClientTLS = wlPeerTLS(pcfg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return wl.Run(ctx, cfg)
}

// wlPeerTLS is the peer TLS of etcd's peer flags: both ends show their
// certificate and check the other's against the peer CA, or take any with
// --peer-auto-tls.
func wlPeerTLS(pcfg peer.Config) (server, client *tls.Config) {
	clientCert := pcfg.GetClientCertificate
	if clientCert == nil {
		clientCert = pcfg.GetCertificate
	}
	server = &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return pcfg.GetCertificate() },
		ClientCAs:      pcfg.CA,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS13,
	}
	client = &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return clientCert() },
		RootCAs:              pcfg.CA,
		InsecureSkipVerify:   pcfg.InsecureSkipVerify, //nolint:gosec // etcd's --peer-auto-tls
		MinVersion:           tls.VersionTLS13,
	}
	if pcfg.InsecureSkipVerify {
		server.ClientAuth = tls.RequireAnyClientCert
	}
	return server, client
}
