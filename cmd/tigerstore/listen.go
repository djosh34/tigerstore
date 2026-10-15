package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	cconfig "go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"

	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
)

// member is one name of --initial-cluster with its peer URLs, which
// etcd lets a name repeat to give it several. Raft IDs are the places of
// the names in the list, starting at 1, so every node must get the same
// list.
type member struct {
	id   uint64
	name string
	urls []string
}

// parseCluster parses --initial-cluster as etcd's types.NewURLsMap does,
// keeping the order of the names.
func parseCluster(s string) ([]member, error) {
	if _, err := types.NewURLsMap(s); err != nil {
		return nil, err
	}
	var out []member
	for part := range strings.SplitSeq(s, ",") {
		name, raw, _ := strings.Cut(part, "=")
		i := slices.IndexFunc(out, func(m member) bool { return m.name == name })
		if i < 0 {
			out = append(out, member{id: uint64(len(out) + 1), name: name})
			i = len(out) - 1
		}
		out[i].urls = append(out[i].urls, raw)
	}
	return out, nil
}

// freshDataDir reports whether the node has not started from the data
// directory before, so bootstraps or joins.
func freshDataDir(dir string) (bool, error) {
	_, err := os.Stat(identityFile(dir))
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	return false, err
}

// verifyBootstrap is etcd's check of a first start: the node's name is
// in --initial-cluster, its URLs there are --initial-advertise-peer-urls
// in a new cluster, and no URL is twice in it.
func verifyBootstrap(ec *embed.Config, lg *zap.Logger) error {
	urls, _, err := ec.PeerURLsMapAndToken("etcd")
	if err != nil {
		return fmt.Errorf("error setting up initial cluster: %w", err)
	}
	sc := &cconfig.ServerConfig{
		Name: ec.Name, InitialPeerURLsMap: urls, PeerURLs: ec.AdvertisePeerUrls, Logger: lg,
		DiscoveryCfg: ec.DiscoveryCfg,
	}
	if ec.IsNewCluster() {
		return sc.VerifyBootstrap()
	}
	return sc.VerifyJoinExisting()
}

// checkPeerURLs refuses peer URLs that are not https: unlike etcd,
// tigerstore always encrypts node-to-node traffic.
func checkPeerURLs(ec *embed.Config, members []member) error {
	urls := slices.Concat(ec.ListenPeerUrls, ec.AdvertisePeerUrls)
	for _, m := range members {
		for _, raw := range m.urls {
			u, err := url.Parse(raw)
			if err != nil {
				return err
			}
			urls = append(urls, *u)
		}
	}
	for _, u := range urls {
		if u.Scheme != "https" {
			return fmt.Errorf("peer URL %s must be https: node-to-node traffic is always encrypted", u.String())
		}
	}
	return nil
}

// peerConfig is the transport's configuration from the peer flags,
// with a listener for each --listen-peer-urls URL. The transport does
// TLS itself and always verifies both ends: against
// --peer-trusted-ca-file, else the system's roots, or not at all for
// the self-signed certificates of --peer-auto-tls.
func peerConfig(ec *embed.Config, selfSigned bool) (peer.Config, error) {
	info := ec.PeerTLSInfo
	if info.Empty() {
		return peer.Config{}, errors.New("peer TLS key/cert (--peer-cert-file, --peer-key-file) or --peer-auto-tls must be provided: node-to-node traffic is always encrypted")
	}
	cert, err := tls.LoadX509KeyPair(info.CertFile, info.KeyFile)
	if err != nil {
		return peer.Config{}, fmt.Errorf("peer certificate: %w", err)
	}
	cfg := peer.Config{
		Certificate: cert, InsecureSkipVerify: selfSigned,
		AllowedCNs: info.AllowedCNs, AllowedHostnames: info.AllowedHostnames,
		WriteTimeout: rafthttp.ConnWriteTimeout, ReadTimeout: rafthttp.ConnReadTimeout,
	}
	if cfg.CA, err = loadCA(info.TrustedCAFile); err != nil {
		return peer.Config{}, fmt.Errorf("peer CA: %w", err)
	}
	for _, u := range ec.ListenPeerUrls {
		ln, err := transport.NewListenerWithOpts(u.Host, u.Scheme, transport.WithSocketOpts(&ec.SocketOpts), transport.WithSkipTLSInfoCheck(true))
		if err != nil {
			return peer.Config{}, errors.Join(err, closeAll(cfg.Listeners))
		}
		cfg.Listeners = append(cfg.Listeners, ln)
	}
	return cfg, nil
}

// loadCA is the pool of a CA file's certificates, the system's without
// one.
func loadCA(file string) (*x509.CertPool, error) {
	if file == "" {
		return x509.SystemCertPool()
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("no certificates in %s", file)
	}
	return pool, nil
}

// clientTLS is the TLS configuration of the https and unixs client and
// metrics URLs, nil without a client certificate.
func clientTLS(ec *embed.Config) (*tls.Config, error) {
	info := ec.ClientTLSInfo
	if info.Empty() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(info.CertFile, info.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("client certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if info.ClientCertAuth {
		if cfg.ClientCAs, err = loadCA(info.TrustedCAFile); err != nil {
			return nil, err
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// clientListener is a listener of the client URLs, with the first URL
// that named its address.
type clientListener struct {
	server.Listener
	url *url.URL
}

// clientListeners are etcd's configureClientListeners: one listener per
// address of --listen-client-urls, serving gRPC and HTTP, and of
// --listen-client-http-urls, serving HTTP; with the latter set, the
// former serve gRPC only. Each listener serves TLS or not by its URL's
// scheme; tigerstore cannot serve both on one address, as etcd can.
func clientListeners(ec *embed.Config, haveTLS bool, lg *zap.Logger) (out []clientListener, err error) {
	defer func() {
		if err != nil {
			for _, l := range out {
				err = errors.Join(err, l.Close())
			}
			out = nil
		}
	}()
	for _, u := range slices.Concat(ec.ListenClientUrls, ec.ListenClientHttpUrls) {
		if u.Scheme == "http" || u.Scheme == "unix" {
			if !ec.ClientTLSInfo.Empty() {
				lg.Warn("scheme is http or unix while key and cert files are present; ignoring key and cert files", zap.String("client-url", u.String()))
			}
			if ec.ClientTLSInfo.ClientCertAuth {
				lg.Warn("scheme is http or unix while --client-cert-auth is enabled; ignoring client cert auth for this URL", zap.String("client-url", u.String()))
			}
		}
		if (u.Scheme == "https" || u.Scheme == "unixs") && !haveTLS {
			return nil, fmt.Errorf("TLS key/cert (--cert-file, --key-file) must be provided for client url %s with HTTPS scheme", u.String())
		}
	}
	grpcOnly := len(ec.ListenClientHttpUrls) > 0
	type spec struct {
		url     url.URL
		traffic server.Traffic
	}
	var specs []spec
	for _, u := range ec.ListenClientUrls {
		traffic := server.TrafficAll
		if grpcOnly {
			traffic = server.TrafficGRPC
		}
		specs = append(specs, spec{u, traffic})
	}
	for _, u := range ec.ListenClientHttpUrls {
		specs = append(specs, spec{u, server.TrafficHTTP})
	}
	addrs := map[string]int{}
	for _, sp := range specs {
		u := sp.url
		addr := u.Host
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			addr = u.Host + u.Path
		}
		secure := u.Scheme == "https" || u.Scheme == "unixs"
		if i, ok := addrs[addr]; ok {
			l := out[i]
			switch {
			case sp.traffic == server.TrafficHTTP && l.Traffic != server.TrafficHTTP:
				return out, fmt.Errorf("cannot bind both --listen-client-urls and --listen-client-http-urls on the same url %s", u.String())
			case secure != l.TLS:
				return out, fmt.Errorf("tigerstore cannot serve both http and https on %s", addr)
			}
			continue
		}
		ln, err := transport.NewListenerWithOpts(addr, u.Scheme, transport.WithSocketOpts(&ec.SocketOpts), transport.WithSkipTLSInfoCheck(true))
		if err != nil {
			return out, err
		}
		addrs[addr] = len(out)
		out = append(out, clientListener{server.Listener{Listener: ln, TLS: secure, Traffic: sp.traffic}, &u})
	}
	return out, nil
}

// trafficName names what a client listener serves in the log.
func trafficName(t server.Traffic) string {
	switch t {
	case server.TrafficAll:
		return "grpc+http"
	case server.TrafficGRPC:
		return "grpc"
	case server.TrafficHTTP:
		return "http"
	}
	return "unknown"
}

// serveMetricsURLs serves the metrics and health endpoints on each
// --listen-metrics-urls URL, as etcd does: over TLS with the client
// certificate for https.
func serveMetricsURLs(ec *embed.Config, ctls *tls.Config, h http.Handler, lg *zap.Logger) ([]*http.Server, error) {
	var srvs []*http.Server
	closeSrvs := func(err error) ([]*http.Server, error) {
		for _, srv := range srvs {
			err = errors.Join(err, srv.Close())
		}
		return nil, err
	}
	for _, u := range ec.ListenMetricsUrls {
		var cfg *tls.Config
		if u.Scheme == "https" || u.Scheme == "unixs" {
			if ctls == nil {
				return closeSrvs(errors.New("client TLS key/cert (--cert-file, --key-file) must be provided for metrics secure url"))
			}
			cfg = ctls.Clone()
			cfg.NextProtos = []string{"h2", "http/1.1"}
		}
		ln, err := transport.NewListenerWithOpts(u.Host, u.Scheme, transport.WithSocketOpts(&ec.SocketOpts), transport.WithSkipTLSInfoCheck(true))
		if err != nil {
			return closeSrvs(err)
		}
		if cfg != nil {
			ln = tls.NewListener(ln, cfg)
		}
		srv := &http.Server{Addr: u.Host, Handler: h, ReadHeaderTimeout: time.Minute}
		srvs = append(srvs, srv)
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				lg.Warn("serve metrics", zap.String("address", u.String()), zap.Error(err))
			}
		}()
		lg.Info("serving metrics", zap.String("address", u.String()))
	}
	return srvs, nil
}

// closeAll closes listeners that no server took.
func closeAll(lns []net.Listener) error {
	var err error
	for _, ln := range lns {
		err = errors.Join(err, ln.Close())
	}
	return err
}
