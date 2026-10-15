//go:build difftest

package difftest

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/net/http2"
)

// TestTLS runs on a server started with one cipher suite for TLS 1.2 and
// a CRL (scripts/difftest.sh).
func TestTLS(t *testing.T) {
	runCases(t, "etcd-v3.7.2-tls.json", []testCase{{"tls", tlsCase}, {"reload", certReload}})
}

// TestTLSStrict runs on a server started with --tls-min-version TLS1.3
// and --client-cert-allowed-hostname localhost.
func TestTLSStrict(t *testing.T) {
	runCases(t, "etcd-v3.7.2-tls-strict.json", []testCase{{"strict", func(r *run) {
		r.served("TLS 1.3 with a client certificate for localhost", r.tlsConfig("client", nil))
		r.served("TLS 1.2", r.tlsConfig("client", func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 }))
		r.served("a client certificate for other.example", r.tlsConfig("other", nil))
	}}})
}

// TestUnix runs on a server that also listens on the unix socket
// unixs://localhost:<client port>, which etcd and tigerstore make as the
// file localhost:<port> in the directory they run in.
func TestUnix(t *testing.T) {
	_, port, err := net.SplitHostPort(os.Getenv("TIGERSTORE_DIFF_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(os.Getenv("TIGERSTORE_DIFF_WORK"), "localhost:"+port)
	runCasesOn(t, "etcd-v3.7.2-unix.json", dialTarget(t, "unix://"+path, "localhost", os.Getenv("TIGERSTORE_DIFF_CA")), []testCase{{"unix", func(r *run) {
		r.put("put over the socket", &pb.PutRequest{Key: r.key("a"), Value: []byte("1")})
		r.get("get it back", &pb.RangeRequest{Key: r.key("a")})
		r.get("serializable get", &pb.RangeRequest{Key: r.key("a"), Serializable: true})
	}}})
}

// tlsCase tries the TLS settings the server takes and refuses.
func tlsCase(r *run) {
	r.served("TLS 1.3", r.tlsConfig("client", nil))
	r.served("TLS 1.2 with the allowed cipher suite", r.tlsConfig("client", func(c *tls.Config) { c.MaxVersion = tls.VersionTLS12 }))
	r.served("TLS 1.2 with another cipher suite", r.tlsConfig("client", func(c *tls.Config) {
		c.MaxVersion = tls.VersionTLS12
		c.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
	}))
	r.served("no client certificate", r.tlsConfig("", nil))
	r.served("a revoked client certificate named client", r.tlsConfig("revoked", nil))
}

// certReload puts node2's certificate and key where the server's are,
// which the server must use for the next connection, and back.
func certReload(r *run) {
	dir := os.Getenv("TIGERSTORE_DIFF_CERTS")
	swap := func(from string) {
		for _, f := range []string{"-key.pem", ".pem"} {
			b, err := os.ReadFile(filepath.Join(dir, from+f))
			if err != nil {
				r.t.Fatal(err)
			}
			// Write a new file and rename it over the old one, so the
			// server never reads half a file.
			tmp := filepath.Join(dir, "node1"+f+".tmp")
			if err := os.WriteFile(tmp, b, 0o600); err != nil {
				r.t.Fatal(err)
			}
			if err := os.Rename(tmp, filepath.Join(dir, "node1"+f)); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	for _, f := range []string{".pem", "-key.pem"} {
		b, err := os.ReadFile(filepath.Join(dir, "node1"+f))
		if err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "node1-saved"+f), b, 0o600); err != nil {
			r.t.Fatal(err)
		}
	}
	r.served("before the change", r.tlsConfig("client", nil))
	swap("node2")
	r.served("after node2's certificate replaced the server's", r.tlsConfig("client", nil))
	swap("node1-saved")
	r.served("after the server's came back", r.tlsConfig("client", nil))
}

// tlsConfig is a TLS configuration that trusts the CA and offers the
// certificate cert of TIGERSTORE_DIFF_CERTS, none if cert is empty,
// with change applied.
func (r *run) tlsConfig(cert string, change func(*tls.Config)) *tls.Config {
	r.t.Helper()
	dir := os.Getenv("TIGERSTORE_DIFF_CERTS")
	ca, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		r.t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	c := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(filepath.Join(dir, cert+".pem"), filepath.Join(dir, cert+"-key.pem"))
		if err != nil {
			r.t.Fatal(err)
		}
		c.Certificates = []tls.Certificate{pair}
	}
	if change != nil {
		change(c)
	}
	return c
}

// served records whether the endpoint answers GET /health over HTTP/1.1
// on a new connection with conf, and if so, the TLS version and the
// common name of the server's certificate. A refused handshake, a
// connection the server closes and an error status are not served.
func (r *run) served(do string, conf *tls.Config) {
	r.t.Helper()
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		TLSClientConfig: conf, DisableKeepAlives: true,
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}}
	res := struct {
		Served  bool   `json:"served"`
		Version string `json:"version,omitempty"`
		Server  string `json:"server,omitempty"`
	}{}
	resp, err := hc.Get("https://" + r.c.endpoint + "/health")
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		res.Served = resp.StatusCode == http.StatusOK
		if res.Served {
			res.Version = tls.VersionName(resp.TLS.Version)
			res.Server = resp.TLS.PeerCertificates[0].Subject.CommonName
		}
	}
	r.record(do, res, nil)
}

// keepalive pings a connection that has no streams, as a client whose
// keepalive is shorter than --grpc-keepalive-min-time does, and records
// how many pings the server took and the GOAWAY it ended with, if any.
func keepalive(r *run) {
	conf := r.c.tls.Clone()
	conf.NextProtos = []string{"h2"}
	conn, err := tls.Dial("tcp", r.c.endpoint, conf)
	if err != nil {
		r.t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test connection
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		r.t.Fatal(err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(); err != nil {
		r.t.Fatal(err)
	}
	res := struct {
		Pings  int    `json:"pings"`
		GoAway string `json:"goAway,omitempty"`
		Debug  string `json:"debug,omitempty"`
	}{}
	for res.Pings < 10 && res.GoAway == "" {
		res.Pings++
		if err := fr.WritePing(false, [8]byte{byte(res.Pings)}); err != nil {
			break
		}
		// Read until the ping's ack or a GOAWAY.
		for acked := false; !acked && res.GoAway == ""; {
			if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				r.t.Fatal(err)
			}
			f, err := fr.ReadFrame()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.t.Fatalf("ping %d: %v", res.Pings, err)
				}
				res.GoAway = "closed"
				break
			}
			switch f := f.(type) {
			case *http2.SettingsFrame:
				if !f.IsAck() {
					if err := fr.WriteSettingsAck(); err != nil {
						r.t.Fatal(err)
					}
				}
			case *http2.PingFrame:
				acked = f.IsAck() && f.Data[0] == byte(res.Pings)
			case *http2.GoAwayFrame:
				res.GoAway, res.Debug = f.ErrCode.String(), string(f.DebugData())
			}
		}
	}
	r.record("pings without a stream", res, nil)
}
