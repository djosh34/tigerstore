package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/testcert"
)

// parseArgs parses a command line as run does.
func parseArgs(t *testing.T, args ...string) (*config, error) {
	t.Helper()
	c := newConfig(io.Discard)
	return c, c.parse(args, io.Discard)
}

func TestPeerCertAllowedFlags(t *testing.T) {
	c, err := parseArgs(t, "--peer-cert-allowed-cn", "old", "--peer-cert-allowed-cn", "node1,node2", "--peer-cert-allowed-hostname", "a.example,10.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.ec.PeerTLSInfo.AllowedCNs, []string{"node1", "node2"}; !slices.Equal(got, want) {
		t.Errorf("allowed CNs = %q, want %q", got, want)
	}
	if got, want := c.ec.PeerTLSInfo.AllowedHostnames, []string{"a.example", "10.0.0.1"}; !slices.Equal(got, want) {
		t.Errorf("allowed hostnames = %q, want %q", got, want)
	}
}

// TestRaftTimeoutsRaised checks that --raft-read-timeout and
// --raft-write-timeout below etcd's 5 s minimum are raised to it, as in
// etcd.
func TestRaftTimeoutsRaised(t *testing.T) {
	read, write := rafthttp.ConnReadTimeout, rafthttp.ConnWriteTimeout
	t.Cleanup(func() { rafthttp.ConnReadTimeout, rafthttp.ConnWriteTimeout = read, write })
	if _, err := parseArgs(t, "--raft-read-timeout", "1s", "--raft-write-timeout", "2s"); err != nil {
		t.Fatal(err)
	}
	if rafthttp.ConnReadTimeout != rafthttp.DefaultConnReadTimeout || rafthttp.ConnWriteTimeout != rafthttp.DefaultConnWriteTimeout {
		t.Errorf("read %v, write %v; want both raised to 5 s", rafthttp.ConnReadTimeout, rafthttp.ConnWriteTimeout)
	}
}

// TestEnvironment checks that ETCD_* variables set the flags not given
// on the command line. (etcd exits when both set one.)
func TestEnvironment(t *testing.T) {
	t.Setenv("ETCD_NAME", "fromenv")
	c, err := parseArgs(t, "--max-txn-ops", "9")
	if err != nil {
		t.Fatal(err)
	}
	if c.ec.Name != "fromenv" || c.ec.MaxTxnOps != 9 {
		t.Errorf("name %q, max-txn-ops %d; want fromenv and 9", c.ec.Name, c.ec.MaxTxnOps)
	}
}

// TestConfigFile checks that a configuration file sets etcd's keys and
// tigerstore's, and that the command line's other flags are ignored.
func TestConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "etcd.yaml")
	if err := os.WriteFile(path, []byte("name: fromfile\nmax-txn-ops: 5\ndisk-budget-bytes: 1000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := parseArgs(t, "--config-file", path, "--max-txn-ops", "9", "--memory-budget-bytes", "7")
	if err != nil {
		t.Fatal(err)
	}
	if c.ec.Name != "fromfile" || c.ec.MaxTxnOps != 5 || c.own.DiskBudget != 1000 || c.own.MemoryBudget != 0 {
		t.Errorf("name %q, max-txn-ops %d, disk budget %d, memory budget %d; want fromfile, 5, 1000 and 0",
			c.ec.Name, c.ec.MaxTxnOps, c.own.DiskBudget, c.own.MemoryBudget)
	}
	t.Setenv("ETCD_CONFIG_FILE", path)
	if c, err = parseArgs(t); err != nil || c.ec.Name != "fromfile" {
		t.Errorf("ETCD_CONFIG_FILE: name %q, error %v", c.ec.Name, err)
	}
}

func TestVersionAndHelp(t *testing.T) {
	var out bytes.Buffer
	if err := newConfig(io.Discard).parse([]string{"--version"}, &out); !errors.Is(err, errHelp) {
		t.Fatalf("--version: %v", err)
	}
	if !strings.HasPrefix(out.String(), "tigerstore Version: dev\netcd Version: 3.7.2\nGit SHA: ") {
		t.Errorf("--version printed %q", out.String())
	}
	out.Reset()
	if err := newConfig(io.Discard).parse([]string{"--help"}, &out); !errors.Is(err, errHelp) || !strings.Contains(out.String(), "-listen-client-urls") {
		t.Errorf("--help: %v, printed %q", err, out.String())
	}
}

func TestBadCommandLines(t *testing.T) {
	if _, err := parseArgs(t, "--no-such-flag"); !errors.Is(err, errUsage) {
		t.Errorf("unknown flag: %v", err)
	}
	if _, err := parseArgs(t, "extra"); err == nil || err.Error() != `"extra" is not a valid flag` {
		t.Errorf("positional argument: %v", err)
	}
	if _, err := parseArgs(t, "--listen-client-urls", "http://127.0.0.1:2379"); !errors.Is(err, embed.ErrUnsetAdvertiseClientURLsFlag) {
		t.Errorf("listen-client-urls without advertise-client-urls: %v", err)
	}
	if _, err := parseArgs(t, "--discovery-fallback", "proxy"); err == nil {
		t.Error("--discovery-fallback proxy: no error")
	}
	// etcd v2's flags are accepted and ignored.
	if _, err := parseArgs(t, "--snapshot", "--peer-heartbeat-interval=5"); err != nil {
		t.Errorf("ignored flags: %v", err)
	}
}

// TestParseCluster checks that a name given twice gets both URLs and
// keeps the place of its first entry.
func TestParseCluster(t *testing.T) {
	ms, err := parseCluster("a=https://h1:2380,b=https://h2:2380,a=https://h3:2380")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].id != 1 || ms[0].name != "a" || !slices.Equal(ms[0].urls, []string{"https://h1:2380", "https://h3:2380"}) || ms[1].id != 2 || ms[1].name != "b" {
		t.Errorf("members %+v", ms)
	}
	if _, err := parseCluster("a=h1:2380"); err == nil {
		t.Error("a URL without a scheme: no error")
	}
}

func TestClientListeners(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "client.sock")
	c, err := parseArgs(t, "--listen-client-urls", "http://127.0.0.1:0,unix://"+sock, "--advertise-client-urls", "http://127.0.0.1:2379")
	if err != nil {
		t.Fatal(err)
	}
	lns, err := clientListeners(&c.ec, false, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lns {
		if l.Traffic != server.TrafficAll || l.TLS {
			t.Errorf("%s: traffic %v, TLS %v", l.url, l.Traffic, l.TLS)
		}
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	}
	if len(lns) != 2 || lns[1].Addr().Network() != "unix" {
		t.Errorf("%d listeners", len(lns))
	}

	c, err = parseArgs(t, "--listen-client-urls", "http://127.0.0.1:0", "--listen-client-http-urls", "http://127.0.0.1:0", "--advertise-client-urls", "http://127.0.0.1:2379")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientListeners(&c.ec, false, zap.NewNop()); !errors.Is(err, errBindBoth) {
		t.Errorf("the same URL in both flags: %v", err)
	}
	c, err = parseArgs(t, "--listen-client-urls", "https://127.0.0.1:0", "--advertise-client-urls", "https://127.0.0.1:2379")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientListeners(&c.ec, false, zap.NewNop()); !errors.Is(err, errNoClientTLS) {
		t.Errorf("https without a certificate: %v", err)
	}
}

// TestCheckPeerURLs checks that peer URLs must be https.
func TestCheckPeerURLs(t *testing.T) {
	c, err := parseArgs(t)
	if err != nil {
		t.Fatal(err)
	}
	ms, err := parseCluster(c.ec.InitialCluster)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPeerURLs(&c.ec, initialMembers(ms)); err == nil {
		t.Error("etcd's default http peer URLs: no error")
	}
}

// TestTLSFlags checks that the client TLS configuration takes etcd's
// --cipher-suites and --tls-*-version and asks for client certificates
// with --trusted-ca-file alone, and that the peer flags refuse TLS 1.2
// and a client certificate without its key.
func TestTLSFlags(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	_, certPEM, keyPEM, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"ca.pem": ca.PEM, "node.pem": certPEM, "node-key.pem": keyPEM}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := func(name string) string { return filepath.Join(dir, name) }
	certs := []string{
		"--cert-file", f("node.pem"), "--key-file", f("node-key.pem"), "--trusted-ca-file", f("ca.pem"),
		"--peer-cert-file", f("node.pem"), "--peer-key-file", f("node-key.pem"), "--peer-trusted-ca-file", f("ca.pem"),
		"--listen-peer-urls", "https://127.0.0.1:0",
	}
	c, err := parseArgs(t, append(certs, "--cipher-suites", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", "--tls-max-version", "TLS1.2")...)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := clientTLS(&c.ec)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.CipherSuites, []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}) || cfg.MinVersion != tls.VersionTLS12 || cfg.MaxVersion != tls.VersionTLS12 || cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("client TLS: suites %v, versions %x-%x, client auth %v", cfg.CipherSuites, cfg.MinVersion, cfg.MaxVersion, cfg.ClientAuth)
	}
	if _, err := peerConfig(&c.ec, false); !errors.Is(err, errPeerTLS13) {
		t.Errorf("peers with --tls-max-version TLS1.2: %v", err)
	}

	if c, err = parseArgs(t, append(certs, "--peer-client-cert-file", f("node.pem"))...); err != nil {
		t.Fatal(err)
	}
	if _, err := peerConfig(&c.ec, false); err == nil {
		t.Error("--peer-client-cert-file without --peer-client-key-file: no error")
	}
	if c, err = parseArgs(t, append(certs, "--peer-client-cert-file", f("node.pem"), "--peer-client-key-file", f("node-key.pem"), "--peer-crl-file", f("crl"))...); err != nil {
		t.Fatal(err)
	}
	pcfg, err := peerConfig(&c.ec, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := closeAll(pcfg.Listeners); err != nil {
		t.Error(err)
	}
	if pcfg.GetClientCertificate == nil || pcfg.CRLFile != f("crl") || !pcfg.CheckClientSAN {
		t.Errorf("peer config: client certificate %v, CRL %q, SAN check %v", pcfg.GetClientCertificate != nil, pcfg.CRLFile, pcfg.CheckClientSAN)
	}

	if c, err = parseArgs(t, append(certs, "--cipher-suites", "NO_SUCH_SUITE")...); err != nil {
		t.Fatal(err)
	}
	if _, err := clientTLS(&c.ec); err == nil {
		t.Error("an unknown cipher suite: no error")
	}
}

// TestSRVCluster checks that two nodes that read the same SRV records,
// each with its own name and numbers for the others, as etcd names them,
// get the same members in the same order and the same cluster key.
func TestSRVCluster(t *testing.T) {
	a, err := types.NewURLsMap("a=https://h2:2380,0=https://h1:2380,1=https://h3:2380")
	if err != nil {
		t.Fatal(err)
	}
	b, err := types.NewURLsMap("0=https://h3:2380,b=https://h1:2380,1=https://h2:2380")
	if err != nil {
		t.Fatal(err)
	}
	sa, sb := srvCluster(a, "a", "tok"), srvCluster(b, "b", "tok")
	if !reflect.DeepEqual(sa.members, sb.members) || sa.key != sb.key {
		t.Fatalf("different clusters:\n%+v %q\n%+v %q", sa.members, sa.key, sb.members, sb.key)
	}
	if sa.self != 1 || sb.self != 0 || sa.members[1].urls[0] != "https://h2:2380" || sa.members[0].name != "" {
		t.Errorf("members %+v, self %d and %d", sa.members, sa.self, sb.self)
	}
}

// TestIdentityKeepsMembers saves the identity of a new cluster's node and
// reads it back with its members, so that a restart needs neither
// --initial-cluster nor discovery.
func TestIdentityKeepsMembers(t *testing.T) {
	dir := t.TempDir()
	if _, started, err := readIdentity(dir); err != nil || started {
		t.Fatalf("an empty data directory: started %v, %v", started, err)
	}
	ms, err := parseCluster("a=https://h1:2380,b=https://h2:2380,b=https://h2b:2380")
	if err != nil {
		t.Fatal(err)
	}
	want := clusterIdentity(clusterSpec{members: ms, self: 1, token: "tok", key: "k"})
	if err := saveIdentity(dir, want); err != nil {
		t.Fatal(err)
	}
	got, started, err := readIdentity(dir)
	if err != nil || !started {
		t.Fatalf("started %v, %v", started, err)
	}
	if got.id != 2 || got.cluster != want.cluster || len(got.members) != 2 {
		t.Fatalf("identity %+v, want %+v", got, want)
	}
	for i, m := range got.members {
		if !proto.Equal(m, want.members[i]) {
			t.Errorf("member %d: %v, want %v", i, m, want.members[i])
		}
	}
}

// TestJoinComesFromIdentity checks that whether a node joined is kept in
// its identity and read from there on every start, whatever this start's
// --initial-cluster-state says: a joiner restarted with "new" must not
// repair a torn bootstrap with its own members, and a first member
// restarted with "existing" must (#280).
func TestJoinComesFromIdentity(t *testing.T) {
	ms, err := parseCluster("a=https://h1:2380,b=https://h2:2380")
	if err != nil {
		t.Fatal(err)
	}
	first := clusterIdentity(clusterSpec{members: ms, self: 0, token: "tok", key: "k"})
	joiner := identity{id: 9, cluster: first.cluster, members: first.members}
	for name, want := range map[string]identity{"first member": first, "joiner": joiner} {
		dir := t.TempDir()
		if err := saveIdentity(dir, want); err != nil {
			t.Fatal(err)
		}
		got, _, err := readIdentity(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range []string{embed.ClusterStateFlagNew, embed.ClusterStateFlagExisting} {
			ec := embed.NewConfig()
			ec.ClusterState = state
			if join := coreConfig(ec, got).Join; join != !want.first {
				t.Errorf("%s restarted with %s: Join %v, want %v", name, state, join, !want.first)
			}
		}
	}
}

// TestMaxRequestBytesCeiling checks that a --max-request-bytes whose
// requests would not fit in a peer frame and a log record with their
// encoding is refused, on the command line and in a configuration file,
// and that the ceiling itself is taken.
func TestMaxRequestBytesCeiling(t *testing.T) {
	if _, err := parseArgs(t, "--max-request-bytes", strconv.Itoa(core.MaxRequestBytesCeiling)); err != nil {
		t.Fatalf("--max-request-bytes at the ceiling: %v", err)
	}
	for _, n := range []int{core.MaxRequestBytesCeiling + 1, 64 << 20} {
		if _, err := parseArgs(t, "--max-request-bytes", strconv.Itoa(n)); !errors.Is(err, errMaxRequestBytes) {
			t.Errorf("--max-request-bytes %d: %v", n, err)
		}
	}
	path := filepath.Join(t.TempDir(), "etcd.yaml")
	if err := os.WriteFile(path, []byte("max-request-bytes: 67108864\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseArgs(t, "--config-file", path); !errors.Is(err, errMaxRequestBytes) {
		t.Errorf("max-request-bytes in a configuration file: %v", err)
	}
}

// TestRequestTimeout checks etcd's request timeout, 5 s plus two election
// timeouts, which the health checks and every request wait for, rounded
// up to whole ticks.
func TestRequestTimeout(t *testing.T) {
	for _, tc := range []struct {
		heartbeat, election string
		want                time.Duration
	}{
		{"100", "1000", 7 * time.Second},
		{"100", "1500", 8 * time.Second},
		{"150", "1500", 8100 * time.Millisecond},
	} {
		c, err := parseArgs(t, "--heartbeat-interval", tc.heartbeat, "--election-timeout", tc.election)
		if err != nil {
			t.Fatal(err)
		}
		cfg := coreConfig(&c.ec, identity{})
		if got := time.Duration(cfg.RequestTicks) * time.Duration(cfg.TickMillis) * time.Millisecond; got != tc.want {
			t.Errorf("heartbeat %s ms, election %s ms: the request timeout is %v, want %v", tc.heartbeat, tc.election, got, tc.want)
		}
	}
}
