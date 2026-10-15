package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/djosh34/tigerstore/internal/server"
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
	c, err := parseArgs(t, "--config-file", path, "--max-txn-ops", "9")
	if err != nil {
		t.Fatal(err)
	}
	if c.ec.Name != "fromfile" || c.ec.MaxTxnOps != 5 || c.own.DiskBudget != 1000 {
		t.Errorf("name %q, max-txn-ops %d, disk budget %d; want fromfile, 5 and 1000", c.ec.Name, c.ec.MaxTxnOps, c.own.DiskBudget)
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
	if !strings.HasPrefix(out.String(), "etcd Version: 3.7.2\nGit SHA: ") {
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
	if _, err := clientListeners(&c.ec, false, zap.NewNop()); err == nil || !strings.Contains(err.Error(), "cannot bind both") {
		t.Errorf("the same URL in both flags: %v", err)
	}
	c, err = parseArgs(t, "--listen-client-urls", "https://127.0.0.1:0", "--advertise-client-urls", "https://127.0.0.1:2379")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientListeners(&c.ec, false, zap.NewNop()); err == nil || !strings.Contains(err.Error(), "must be provided") {
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
	if err := checkPeerURLs(&c.ec, ms); err == nil {
		t.Error("etcd's default http peer URLs: no error")
	}
}
