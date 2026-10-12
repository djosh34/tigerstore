package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const deadline = "2026-10-13T22:00:00Z"

var sink uint64

func checkTime() error {
	d, err := time.Parse(time.RFC3339, deadline)
	if err != nil {
		return err
	}
	if !time.Now().Before(d) {
		return errors.New("deadline reached; refusing new work")
	}
	return nil
}
func cpuNS() int64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		fmt.Fprintln(os.Stderr, "getrusage:", err)
		os.Exit(1)
	}
	return r.Utime.Sec*1e9 + r.Utime.Usec*1000 + r.Stime.Sec*1e9 + r.Stime.Usec*1000
}
func emit(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }
func affinity() (map[string]string, error) {
	if runtime.GOMAXPROCS(0) != 1 {
		return nil, errors.New("GOMAXPROCS must equal 1")
	}
	paths, err := filepath.Glob("/proc/self/task/*/status")
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	expected := ""
	for _, p := range paths {
		b, e := os.ReadFile(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		found := false
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "Cpus_allowed_list:") {
				cpu := strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:"))
				if strings.ContainsAny(cpu, ",-") || cpu == "" {
					return nil, fmt.Errorf("non-singleton affinity %s", cpu)
				}
				if expected != "" && cpu != expected {
					return nil, errors.New("thread affinity mismatch")
				}
				expected = cpu
				m[filepath.Base(filepath.Dir(p))] = cpu
				found = true
			}
		}
		if !found {
			return nil, errors.New("missing affinity")
		}
	}
	if len(m) == 0 {
		return nil, errors.New("no thread affinity evidence")
	}
	return m, nil
}
func evidence(label string) error {
	a, err := affinity()
	if err != nil {
		return err
	}
	return emit(map[string]any{"kind": "affinity", "label": label, "pid": os.Getpid(), "threads": a, "gomaxprocs": runtime.GOMAXPROCS(0), "go": runtime.Version(), "arch": runtime.GOARCH})
}
func stats(samples []int64) map[string]float64 {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	r := map[string]float64{}
	if len(samples) == 0 {
		return r
	}
	for _, q := range []struct {
		name string
		q    float64
	}{{"p50_ns", .5}, {"p95_ns", .95}, {"p99_ns", .99}, {"max_ns", 1}} {
		i := int(q.q * float64(len(samples)-1))
		r[q.name] = float64(samples[i])
	}
	return r
}
func tlsConfigs(dir string) (*tls.Config, *tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		return nil, nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, "cert.pem"))
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, nil, errors.New("bad certificate")
	}
	verify := func(s tls.ConnectionState) error {
		if s.Version != tls.VersionTLS13 || s.CipherSuite != tls.TLS_AES_128_GCM_SHA256 {
			return fmt.Errorf("wrong TLS version/cipher: %x/%x", s.Version, s.CipherSuite)
		}
		return nil
	}
	client := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "localhost", VerifyConnection: verify, DynamicRecordSizingDisabled: true}
	server := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, VerifyConnection: verify, DynamicRecordSizingDisabled: true}
	return client, server, nil
}
func genCert(dir string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	t := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "pre50 experiment"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, t, t, &key.PublicKey, key)
	if err != nil {
		return err
	}
	kb, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), 0600)
}
func run() error {
	if err := checkTime(); err != nil {
		return err
	}
	if len(os.Args) < 2 {
		return errors.New("mode required")
	}
	if os.Args[1] == "cert" {
		return genCert(os.Args[2])
	}
	if err := evidence("start"); err != nil {
		return err
	}
	var err error
	switch os.Args[1] {
	case "local":
		err = local(os.Args[2])
	case "fsync":
		err = fsyncBench(os.Args[2])
	case "follower":
		err = follower(os.Args[2:])
	case "quorum":
		err = quorum(os.Args[2:])
	case "rpc-server":
		err = rpcServer(os.Args[2:])
	case "rpc-client":
		err = rpcClient(os.Args[2:])
	default:
		return errors.New("unknown mode")
	}
	if err != nil {
		return err
	}
	return evidence("end")
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
