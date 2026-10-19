// Command factory starts a fresh 3-node tigerstore cluster for one call of
// the k8s.io/apiserver storage tests' RunEtcd, through the adapter in
// apiservertest. It prints the client endpoints as one comma-separated
// line, keeps the cluster up until SIGINT or SIGTERM, then stops every
// node and exits 0 if all of them stopped cleanly. With ETCD set, it
// starts a 3-node etcd with the same certificates and layout instead, to
// compare the two on the same machine.
//
// Environment:
//
//	TIGERSTORE                      the tigerstore binary
//	ETCD                            an etcd binary to run instead
//	STORAGE_TEST_CERTS              certificates made by scripts/dev-certs.sh
//	STORAGE_TEST_DATA_DIR           a fresh directory for the nodes' data
//	STORAGE_TEST_PROGRESS_INTERVAL  the watch progress interval, 0s for the default (10 minutes)
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/internal/testport"
)

// stopTimeout is how long a tigerstore node may take to stop after
// SIGINT. As etcd's, a leader first gives its clients a second and then
// hands its leadership to a peer, which waits up to the request timeout
// of 7 s when the peers stop at the same time (#285). etcd gets
// etcdStopTimeout. The adapter waits 10 s for the factory.
const (
	stopTimeout     = 9 * time.Second
	etcdStopTimeout = 8 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "factory: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	bin, certs, dir := os.Getenv("TIGERSTORE"), os.Getenv("STORAGE_TEST_CERTS"), os.Getenv("STORAGE_TEST_DATA_DIR")
	etcd := os.Getenv("ETCD")
	if etcd != "" {
		bin = etcd
	}
	if bin == "" || certs == "" || dir == "" {
		return errors.New("set TIGERSTORE or ETCD, STORAGE_TEST_CERTS and STORAGE_TEST_DATA_DIR")
	}
	ports, err := freePorts(6)
	if err != nil {
		return err
	}
	var cluster, endpoints []string
	for i := range 3 {
		cluster = append(cluster, fmt.Sprintf("node%d=https://127.0.0.1:%d", i+1, ports[3+i]))
		endpoints = append(endpoints, fmt.Sprintf("https://127.0.0.1:%d", ports[i]))
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	exited := make(chan error, 3)
	var nodes []*exec.Cmd
	for i := range 3 {
		name := fmt.Sprintf("node%d", i+1)
		peer := cluster[i][len(name)+1:]
		args := []string{
			"--name", name, "--data-dir", filepath.Join(dir, name),
			"--initial-cluster", strings.Join(cluster, ","),
			"--listen-client-urls", endpoints[i], "--advertise-client-urls", endpoints[i],
			"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
			"--cert-file", filepath.Join(certs, name+".pem"), "--key-file", filepath.Join(certs, name+"-key.pem"),
			"--trusted-ca-file", filepath.Join(certs, "ca.pem"), "--client-cert-auth",
			"--peer-cert-file", filepath.Join(certs, name+".pem"), "--peer-key-file", filepath.Join(certs, name+"-key.pem"),
			"--peer-trusted-ca-file", filepath.Join(certs, "ca.pem"),
			// As the version's own test server sets UnsafeNoFsync: tests
			// such as TestCompact give a request 100 ms, which one fsync
			// on a shared runner can pass.
			"--unsafe-no-fsync",
		}
		if etcd != "" {
			// tigerstore always verifies peer certificates.
			args = append(args, "--peer-client-cert-auth", "--log-level", "error")
		}
		if iv := os.Getenv("STORAGE_TEST_PROGRESS_INTERVAL"); iv != "" {
			args = append(args, "--watch-progress-notify-interval", iv)
		}
		cmd := exec.Command(bin, args...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		// A node must not outlive a factory that is killed.
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		if err := cmd.Start(); err != nil {
			return errors.Join(err, stop(nodes, exited, len(nodes), etcd != ""))
		}
		nodes = append(nodes, cmd)
		go func() { exited <- cmd.Wait() }()
	}
	if _, err := fmt.Fprintln(os.Stdout, strings.Join(endpoints, ",")); err != nil {
		return errors.Join(err, stop(nodes, exited, len(nodes), etcd != ""))
	}
	select {
	case <-sigs:
		return stop(nodes, exited, len(nodes), etcd != "")
	case err := <-exited:
		// A node that stops on its own fails the test that uses it.
		return errors.Join(fmt.Errorf("a node stopped before the test ended: %w", err), stop(nodes, exited, len(nodes)-1, etcd != ""))
	}
}

// freePorts finds n free loopback ports below the ephemeral range
// (testport). Another test's node may still take one before tigerstore
// binds it; the test then fails to start.
func freePorts(n int) ([]int, error) {
	var ports []int
	var lns []net.Listener
	defer func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
	}()
	for range n {
		ln, port, err := testport.Listen()
		if err != nil {
			return nil, err
		}
		lns = append(lns, ln)
		ports = append(ports, port)
	}
	return ports, nil
}

// stop sends every node SIGINT and waits for the running ones to exit,
// killing all of them if that takes longer than stopTimeout. tigerstore
// must exit 0; etcd, which ends on SIGINT itself, may also exit by it.
func stop(nodes []*exec.Cmd, exited chan error, running int, etcd bool) error {
	limit := stopTimeout
	if etcd {
		limit = etcdStopTimeout
	}
	for _, cmd := range nodes {
		_ = cmd.Process.Signal(os.Interrupt) // fails only for a node that already exited
	}
	var errs []error
	timeout := time.After(limit)
	for range running {
		select {
		case err := <-exited:
			if err != nil && (!etcd || !interrupted(err)) {
				errs = append(errs, err)
			}
		case <-timeout:
			for _, cmd := range nodes {
				_ = cmd.Process.Kill()
			}
			return errors.Join(append(errs, fmt.Errorf("a node did not stop within %v", limit))...)
		}
	}
	return errors.Join(errs...)
}

// interrupted reports whether a node was ended by the SIGINT it was sent,
// as etcd is.
func interrupted(err error) bool {
	exit, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		return false
	}
	ws, ok := exit.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGINT
}
