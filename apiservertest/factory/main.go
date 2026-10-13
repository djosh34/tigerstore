// Command factory starts a fresh 3-node tigerstore cluster for one call of
// the k8s.io/apiserver storage tests' RunEtcd, through the adapter in
// apiservertest. It prints the client endpoints as one comma-separated
// line, keeps the cluster up until SIGINT or SIGTERM, then stops every
// node and exits 0 if all of them stopped cleanly.
//
// Environment:
//
//	TIGERSTORE                      the tigerstore binary
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
)

// stopTimeout is how long a node may take to stop after SIGINT.
const stopTimeout = 4 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "factory: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	bin, certs, dir := os.Getenv("TIGERSTORE"), os.Getenv("STORAGE_TEST_CERTS"), os.Getenv("STORAGE_TEST_DATA_DIR")
	if bin == "" || certs == "" || dir == "" {
		return errors.New("set TIGERSTORE, STORAGE_TEST_CERTS and STORAGE_TEST_DATA_DIR")
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
		args := []string{
			"--name", name, "--data-dir", filepath.Join(dir, name),
			"--initial-cluster", strings.Join(cluster, ","),
			"--listen-client-urls", endpoints[i],
			"--cert-file", filepath.Join(certs, name+".pem"), "--key-file", filepath.Join(certs, name+"-key.pem"),
			"--trusted-ca-file", filepath.Join(certs, "ca.pem"), "--client-cert-auth",
			"--peer-cert-file", filepath.Join(certs, name+".pem"), "--peer-key-file", filepath.Join(certs, name+"-key.pem"),
			"--peer-trusted-ca-file", filepath.Join(certs, "ca.pem"),
		}
		if iv := os.Getenv("STORAGE_TEST_PROGRESS_INTERVAL"); iv != "" {
			args = append(args, "--watch-progress-notify-interval", iv)
		}
		cmd := exec.Command(bin, args...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		// A node must not outlive a factory that is killed.
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		if err := cmd.Start(); err != nil {
			return errors.Join(err, stop(nodes, exited, len(nodes)))
		}
		nodes = append(nodes, cmd)
		go func() { exited <- cmd.Wait() }()
	}
	if _, err := fmt.Fprintln(os.Stdout, strings.Join(endpoints, ",")); err != nil {
		return errors.Join(err, stop(nodes, exited, len(nodes)))
	}
	select {
	case <-sigs:
		return stop(nodes, exited, len(nodes))
	case err := <-exited:
		// A node that stops on its own fails the test that uses it.
		return errors.Join(fmt.Errorf("a node stopped before the test ended: %w", err), stop(nodes, exited, len(nodes)-1))
	}
}

// freePorts asks the kernel for n free loopback ports. Another process may
// take one before tigerstore binds it; the test then fails to start.
func freePorts(n int) ([]int, error) {
	var ports []int
	var lns []net.Listener
	defer func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
	}()
	for range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns = append(lns, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

// stop sends every node SIGINT and waits for the running ones to exit,
// killing all of them if that takes longer than stopTimeout.
func stop(nodes []*exec.Cmd, exited chan error, running int) error {
	for _, cmd := range nodes {
		_ = cmd.Process.Signal(os.Interrupt) // fails only for a node that already exited
	}
	var errs []error
	timeout := time.After(stopTimeout)
	for range running {
		select {
		case err := <-exited:
			errs = append(errs, err)
		case <-timeout:
			for _, cmd := range nodes {
				_ = cmd.Process.Kill()
			}
			return errors.Join(append(errs, fmt.Errorf("a node did not stop within %v", stopTimeout))...)
		}
	}
	return errors.Join(errs...)
}
