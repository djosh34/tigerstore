package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/djosh34/tigerstore/chaos/env"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/testport"
)

// cluster is three tigerstore processes on this host, each in a cgroup
// of its own with a memory limit, talking TLS.
type cluster struct {
	bin   string
	dir   string
	tls   *tls.Config
	nodes [3]*node
	// endpoints are the nodes' client addresses.
	endpoints [3]string
	// exited is told of every exit the soak did not cause, as it happens.
	exited func(string)
}

// node is one member's process and what the soak expects of it.
type node struct {
	i    int
	args []string
	data string
	log  *os.File
	cg   *env.Cgroup
	// one is a client of this node alone, for probes and hashes.
	one *clientv3.Client

	mu  sync.Mutex
	cmd *exec.Cmd
	// expecting is set while the soak kills or stops the process, and
	// exits lists the exits it did not cause.
	expecting bool
	exits     []string
	starts    int
}

func newCluster(cfg config) (*cluster, error) {
	c := &cluster{bin: cfg.tigerstore, dir: cfg.out}
	certs := filepath.Join(cfg.out, "certs")
	if err := os.MkdirAll(certs, 0o755); err != nil {
		return nil, err
	}
	ca, err := testcert.NewCA()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(certs, "ca.pem"), ca.PEM, 0o600); err != nil {
		return nil, err
	}
	for i, name := range []string{"node1", "node2", "node3", "client"} {
		cert, certPEM, keyPEM, err := ca.Issue(name, int64(i+2))
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(certs, name+".pem"), certPEM, 0o600); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(certs, name+"-key.pem"), keyPEM, 0o600); err != nil {
			return nil, err
		}
		if name == "client" {
			c.tls = &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		}
	}
	var clientPort, peerPort [3]int
	var held []net.Listener
	defer func() {
		for _, ln := range held {
			_ = ln.Close()
		}
	}()
	for i := range 3 {
		for _, port := range []*int{&clientPort[i], &peerPort[i]} {
			ln, p, err := testport.Listen()
			if err != nil {
				return nil, err
			}
			held = append(held, ln)
			*port = p
		}
	}
	var members []string
	for i := range 3 {
		members = append(members, fmt.Sprintf("node%d=https://127.0.0.1:%d", i+1, peerPort[i]))
	}
	for i := range 3 {
		name := fmt.Sprintf("node%d", i+1)
		n := &node{i: i, data: filepath.Join(cfg.out, name)}
		if n.log, err = os.OpenFile(filepath.Join(cfg.out, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
			return nil, err
		}
		cert, key, caFile := filepath.Join(certs, name+".pem"), filepath.Join(certs, name+"-key.pem"), filepath.Join(certs, "ca.pem")
		c.endpoints[i] = fmt.Sprintf("127.0.0.1:%d", clientPort[i])
		n.args = []string{
			"--name", name, "--data-dir", n.data,
			"--initial-cluster", strings.Join(members, ","),
			"--listen-peer-urls", fmt.Sprintf("https://127.0.0.1:%d", peerPort[i]),
			"--initial-advertise-peer-urls", fmt.Sprintf("https://127.0.0.1:%d", peerPort[i]),
			"--listen-client-urls", "https://" + c.endpoints[i],
			"--advertise-client-urls", "https://" + c.endpoints[i],
			"--cert-file", cert, "--key-file", key, "--trusted-ca-file", caFile, "--client-cert-auth",
			"--peer-cert-file", cert, "--peer-key-file", key, "--peer-trusted-ca-file", caFile,
		}
		if cfg.memory > 0 {
			// The node's budget is its cgroup's limit, which it would read
			// itself if it started inside the group; it is moved in just
			// after it starts.
			n.args = append(n.args, "--memory-budget-bytes", strconv.FormatInt(cfg.memory, 10))
		}
		if cfg.diskBudget > 0 {
			n.args = append(n.args, "--disk-budget-bytes", strconv.FormatInt(cfg.diskBudget, 10))
		}
		n.args = append(n.args, cfg.nodeArgs...)
		if n.cg, err = env.NewCgroup(fmt.Sprintf("soak-%d-%s", os.Getpid(), name)); err != nil {
			return nil, err
		}
		if cfg.memory > 0 {
			if err := n.cg.LimitMemory(cfg.memory); err != nil {
				return nil, err
			}
		}
		c.nodes[i] = n
	}
	return c, nil
}

// dial makes a client of the given endpoints. Unary calls are not
// retried, so an error leaves a write ambiguous and nothing else.
func (c *cluster) dial(endpoints ...string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints: endpoints, TLS: c.tls, DialTimeout: 5 * time.Second, MaxUnaryRetries: 1,
		Logger: zap.NewNop(), DialOptions: []grpc.DialOption{grpc.WithDisableRetry()},
		MaxCallSendMsgSize: 4 << 20, MaxCallRecvMsgSize: 64 << 20,
	})
}

// start starts n's process and moves it into its cgroup.
func (c *cluster) start(n *node) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.starts++
	_, _ = fmt.Fprintf(n.log, "soak: start incarnation %d at %s\n", n.starts, time.Now().UTC().Format(time.RFC3339Nano))
	cmd := exec.Command(c.bin, n.args...)
	cmd.Stdout, cmd.Stderr = n.log, n.log
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := n.cg.Add(cmd.Process.Pid); err != nil {
		return errors.Join(err, cmd.Process.Kill())
	}
	n.cmd, n.expecting = cmd, false
	starts := n.starts
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		msg := ""
		if !n.expecting {
			msg = fmt.Sprintf("node%d incarnation %d exited on its own: %v", n.i+1, starts, err)
			n.exits = append(n.exits, msg)
		}
		n.mu.Unlock()
		if msg != "" && c.exited != nil {
			c.exited(msg)
		}
	}()
	return nil
}

// signal sends sig to n's process; for SIGKILL it waits until it exited.
func (n *node) signal(sig syscall.Signal) error {
	n.mu.Lock()
	cmd := n.cmd
	if sig == syscall.SIGKILL || sig == syscall.SIGTERM {
		n.expecting = true
	}
	n.mu.Unlock()
	if err := cmd.Process.Signal(sig); err != nil {
		return err
	}
	if sig == syscall.SIGKILL {
		for cmd.ProcessState == nil {
			if err := syscall.Kill(cmd.Process.Pid, 0); errors.Is(err, syscall.ESRCH) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return nil
}

// unexpected lists the exits the soak did not cause.
func (c *cluster) unexpected() []string {
	var out []string
	for _, n := range c.nodes {
		n.mu.Lock()
		out = append(out, n.exits...)
		n.mu.Unlock()
	}
	return out
}

// ready waits until every node has a leader.
func (c *cluster) ready(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, n := range c.nodes {
		for {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			st, err := n.one.Status(cctx, n.one.Endpoints()[0])
			cancel()
			if err == nil && st.Leader != 0 {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("node%d not ready within %v: %w", n.i+1, timeout, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// leader is the index of the node that two nodes name as leader.
func (c *cluster) leader(ctx context.Context) (int, error) {
	ids := map[uint64]int{}
	votes := map[uint64]int{}
	var last error
	for _, n := range c.nodes {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := n.one.Status(cctx, n.one.Endpoints()[0])
		cancel()
		if err != nil {
			last = err
			continue
		}
		ids[st.Header.MemberId] = n.i
		if st.Leader != 0 {
			votes[st.Leader]++
		}
	}
	for id, v := range votes {
		if i, ok := ids[id]; ok && v >= 2 {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no leader that two nodes agree on: %w", last)
}

// stop ends every process, SIGTERM first.
func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.mu.Lock()
		cmd := n.cmd
		n.expecting = true
		n.mu.Unlock()
		if cmd == nil {
			continue
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
	deadline := time.Now().Add(20 * time.Second)
	for _, n := range c.nodes {
		if n.cmd == nil {
			continue
		}
		for syscall.Kill(n.cmd.Process.Pid, 0) == nil && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		_ = n.cmd.Process.Kill()
	}
}

// close removes the cgroups and closes the logs.
func (c *cluster) close() {
	for _, n := range c.nodes {
		if n == nil {
			continue
		}
		if n.one != nil {
			_ = n.one.Close()
		}
		if n.cg != nil {
			time.Sleep(100 * time.Millisecond)
			_ = n.cg.Close()
		}
		_ = n.log.Close()
	}
}

// dirSize is the bytes of the files under dir.
func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

// rss is the resident memory of process pid, from /proc.
func rss(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			var kb int64
			_, _ = fmt.Sscan(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), "kB")), &kb)
			return kb << 10
		}
	}
	return 0
}
