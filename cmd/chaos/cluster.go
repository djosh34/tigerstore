package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
	"github.com/djosh34/tigerstore/internal/testcert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// link is one proxied connection path: a client to node To, or node From
// to node To. From is -1 for a client link.
type link struct {
	Name  string
	From  int
	To    int
	Proxy *proxy.Proxy
}

// cluster is three tigerstore processes behind one proxy per link.
type cluster struct {
	dir     string
	bin     string
	nodes   [3]*node
	clients [3]*link    // client to node i
	peers   [3][3]*link // node i to node j
	links   []*link
	tls     *tls.Config
	ids     [3]uint64 // member ID of each node
}

// node is one tigerstore process and its restarts.
type node struct {
	i    int
	args []string
	log  *os.File

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan *os.ProcessState
	// starts counts incarnations; unexpected records exits nobody asked for.
	starts     int
	expecting  bool
	unexpected []string
	// control talks to the node directly, past its proxy, for status.
	control *clientv3.Client
}

// reservePort returns a free port and the listener that holds it. The
// caller closes the listener once the proxies listen, so the kernel cannot
// hand the port to a proxy meanwhile.
func reservePort() (net.Listener, int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, err
	}
	return ln, ln.Addr().(*net.TCPAddr).Port, nil
}

func writeFile(path string, b []byte) error {
	return os.WriteFile(path, b, 0o600)
}

// newCluster makes certificates, proxies and node arguments; start runs
// the processes.
func newCluster(dir, bin string, seed uint64) (*cluster, error) {
	c := &cluster{dir: dir, bin: bin}
	certs := filepath.Join(dir, "certs")
	if err := os.MkdirAll(certs, 0o755); err != nil {
		return nil, err
	}
	ca, err := testcert.NewCA()
	if err != nil {
		return nil, err
	}
	if err := writeFile(filepath.Join(certs, "ca.pem"), ca.PEM); err != nil {
		return nil, err
	}
	for i, name := range []string{"node1", "node2", "node3", "client"} {
		cert, certPEM, keyPEM, err := ca.Issue(name, int64(i+2))
		if err != nil {
			return nil, err
		}
		if err := writeFile(filepath.Join(certs, name+".pem"), certPEM); err != nil {
			return nil, err
		}
		if err := writeFile(filepath.Join(certs, name+"-key.pem"), keyPEM); err != nil {
			return nil, err
		}
		if name == "client" {
			c.tls = &tls.Config{RootCAs: ca.Pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		}
	}
	var clientPort, peerPort [3]int
	var held []net.Listener
	// The node ports stay held until every proxy listens: a proxy once got
	// a node's port, and the node could not start.
	defer func() {
		for _, ln := range held {
			_ = ln.Close()
		}
	}()
	for i := range 3 {
		for _, port := range []*int{&clientPort[i], &peerPort[i]} {
			ln, p, err := reservePort()
			if err != nil {
				return nil, err
			}
			held = append(held, ln)
			*port = p
		}
	}
	add := func(name string, from, to, port int) (*link, error) {
		// Each link gets its own seed, so two links never share choices.
		p, err := proxy.Listen("127.0.0.1:0", fmt.Sprintf("127.0.0.1:%d", port), seed*64+uint64(len(c.links)))
		if err != nil {
			return nil, err
		}
		l := &link{Name: name, From: from, To: to, Proxy: p}
		c.links = append(c.links, l)
		return l, nil
	}
	for i := range 3 {
		if c.clients[i], err = add(fmt.Sprintf("client-node%d", i+1), -1, i, clientPort[i]); err != nil {
			return nil, errors.Join(err, c.closeProxies())
		}
	}
	for i := range 3 {
		for j := range 3 {
			if i == j {
				continue
			}
			if c.peers[i][j], err = add(fmt.Sprintf("node%d-node%d", i+1, j+1), i, j, peerPort[j]); err != nil {
				return nil, errors.Join(err, c.closeProxies())
			}
		}
	}
	for i := range 3 {
		// Node i reaches node j at the proxy of link i to j, so its
		// --initial-cluster differs from the other nodes' in the peer
		// addresses. Raft IDs are positions in the list and match.
		var members []string
		for j := range 3 {
			addr := fmt.Sprintf("127.0.0.1:%d", peerPort[j])
			if j != i {
				addr = c.peers[i][j].Proxy.Addr()
			}
			members = append(members, fmt.Sprintf("node%d=https://%s", j+1, addr))
		}
		name := fmt.Sprintf("node%d", i+1)
		log, err := os.OpenFile(filepath.Join(dir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, errors.Join(err, c.closeProxies())
		}
		cert := filepath.Join(certs, name+".pem")
		key := filepath.Join(certs, name+"-key.pem")
		caFile := filepath.Join(certs, "ca.pem")
		n := &node{i: i, log: log, args: []string{
			"--name", name, "--data-dir", filepath.Join(dir, "data", name),
			"--initial-cluster", strings.Join(members, ","),
			"--listen-peer-urls", fmt.Sprintf("https://127.0.0.1:%d", peerPort[i]),
			"--listen-client-urls", fmt.Sprintf("https://127.0.0.1:%d", clientPort[i]),
			"--advertise-client-urls", "https://" + c.clients[i].Proxy.Addr(),
			"--cert-file", cert, "--key-file", key, "--trusted-ca-file", caFile, "--client-cert-auth",
			"--peer-cert-file", cert, "--peer-key-file", key, "--peer-trusted-ca-file", caFile,
		}}
		n.control, err = clientv3.New(clientv3.Config{
			Endpoints: []string{fmt.Sprintf("127.0.0.1:%d", clientPort[i])}, TLS: c.tls, DialTimeout: 2 * time.Second, Logger: zap.NewNop(),
		})
		if err != nil {
			return nil, errors.Join(err, c.closeProxies())
		}
		c.nodes[i] = n
	}
	return c, nil
}

func (c *cluster) closeProxies() error {
	var errs []error
	for _, l := range c.links {
		errs = append(errs, l.Proxy.Close())
	}
	return errors.Join(errs...)
}

// start starts the process of n.
func (c *cluster) start(n *node) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.starts++
	_, _ = fmt.Fprintf(n.log, "chaos: start incarnation %d at %s\n", n.starts, time.Now().UTC().Format(time.RFC3339Nano))
	cmd := exec.Command(c.bin, n.args...)
	cmd.Stdout, cmd.Stderr = n.log, n.log
	if err := cmd.Start(); err != nil {
		return err
	}
	n.cmd = cmd
	n.expecting = false
	exited := make(chan *os.ProcessState, 1)
	n.exited = exited
	go func() {
		_ = cmd.Wait()
		n.mu.Lock()
		if !n.expecting {
			n.unexpected = append(n.unexpected, fmt.Sprintf("incarnation %d (pid %d) exited on its own: %v", n.starts, cmd.Process.Pid, cmd.ProcessState))
		}
		n.mu.Unlock()
		exited <- cmd.ProcessState
	}()
	return nil
}

// signal sends sig to n's process. For SIGKILL and SIGTERM it waits for
// the exit and returns the wait status.
func (n *node) signal(sig syscall.Signal) (*os.ProcessState, int, error) {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	if sig == syscall.SIGKILL || sig == syscall.SIGTERM {
		n.expecting = true
	}
	n.mu.Unlock()
	pid := cmd.Process.Pid
	if err := cmd.Process.Signal(sig); err != nil {
		return nil, pid, err
	}
	if sig != syscall.SIGKILL && sig != syscall.SIGTERM {
		return nil, pid, nil
	}
	select {
	case ps := <-exited:
		return ps, pid, nil
	case <-time.After(10 * time.Second):
		return nil, pid, fmt.Errorf("pid %d did not exit within 10s of %v", pid, sig)
	}
}

// procState reads the scheduler state letter of pid from /proc.
func procState(pid int) (string, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return "", fmt.Errorf("unreadable /proc/%d/stat", pid)
	}
	return s[i+2 : i+3], nil
}

// waitState polls until the state of pid is want (or, with not, is not
// want) for up to 2s, and returns the last state seen.
func waitState(pid int, want string, not bool) (string, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, err := procState(pid)
		if err != nil {
			return st, err
		}
		if (st == want) != not {
			return st, nil
		}
		if time.Now().After(deadline) {
			return st, fmt.Errorf("pid %d state %s after 2s", pid, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// status asks node i directly for its member ID, leader and revision.
func (c *cluster) status(ctx context.Context, i int) (*clientv3.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	n := c.nodes[i]
	return n.control.Status(ctx, n.control.Endpoints()[0])
}

// leader returns the node that every node that answers names as leader,
// waiting up to timeout.
func (c *cluster) leader(ctx context.Context, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		lead := uint64(0)
		agree := 0
		for i := range 3 {
			st, err := c.status(ctx, i)
			if err != nil {
				last = err
				continue
			}
			if st.Leader == 0 || (lead != 0 && st.Leader != lead) {
				agree = 0
				break
			}
			lead = st.Leader
			agree++
		}
		if agree >= 2 {
			for i, id := range c.ids {
				if id == lead {
					return i, nil
				}
			}
			return -1, fmt.Errorf("leader %d is not a member", lead)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1, fmt.Errorf("no agreed leader within %v: %w", timeout, last)
}

// converged waits until all three nodes answer, name one leader and
// report one revision, so the cluster is whole again.
func (c *cluster) converged(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		var lead uint64
		var rev int64
		ok := true
		for i := range 3 {
			st, err := c.status(ctx, i)
			if err != nil {
				last, ok = err, false
				break
			}
			if i == 0 {
				lead, rev = st.Leader, st.Header.Revision
			}
			if st.Leader == 0 || st.Leader != lead || st.Header.Revision != rev {
				last, ok = fmt.Errorf("node%d: leader %d revision %d, node1: leader %d revision %d", i+1, st.Leader, st.Header.Revision, lead, rev), false
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not converge within %v: %w", timeout, last)
}

// ready waits for the first leader and records member IDs.
func (c *cluster) ready(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for i := range 3 {
		for {
			st, err := c.status(ctx, i)
			if err == nil && st.Leader != 0 {
				c.ids[i] = st.Header.MemberId
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("node%d not ready within %v: %w", i+1, timeout, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// stop ends every process with SIGTERM, or SIGKILL if that fails, so the
// data directories can be read.
func (c *cluster) stop() error {
	var errs []error
	for _, n := range c.nodes {
		if _, _, err := n.signal(syscall.SIGCONT); err != nil {
			errs = append(errs, err)
		}
		if _, _, err := n.signal(syscall.SIGTERM); err != nil {
			_, _, kerr := n.signal(syscall.SIGKILL)
			errs = append(errs, err, kerr)
		}
		errs = append(errs, n.control.Close())
	}
	return errors.Join(errs...)
}

// unexpectedExits lists the exits nobody asked for.
func (c *cluster) unexpectedExits() []string {
	var out []string
	for _, n := range c.nodes {
		n.mu.Lock()
		for _, u := range n.unexpected {
			out = append(out, fmt.Sprintf("node%d: %s", n.i+1, u))
		}
		n.mu.Unlock()
	}
	return out
}
