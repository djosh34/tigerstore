package proxy

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

// ErrNoDialer means that no process among those asked about holds the
// other end of a connection.
var ErrNoDialer = errors.New("proxy: no such process dialed the connection")

// Dialer returns which of pids holds the other end of c, a connection
// that a Router accepted on the IPv4 loopback, as Linux's /proc shows
// it: the socket whose local address is c's remote one and whose remote
// address is c's local one, and the process with that socket open.
func Dialer(c *net.TCPConn, pids []int) (int, error) {
	inode, err := socketInode(c.RemoteAddr().(*net.TCPAddr), c.LocalAddr().(*net.TCPAddr))
	if err != nil {
		return 0, err
	}
	want := "socket:[" + inode + "]"
	for _, pid := range pids {
		dir := fmt.Sprintf("/proc/%d/fd", pid)
		fds, err := os.ReadDir(dir)
		if err != nil {
			// The process may have exited meanwhile.
			continue
		}
		for _, fd := range fds {
			if link, err := os.Readlink(dir + "/" + fd.Name()); err == nil && link == want {
				return pid, nil
			}
		}
	}
	return 0, ErrNoDialer
}

// socketInode finds the inode of the TCP socket from local to remote in
// /proc/net/tcp.
func socketInode(local, remote *net.TCPAddr) (string, error) {
	f, err := os.Open("/proc/net/tcp")
	if err != nil {
		return "", err
	}
	defer f.Close() //nolint:errcheck // read only
	l, r := procAddr(local), procAddr(remote)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) > 9 && fields[1] == l && fields[2] == r {
			return fields[9], nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%w: no socket from %v to %v", ErrNoDialer, local, remote)
}

// procAddr is an IPv4 address as /proc/net/tcp writes it on a
// little-endian host: the address as a 32-bit number in host order, and
// the port, both in hex.
func procAddr(a *net.TCPAddr) string {
	ip := a.IP.To4()
	if ip == nil {
		return ""
	}
	n := uint32(ip[0]) | uint32(ip[1])<<8 | uint32(ip[2])<<16 | uint32(ip[3])<<24
	return fmt.Sprintf("%08X:%04X", n, a.Port)
}
