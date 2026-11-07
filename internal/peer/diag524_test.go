package peer

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// TestDiag524TLSBulk isolates the bench's delayed TCP link. Run only on an
// isolated host with bench/netem.sh on 10. No Raft, disk, index, or clients.
// Four fresh connections expose differences in TCP's initial conditions.
func TestDiag524TLSBulk(t *testing.T) {
	if os.Getenv("DIAG524_NET") == "" {
		t.Skip("needs an isolated host with netem on the bench peer ports")
	}
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("node", 2)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:20021", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	const total = 64 << 20
	for i := 0; i < 4; i++ {
		done := make(chan error, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			_, err = io.CopyN(io.Discard, c, total)
			if err == nil {
				_, err = c.Write([]byte{1})
			}
			done <- err
		}()
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", ln.Addr().String(), &tls.Config{RootCAs: ca.Pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4<<20)
		start := time.Now()
		for n := 0; n < total; n += len(buf) {
			if _, err := c.Write(buf); err != nil {
				t.Fatal(err)
			}
		}
		var reply [1]byte
		if _, err := io.ReadFull(c, reply[:]); err != nil {
			t.Fatal(err)
		}
		d := time.Since(start)
		c.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		t.Logf("connection %d: 64 MiB in %v (%.2f MiB/s)", i, d, 64/d.Seconds())
		// The preload offers up to ~18 MiB/s to each follower. Give this
		// disk-free, otherwise idle link 7 s, the server request timeout.
		if d > 7*time.Second {
			t.Errorf("bulk link alone exceeded the 7 s request timeout")
		}
	}
}
