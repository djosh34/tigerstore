// Package testport picks loopback ports for the processes that tests and
// harnesses start.
package testport

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
)

// The ports come from below Linux's ephemeral range, which starts at 32768
// by default. There the kernel picks the ports of outgoing connections and
// of listeners on port 0, so a port found free can be taken before the
// process binds it (#184).
const lo, hi = 10000, 32768

// Listen listens on a free loopback port below the ephemeral range and
// returns the listener and its port. The caller closes the listener just
// before the process that uses the port starts.
func Listen() (net.Listener, int, error) {
	for range 100 {
		port := lo + rand.IntN(hi-lo)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			return ln, port, nil
		}
	}
	return nil, 0, errors.New("testport: no free port below 32768 in 100 tries")
}
