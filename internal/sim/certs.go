package sim

import (
	"fmt"
	"time"
)

// expireCerts resets every connection a second after the nodes'
// certificate expired (Config.CertExpiresAt): from now on no TLS
// handshake succeeds, so no node reaches another and no client reaches a
// node, until an operator rotates the certificate.
func (s *Sim) expireCerts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.certsExpiredAt, s.stats.CertsExpiredAt = s.now(), s.now()
	for i := range len(s.links) + 1 {
		for j := range i {
			s.net.resetBetween(i, j)
		}
	}
	s.tracef("the nodes' certificate expired: every connection reset")
}

// rotateCerts is an operator who rotates the expired certificate: the
// same CA issues a new one, and every node that runs restarts with it,
// as a process kill keeps what its disk holds. The liveness rule waits
// for the cluster to recover, as after a node that stopped late.
func (s *Sim) rotateCerts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	nw := &s.net
	cert, _, _, err := nw.signer.IssueUntil("tigerstore-sim", 3, nil, nw.hosts, time.Now().Add(nw.valid))
	if err != nil {
		s.fail("rotating the certificate: %w", err)
		return
	}
	nw.cert, nw.expires = cert, time.Now().Add(nw.valid)
	s.certsRotatedAt = s.now()
	for _, n := range s.nodes {
		if n.proc != nil && !n.out() {
			n.why = "an operator restarted it with a new certificate"
			s.down(n)
			s.restartLater(n, 1+s.rng.Int64N(200*Millisecond))
		}
	}
	s.lateStopAt = s.now()
	s.extendLiveness()
	s.tracef("an operator rotated the certificate and restarted every node")
}

// checkExpired checks a run whose certificate expired: no write that
// started after the connections were reset and ended before the
// certificate was rotated succeeded, as no node could reach another.
func (s *Sim) checkExpired(res *Result) error {
	end := s.certsRotatedAt
	if end == 0 {
		end = s.endAt
	}
	writes := 0
	for _, r := range res.History {
		if r.Call < s.certsExpiredAt || r.Return >= end || !isWrite(r.req) {
			continue
		}
		if r.Err == nil {
			return fmt.Errorf("expired certificate: client %d's write at %d ms, after the connections were reset at %d ms and before the rotation at %d ms, succeeded: %v",
				r.Client, r.Call/Millisecond, s.certsExpiredAt/Millisecond, end/Millisecond, r.req)
		}
		writes++
	}
	res.Stats.ExpiredWrites = writes
	s.mu.Lock()
	s.tracef("expired certificate: %d writes between the reset and the rotation, none succeeded", writes)
	s.mu.Unlock()
	return nil
}
