package wl

import (
	"bytes"
	"strconv"
	"strings"
)

// A request whose simple token names an entry this node has not applied
// yet waits for it, as etcd's isValidSimpleToken waits on the token's
// index: the node that took the Authenticate made the token as it
// applied the entry, and this node makes it as it applies it too.
// Refused at once, a valid token was refused by a node that lagged a
// little: a watch of a client that authenticated through another node
// was canceled as "invalid auth token" (#689's CI loop for #733). A
// request waits at most requestTimeout, then is refused as before; one
// behind it on its stream waits behind it, in order. What waits counts in
// its client's held bytes, so a client past its share is refused new
// requests (holdsTooMuch), as for replies that wait for room.

// tokenWait is a request that waits for the entry of its token.
type tokenWait struct {
	h     handle
	index uint64
	msg   []byte
	at    int64 // n.now, in nanoseconds, as it came
}

// tokenIndex is the index a simple token names, "<prefix>.<index>", or
// 0 for none.
func tokenIndex(token string) uint64 {
	_, index, ok := strings.CutLast(token, ".")
	if !ok {
		return 0
	}
	v, err := strconv.ParseUint(index, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// waitsForToken reports whether request msg of s waits for the entry of
// its token, and holds it if so.
func (n *node) waitsForToken(c *client, s *stream, msg []byte) bool {
	if s.tokenWaits == 0 {
		if n.jwt != nil || s.token == "" {
			return false
		}
		_, known := n.tokens[s.token]
		if known {
			return false
		}
	}
	index := tokenIndex(s.token)
	if s.tokenWaits == 0 && index <= n.applied {
		// Applied and still unknown: expired, dropped, or made before a
		// restart. It is refused.
		return false
	}
	s.tokenWaits++
	n.tokenWaits = append(n.tokenWaits, tokenWait{h: mkHandle(c, s.id), index: index, msg: bytes.Clone(msg), at: n.now.UnixNano()})
	c.growOut(len(msg))
	c.heldCounted += int64(len(msg))
	return true
}

// retryTokenWaits takes again the requests whose token's entry applied,
// in the order they came, and refuses those that waited requestTimeout.
func (n *node) retryTokenWaits() {
	type ready struct {
		w       tokenWait
		expired bool
	}
	var goes []ready
	k := 0
	now := n.now.UnixNano()
	for _, w := range n.tokenWaits {
		expired := now-w.at >= int64(requestTimeout)
		if w.index > n.applied && !expired {
			n.tokenWaits[k] = w
			k++
			continue
		}
		goes = append(goes, ready{w: w, expired: expired && w.index > n.applied})
	}
	clear(n.tokenWaits[k:])
	n.tokenWaits = n.tokenWaits[:k]
	// Each leaves its stream's count before any is taken again, so that
	// one is not held again behind another of its stream that goes now.
	for _, r := range goes {
		if c, s := n.clientOf(r.w.h); c != nil {
			s.tokenWaits--
			c.growOut(-len(r.w.msg))
			c.heldCounted -= int64(len(r.w.msg))
		}
	}
	for _, r := range goes {
		c, s := n.clientOf(r.w.h)
		if c == nil {
			continue
		}
		if r.expired {
			n.reject(c, s, errInvalidAuthToken)
			continue
		}
		if err := n.request(c, s, r.w.msg); err != nil {
			n.reject(c, s, badRequest(err))
		}
	}
}

// dropTokenWaits lets go of what client c waits for its tokens with, or
// only stream sid's when one; the client reset it or went.
func (n *node) dropTokenWaits(c *client, sid uint32, one bool) {
	k := 0
	for _, w := range n.tokenWaits {
		idx, gen, wsid := w.h.parts()
		if idx == c.idx && gen == c.gen && (!one || wsid == sid) {
			c.growOut(-len(w.msg))
			c.heldCounted -= int64(len(w.msg))
			if s := c.get(wsid); s != nil {
				s.tokenWaits--
			}
			continue
		}
		n.tokenWaits[k] = w
		k++
	}
	clear(n.tokenWaits[k:])
	n.tokenWaits = n.tokenWaits[:k]
}
