package wl

import (
	"strconv"
	"strings"
	"time"
)

// A call whose simple token names an entry this node has not applied yet
// waits for it, as etcd's isValidSimpleToken waits on the token's index:
// the node that took the Authenticate made the token as it applied the
// entry, and this node makes it as it applies it too. Refused at once, a
// valid token was refused by a node that lagged a little: a watch of a
// client that authenticated through another node was canceled as
// "invalid auth token" (#689's CI loop for #733).
//
// The call's headers stop the reading of its connection: the node reads
// none of its frames past them until the entry applies, or requestTimeout
// passes, and then reads on as before, the call checked as any other. So
// every frame keeps its place (a request and its stream's end, a watch's
// create and cancel, a reset), nothing is copied or held past the
// connection's own buffers, and a call whose entry does not come is
// refused as any with an unknown token is: a watch create is canceled,
// its stream goes on. Its connection's other calls wait with it, as
// they would behind a slow read; the entry is one the cluster committed,
// so a node that is not cut off applies it soon (#689's re-check of
// 13f0e6d9, which held the requests alone and lost their streams' ends).

// tokenIndex is the index a simple token names, "<prefix>.<index>", or
// 0 for none: as etcd's, a token of more dots or none names none.
func tokenIndex(token string) uint64 {
	_, index, ok := strings.Cut(token, ".")
	if !ok {
		return 0
	}
	v, err := strconv.ParseUint(index, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// waitForToken stops the reading of c's frames past the headers of s
// while s's simple token names an entry the node has not applied.
func (n *node) waitForToken(c *client, s *stream) {
	if !n.auth.enabled || n.jwt != nil || s.token == "" || s.kind == kAuthenticate {
		// etcd checks no token of an Authenticate.
		return
	}
	_, known := n.tokens[s.token]
	if known {
		return
	}
	index := tokenIndex(s.token)
	if index <= n.applied {
		// Applied and still unknown: expired, dropped, or made before a
		// restart. The call is refused.
		return
	}
	c.tokenWait, c.tokenAt = index, n.now
	n.tokenWaits = append(n.tokenWaits, c)
	n.markDirty(&c.sock)
}

// tokenReady reports whether c, which waits for the entry of a token,
// reads on: the entry applied, or it waited requestTimeout.
func (n *node) tokenReady(c *client, now time.Time) bool {
	return c.tokenWait <= n.applied || now.Sub(c.tokenAt) >= requestTimeout
}
