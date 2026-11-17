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
// its stream goes on (#689's re-check of 13f0e6d9, which held the
// requests alone and lost their streams' ends).
//
// Its connection's other calls wait with it, so a call waits only for an
// entry already in this node's log and not yet applied: an apply's lag,
// not a replication's. A token naming an entry past the log is refused
// at once, as before: waited for, one call of a made-up token stalled its
// connection for requestTimeout, again and again, and the node's own
// client, which every user's gateway call and Lock or Election goes
// through, with it (the L re-check of d33d3711). Calls etcd answers with
// no token checked do not wait: unknown methods, Health, Authenticate,
// and the side calls' own (byCaller), whose outer call waited.

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
	if !n.auth.enabled || n.jwt != nil || s.token == "" || s.kind == kAuthenticate || s.kind == kUnknown || s.byCaller || healthKind(s.kind) {
		return
	}
	_, known := n.tokens[s.token]
	if known {
		return
	}
	index := tokenIndex(s.token)
	if index <= n.applied || index > n.storage.lastIndex() {
		// Applied and still unknown: expired, dropped, or made before a
		// restart; or past the log. The call is refused.
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

// healthKind reports whether kind is a method of gRPC's Health service,
// which etcd answers with no token checked.
func healthKind(kind uint8) bool {
	return sideKind(kind) && strings.HasPrefix(sideMethods[kind-kSide].path, "/grpc.health.v1.Health/")
}
