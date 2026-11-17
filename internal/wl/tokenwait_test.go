package wl

import (
	"testing"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
)

// A request whose simple token names an entry this node has not applied
// yet waits for it, as etcd's isValidSimpleToken does, and is taken once
// the entry applies. One whose entry does not come in requestTimeout is
// refused; a reset stream gives back what its request held; a token
// whose entry applied and that the node does not know is refused at
// once. Refused at once, a lagging node refused a valid token: a watch
// was canceled as "invalid auth token" (#689's CI loop for #733).
func TestARequestWaitsForTheEntryOfItsToken(t *testing.T) {
	n := authNode(t)
	c, s := flowClient(t)
	c.node = n
	c.idx, c.gen = 0, 1
	n.socks, n.gens = []any{c}, []uint16{1}
	n.now = time.Now()
	n.applied = 10
	refused := func() int64 { return n.gs.calls[kRange].handled[codes.Unauthenticated] }

	s.token = "abc.12"
	if err := n.request(c, s, rangeMsg("b")); err != nil {
		t.Fatal(err)
	}
	if refused() != 0 || len(n.reads) != 0 || len(n.tokenWaits) != 1 || c.heldCounted == 0 {
		t.Fatalf("a request with a token of entry 12, 10 applied: %d refused, %d reads, %d waiting, %d held", refused(), len(n.reads), len(n.tokenWaits), c.heldCounted)
	}
	n.retryTokenWaits()
	if len(n.tokenWaits) != 1 {
		t.Fatal("the request went on before its token's entry applied")
	}
	if _, err := n.authenticated(&etcdpb.InternalAuthenticateRequest{Name: "alice", SimpleToken: "abc"}, 12, &etcdpb.ResponseHeader{}); err != nil {
		t.Fatal(err)
	}
	n.applied = 12
	n.retryTokenWaits()
	if refused() != 0 || len(n.reads) != 1 || len(n.tokenWaits) != 0 || c.heldCounted != 0 || s.tokenWaits != 0 {
		t.Fatalf("once entry 12 applied: %d refused, %d reads, %d waiting, %d held, stream waits %d", refused(), len(n.reads), len(n.tokenWaits), c.heldCounted, s.tokenWaits)
	}

	// An entry that does not come: refused after requestTimeout.
	s2 := c.newStream(3, kRange)
	s2.token = "abc.99"
	if err := n.request(c, s2, rangeMsg("b")); err != nil {
		t.Fatal(err)
	}
	n.now = n.now.Add(requestTimeout - time.Millisecond)
	n.retryTokenWaits()
	if refused() != 0 || len(n.tokenWaits) != 1 {
		t.Fatalf("before requestTimeout: %d refused, %d waiting", refused(), len(n.tokenWaits))
	}
	n.now = n.now.Add(time.Millisecond)
	n.retryTokenWaits()
	if refused() != 1 || len(n.tokenWaits) != 0 || c.heldCounted != 0 {
		t.Fatalf("at requestTimeout: %d refused, %d waiting, %d held", refused(), len(n.tokenWaits), c.heldCounted)
	}

	// A stream reset while its request waits gives back what it held.
	s3 := c.newStream(5, kRange)
	s3.token = "abc.99"
	if err := n.request(c, s3, rangeMsg("b")); err != nil {
		t.Fatal(err)
	}
	n.dropWaits(c, 5)
	if len(n.tokenWaits) != 0 || c.heldCounted != 0 {
		t.Fatalf("after a reset: %d waiting, %d held", len(n.tokenWaits), c.heldCounted)
	}

	// A token whose entry applied, unknown: refused at once.
	s4 := c.newStream(7, kRange)
	s4.token = "abc.11"
	if err := n.request(c, s4, rangeMsg("b")); err != nil {
		t.Fatal(err)
	}
	if refused() != 2 || len(n.tokenWaits) != 0 {
		t.Fatalf("a token of an applied entry, unknown: %d refused, %d waiting", refused(), len(n.tokenWaits))
	}
}
