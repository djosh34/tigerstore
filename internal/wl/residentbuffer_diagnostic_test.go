package wl

import (
	"fmt"
	"strings"
	"testing"
)

// Ruling7228485870 requires numbers BEFORE restoring any main direct path
// that could reintroduce per-consumer buffering. This reproduces main's
// large resident path only; it is not permission to activate that path.
func TestMainResidentLargeBufferRiskDiagnostic(t *testing.T) {
	c, _, st, log := replyRoomClient(t)
	n := c.node
	st.setBudget(83<<20, 0)
	n.sizes = sizeFor(83 << 20)
	n.cursorFetch = false
	n.connOut = n.sizes.connOut(3)
	for i := range 4 {
		putValue(t, st, log, fmt.Sprintf("/large-resident/%d", i), strings.Repeat("v", (2<<20)-256))
	}
	msg := appendBytesField(appendBytesField(nil, 1, []byte("/large-resident/")), 2, []byte("/large-resident0"))
	clients := []*client{c}
	for i := 1; i < 3; i++ {
		other := newClient(&handoff{fd: -1, wr: c.wr, rd: c.rd}, n)
		other.takeBufs()
		other.idx = i
		other.gen = 1
		n.socks = append(n.socks, other)
		n.gens = append(n.gens, 1)
		clients = append(clients, other)
	}
	if n.cpool > n.sizes.clientInBufs() {
		t.Fatalf("diagnostic bypassed input admission: %d > %d", n.cpool, n.sizes.clientInBufs())
	}
	for _, cl := range clients {
		t.Cleanup(cl.dropAll)
		s := cl.newStream(3, kRange)
		cl.flow.conn = 1024
		s.win = 1024
		serveRoomRange(t, cl, s, msg)
	}
	retained, capacity := 0, 0
	for _, b := range n.bigs {
		retained += len(b.buf) - b.pos
		capacity += cap(b.buf)
	}
	t.Logf("main direct large resident: consumers=%d maxClients=%d B=%d P=%d input/idle=%d output=%d stageOther=%d retained=%d buf-capacity=%d stages=%d loads=%d", len(n.bigs), n.sizes.maxClients(), st.budget, n.sizes.clientPool, n.cpool, n.cout, n.stageOther(0), retained, capacity, n.stageCredits.used, st.loads)
	// Keep this diagnostic factual: prove that three admitted, blocked consumers
	// can exceed P using plain copied byte buffers, with all values resident.
	if len(n.bigs) != 3 || int64(capacity) <= n.sizes.clientPool {
		t.Fatalf("risk did not reproduce: bigs=%d cap=%d P=%d", len(n.bigs), capacity, n.sizes.clientPool)
	}
}
