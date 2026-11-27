package wl

import "testing"

// Unlike #904/#908, ordinary watch-control output is not excluded. Main
// accepts these requests within one input turn, even with DATA closed.
func TestWatchControlCannotSpendProtectedFetchCapacity(t *testing.T) {
	c, _, _, _, _ := fillRangeFixture(t)
	n := c.node
	other := partClient(t, n, 1)
	t.Cleanup(other.dropAll)
	other.flow.conn = 0
	n.socks, n.gens = []any{c, other}, []uint16{1, 1}
	n.clients = 2
	s := other.newStream(1, kWatch)
	if err := n.request(other, s, nil); err != nil {
		t.Fatal(err)
	} // fund initial response headers before the protected promises
	wire := stageRangeBytes(t, 512)
	for i := range 1024 {
		stage := n.admitStage(c, len(wire))
		if stage == nil {
			break
		}
		stage.append(wire)
		s := c.newStream(uint32(3+2*i), kRange)
		if !c.holdStage(s, stage) {
			t.Fatal("source commit")
		}
	}
	const requests = 250000
	msg := []byte{0x1a, 0x00}
	if requests*(5+len(msg)) > int(n.sizes.reserve()) {
		t.Fatal("probe exceeded a normal input turn")
	}
	for i := range requests {
		if err := n.request(other, s, msg); err != nil {
			t.Fatal(err)
		}
		if used := n.stageCredits.used + n.stageOther(0); used > n.sizes.clientPool {
			t.Fatalf("UNRESOLVED ordinary watch control stole protected P after %d requests: %d > %d; no capture or sort", i+1, used, n.sizes.clientPool)
		}
	}
}
