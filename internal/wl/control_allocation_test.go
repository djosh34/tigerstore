package wl

import (
	"testing"
	"unsafe"
)

// #912 excludes only inherited watch-control output and held backing.
// Keep main's admission/backpressure, prove exact parity, and keep #897's
// entry at 56 bytes rather than exempting its former 80-byte growth.
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
	if unsafe.Sizeof(held{}) != 56 || unsafe.Sizeof(replyStage{}) > stageUnit || unsafe.Sizeof(materialReply{}) > stageUnit {
		t.Fatal("#897 grew main's held entry or overflowed funded control storage")
	}
	for i := range requests {
		if err := n.request(other, s, msg); err != nil {
			t.Fatal(err)
		}
		if used := n.stageCredits.used + n.stageOther(0); used > n.sizes.clientPool {
			t.Fatalf("watch control stole protected P after %d requests: %d > %d; #912 excludes only unchanged control bytes", i+1, used, n.sizes.clientPool)
		}
	}
	main, _, _, _, _ := fillRangeFixture(t)
	main.node.cursorFetch = false
	main.flow.conn = 0
	ms := main.newStream(3, kWatch)
	if err := main.node.request(main, ms, nil); err != nil {
		t.Fatal(err)
	}
	before := main.node.cout
	for range requests {
		if err := main.node.request(main, ms, msg); err != nil {
			t.Fatal(err)
		}
	}
	if len(other.flow.held) != len(main.flow.held) || cap(other.flow.held) != cap(main.flow.held) || other.flow.bytes != main.flow.bytes || other.controlHeld != main.controlHeld || other.controlHeld != main.node.cout-before {
		t.Fatal("#897 enlarged inherited #912 output or held backing")
	}
	t.Logf("#912 parity: controls=%d bytes=%d held capacity=%d x 56; added fetch P=%d/%d", requests, other.controlHeld, cap(other.flow.held), n.stageCredits.used+n.stageOther(0), n.sizes.clientPool)
	// The #912 boundary covers inherited socket output as well as held
	// bytes. Exercise its transfer while protected fetch storage stays live.
	other.windowUpdate(s.id, 1<<29)
	main.windowUpdate(ms.id, 1<<29)
	other.windowUpdate(0, 1<<29)
	main.windowUpdate(0, 1<<29)
	if cap(other.out) > cap(main.out) || len(other.out) != len(main.out) {
		t.Fatal("#897 enlarged inherited control socket output")
	}
	if used := n.stageCredits.used + n.stageOther(0); used > n.sizes.clientPool {
		t.Fatalf("inherited control drain lost its parity debit: P=%d/%d", used, n.sizes.clientPool)
	}
	other.dropHeld(s.id)
	if other.controlHeld != 0 || n.controlHeld != 0 {
		t.Fatal("reset retained inherited-control debit")
	}
	main.dropAll()
	other.dropAll()
	if n.controlCapacity != 0 || main.node.controlCapacity != 0 {
		t.Fatal("close retained inherited socket capacity")
	}
	if main.controlHeld != 0 || main.node.controlHeld != 0 {
		t.Fatal("close retained inherited-control debit")
	}
}
