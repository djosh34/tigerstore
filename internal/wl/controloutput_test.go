package wl

import "testing"

func TestControlSocketCannotBecomeUnfundedFetchBacking(t *testing.T) {
	c, s, _, _, _ := fillRangeFixture(t)
	n := c.node
	s.kind = kWatch
	for range 4096 {
		n.writeProgress(c, s, -1)
	}
	c.windowUpdate(s.id, 1<<29)
	c.windowUpdate(0, 1<<29)
	if c.controlCapacity == 0 || c.controlHeld != 0 || cap(c.out) < 64<<10 {
		t.Fatal("fixture did not drain controls into inherited socket growth")
	}
	c.flow.conn = 0
	rangeStream := c.newStream(3, kRange)
	wire := stageRangeBytes(t, 512)
	stage := n.admitStage(c, len(wire))
	if stage == nil || c.socketPromise != 64<<10 {
		t.Fatal("inherited control socket was treated as independently funded fetch storage")
	}
	stage.append(wire)
	if !c.holdStage(rangeStream, stage) {
		t.Fatal("complete response failed queue commit")
	}
	// Model the successful-write boundary with no remaining socket bytes.
	// flush must not retain control-grown high water for #897's benefit.
	c.out, c.ow, c.rec = c.out[:0], 0, -1
	if _, err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if c.out != nil || n.controlCapacity != 0 || c.controlCapacity != 0 || c.socketPromise != 64<<10 {
		t.Fatal("empty inherited backing was kept or its future replacement was unfunded")
	}
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool {
		t.Fatal("control retirement spent protected capacity")
	}
	c.windowUpdate(0, 64<<10)
	if stage.charged != 0 || c.socketPromise != 0 || c.outputs != nil || c.stages != 0 || n.controlCapacity != 0 {
		t.Fatal("fetch failed to drain from independent replacement storage")
	}
	if n.stageOther(0)+n.stageCredits.used > n.sizes.clientPool {
		t.Fatal("replacement socket spent protected capacity")
	}
}
