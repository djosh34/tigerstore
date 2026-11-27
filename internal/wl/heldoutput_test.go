package wl

import (
	"testing"
	"unsafe"
)

func TestHeldOutputHandlesKeepMainLayoutAndRetire(t *testing.T) {
	// #912: no larger backing array, and no new per-control owner.
	if unsafe.Sizeof(held{}) != 56 || unsafe.Offsetof(held{}.data) != 8 || unsafe.Offsetof(held{}.trailer) != 32 {
		t.Fatal("main held layout changed")
	}
	if unsafe.Sizeof(replyStage{}) > stageUnit || unsafe.Sizeof(materialReply{}) > stageUnit {
		t.Fatal("intrusive owners exceed their existing funded control units")
	}
	c, a := flowClient(t)
	n := c.node
	n.sizes, n.connOut, n.cursorFetch = sizeFor(83<<20), 64<<10, true
	c.flow.conn = 0
	defer c.dropAll()
	b := c.newStream(3, kRange)
	wire := stageRangeBytes(t, 512)
	stages := make([]*replyStage, 3)
	for i, s := range []*stream{a, b, a} {
		stages[i] = n.admitStage(c, len(wire))
		if stages[i] == nil {
			t.Fatal("no physical backing")
		}
		stages[i].append(wire)
		if i == 2 {
			// Force wrap and a collision with the first live handle.
			c.outputSeq = 1 << 30
		}
		if !c.holdStage(s, stages[i]) {
			t.Fatal("complete message failed to commit")
		}
	}
	if c.flow.held[0].flatCharge == c.flow.held[2].flatCharge || c.heldStage(&c.flow.held[2]) != stages[2] {
		t.Fatal("wrap reused a live output handle")
	}
	c.dropHeld(a.id)
	if len(c.flow.held) != 1 || c.heldStage(&c.flow.held[0]) != stages[1] || stages[0].charged != 0 || stages[2].charged != 0 {
		t.Fatal("reset released another stream or retained cancelled storage")
	}
	c.windowUpdate(0, 64<<10)
	if len(c.flow.held) != 0 || c.outputs != nil || stages[1].charged != 0 || n.stageCredits.used != 0 {
		t.Fatal("drain retained an owner or released live capacity early")
	}
}
