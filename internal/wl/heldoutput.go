package wl

// #912 leaves main's held backing outside this proof, but not any growth
// introduced here. Keep held at 56 bytes. Cursor/material handles occupy
// main's four padding bytes; their intrusive nodes live in already-funded
// stage/root or material control units, never an auxiliary map or slice.
const heldControl uint32 = 1

type heldOutput struct {
	next     *heldOutput
	stage    *replyStage
	material *materialReply
	key      uint32
}

func (h *held) flatBytes() int64 {
	if h.flatCharge&3 != 0 {
		return 0
	}
	return int64(h.flatCharge)
}

func (c *client) linkOutput(o *heldOutput, tag uint32) uint32 {
	// Wrap is harmless: never reuse a still-live handle. No allocation is
	// needed, including after cancellation or high-water container reuse.
	for {
		c.outputSeq++
		key := c.outputSeq<<2 | tag
		if c.findOutput(key) == nil {
			o.key, o.next = key, c.outputs
			c.outputs = o
			return key
		}
	}
}

func (c *client) findOutput(key uint32) *heldOutput {
	if key&3 < 2 {
		return nil
	}
	for o := c.outputs; o != nil; o = o.next {
		if o.key == key {
			return o
		}
	}
	return nil
}

func (c *client) heldStage(h *held) *replyStage {
	if o := c.findOutput(h.flatCharge); o != nil {
		return o.stage
	}
	return nil
}

func (c *client) heldMaterial(h *held) *materialReply {
	if o := c.findOutput(h.flatCharge); o != nil {
		return o.material
	}
	return nil
}

func (c *client) unlinkOutput(h *held) {
	for p := &c.outputs; *p != nil; p = &(*p).next {
		if (*p).key == h.flatCharge {
			o := *p
			*p = o.next
			o.next, o.stage, o.material = nil, nil, nil
			h.flatCharge = 0
			return
		}
	}
}
