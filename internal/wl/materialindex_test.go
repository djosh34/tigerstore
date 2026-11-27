package wl

import "testing"

func TestMaterialTransportIndexKeepsOffsetsLinksAndReadiness(t *testing.T) {
	for _, wide := range []bool{false, true} {
		payload := int64(1024)
		if wide {
			payload = int64(^uint32(0)) + 1024
		}
		b := legacyBatch{n: 2, payload: payload}
		b.materialIndex = make([]uint64, b.materialWords())
		offset := 512
		if wide {
			offset = int(^uint32(0)) + 1
		}
		b.initMaterialSlot(0, 0, true)
		b.initMaterialSlot(1, offset, false)
		b.setMaterialNext(0, 1)
		if b.materialOffset(0) != 0 || b.materialOffset(1) != int64(offset) || b.materialOffset(2) != payload || b.materialNext(0) != 1 || b.materialNext(1) != materialEnd || *b.materialWord(0)&materialReady == 0 || *b.materialWord(1)&materialReady != 0 {
			t.Fatal("transport metadata truncated an offset/link or changed readiness")
		}
		b.setMaterialNext(0, materialEnd)
		if b.materialNext(0) != materialEnd || b.materialOffset(1) != int64(offset) || *b.materialWord(0)&materialReady == 0 {
			t.Fatal("output linking changed immutable transport metadata")
		}
	}
}
