package wl

import "testing"

func TestDirectPieceAllocationRequiresAlreadyHeldPCredit(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		used, limit, charge, other, left int64
	}{
		{"not-reserved", 0, 2 * stageUnit, 2 * stageUnit, 0, 2 * stageUnit},
		{"returned-before-allocation", stageUnit, 2 * stageUnit, 2 * stageUnit, 0, stageUnit},
		{"spent-token", 2 * stageUnit, 2 * stageUnit, 2 * stageUnit, 0, 0},
		{"other-owners-exhaust-P", 2 * stageUnit, 2 * stageUnit, 2 * stageUnit, 1, stageUnit},
		{"forged-token", 2 * stageUnit, 2 * stageUnit, 2 * stageUnit, 0, 3 * stageUnit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := stageCredit{used: tc.used, limit: tc.limit}
			left := tc.left
			if claimStageAllocation(&b, tc.charge, tc.other, &left) {
				t.Fatal("unreserved allocation was not stopped BEFORE allocating")
			}
			if left != tc.left {
				t.Fatal("rejected allocation spent its token")
			}
		})
	}
	// No connection ceiling participates: the exact root/page allocation credit
	// is asserted in the allocator itself, including an additional index root.
	for _, size := range []int{0, 1, stagePayload, stagePayload + 1, stagePayload * (stageSlots + 1)} {
		charge := stageCapacity(size)
		b := stageCredit{limit: charge}
		s := newReplyStage(&b, size, 0)
		if s == nil || b.used != charge || s.charged != charge {
			t.Fatal("stage did not reserve all allocations first")
		}
		s.discard()
		if b.used != 0 {
			t.Fatal("funded allocation leaked credit")
		}
	}
}
