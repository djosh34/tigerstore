package node

import (
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// This controls arrivals during a held append. It proves the scheduling
// rule, not that closed-loop gRPC concurrency produces the same arrivals.
func TestGroupFollowsQueuedLoad(t *testing.T) {
	for _, count := range []int{16, 64, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			n, _ := testNode(t, []uint64{1})
			_, ctx, _ := startCommit(t, n)
			submit(t, n, ctx, 1)
			first := receive(t, n.persistQ)
			for id := 2; id < count+2; id++ {
				submit(t, n, ctx, uint64(id))
			}
			if len(n.persistQ) != 0 {
				t.Fatal("another group escaped the held append")
			}
			complete(t, n, first)
			group := receive(t, n.persistQ)
			seen := make(map[uint64]bool)
			for _, job := range group {
				for _, entry := range job.Entries {
					if err := core.DecodeBatch(entry.Data, func(_, _, id uint64, _ proto.Message) error {
						if seen[id] || id < 2 || id >= uint64(count+2) {
							t.Errorf("unexpected or duplicate request %d", id)
						}
						seen[id] = true
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(seen) != count {
				t.Fatalf("one next group contains %d requests, want %d", len(seen), count)
			}
			t.Logf("held arrivals=%d, next physical group requests=%d", count, len(seen))
		})
	}
}
