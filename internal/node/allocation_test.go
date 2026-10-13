package node

import (
	"context"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
)

type allocationTestLink struct{ next *allocationTestLink }

func (b *allocationTestLink) ManagedRequest() (core.Request, any) { return core.Request{}, b }
func (b *allocationTestLink) NextManagedRequest() ManagedBatch {
	if b.next == nil {
		return nil
	}
	return b.next
}

type allocationTestCompleter struct{ calls int }

func (c *allocationTestCompleter) CompleteOne(tag any, _ core.Reply) {
	c.calls++
	tag.(*allocationTestLink).next = nil
}

func TestAllocationShutdownRetiresUnprocessedInboxLinks(t *testing.T) {
	n, err := New(Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.wal.Close() }()
	last := &allocationTestLink{}
	middle := &allocationTestLink{next: last}
	first := &allocationTestLink{next: middle}
	var to allocationTestCompleter
	if err := n.SubmitManagedBatch(context.Background(), first, 3, &to); err != nil {
		t.Fatal(err)
	}
	n.shutdownManaged(map[uint64]waiter{})
	if to.calls != 3 {
		t.Fatalf("shutdown orphaned links: completions=%d", to.calls)
	}
	if err := n.SubmitManagedBatch(context.Background(), first, 1, &to); err == nil {
		t.Fatal("accepted managed request after shutdown")
	}
}
