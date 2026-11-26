package wl

import "testing"

func TestFetchWorkRoundsDownToWholeArenaUnits(t *testing.T) {
	const budget = 83<<20 + 12345
	b := newFetchWorkBook(budget)
	if b.limit%stageUnit != 0 || b.limit > fetchedMax(budget) || fetchedMax(budget)-b.limit >= stageUnit {
		t.Fatal("the work allowance is not a whole-arena floor of B/32")
	}
}

func TestFetchWorkLegacyAndCursorShareOneAllowance(t *testing.T) {
	for _, budget := range []int64{83 << 20, 137 << 20, 191 << 20} {
		for _, legacyFirst := range []bool{false, true} {
			b := newFetchWorkBook(budget)
			const legacy = 2560<<10 + 16<<10
			const cursor = fetchCursorWork - fetchCoreWork
			first, second := int64(cursor), int64(legacy)
			if legacyFirst {
				first, second = second, first
			}
			a := b.reserve(first)
			if a == nil {
				t.Fatalf("budget %d: the first path got no work allowance", budget)
			}
			z := b.reserve(second)
			if budget == 83<<20 {
				if z != nil || b.used > b.limit {
					t.Fatal("both fetch paths spent the same 83 MiB budget allowance")
				}
				if !a.release() {
					t.Fatal("the acknowledged first path could not release its work")
				}
				z = b.reserve(second)
			} else if z == nil || b.used != 2960<<10 {
				t.Fatalf("budget %d: coexistence charged %d, want 2960 KiB", budget, b.used)
			}
			if z == nil || !z.release() {
				t.Fatal("the second path did not make progress after credit returned")
			}
			a.release()
			if b.used != fetchCoreWork {
				t.Fatalf("budget %d retained %d work bytes after both owners released", budget, b.used)
			}
		}
	}
}

func TestFetchWorkCancellationWaitsForAcknowledgement(t *testing.T) {
	b := newFetchWorkBook(83 << 20)
	a := b.reserve(fetchCursorWork - fetchCoreWork)
	z := b.reserve(16 << 10)
	if !a.start() || z.start() {
		t.Fatal("the single worker ran two fetches")
	}
	a.cancel()
	if a.release() || b.used != fetchCursorWork+16<<10 || z.start() {
		t.Fatal("cancelled I/O released live work or its worker slot")
	}
	if !a.acknowledge() || !z.start() {
		t.Fatal("acknowledgement did not hand the worker to ready work")
	}
	if a.acknowledge() || a.release() || b.active != z {
		t.Fatal("a late completion changed the next job's wait")
	}
	z.cancel()
	if !z.acknowledge() || b.used != fetchCoreWork {
		t.Fatal("the final cancelled job did not release exactly once")
	}
}

func TestFetchWorkPendingCompletionKeepsItsReservation(t *testing.T) {
	b := newFetchWorkBook(83 << 20)
	a := b.reserve(b.limit - fetchCoreWork)
	if a == nil || !a.start() {
		t.Fatal("the worker could not take its allowance")
	}
	// Worker execution has finished, but its callback has not run on
	// the loop. Neither retained work nor new work can reuse its bytes.
	if b.reserve(1) != nil || a.release() {
		t.Fatal("a posted completion's allocation was counted as free")
	}
	if !a.acknowledge() || b.reserve(1) != nil {
		t.Fatal("acknowledging a retained result implicitly released it")
	}
	if !a.release() || b.used != fetchCoreWork {
		t.Fatal("consuming the result did not release its allocation")
	}
}
