package wl

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestReplyStageIncludesPageAndIndexCapacity(t *testing.T) {
	for _, tc := range []struct{ wire, capacity int }{
		{3928 << 10, 3968 << 10},
		{4872 << 10, 4928 << 10},
		{3376 << 10, 3416 << 10},
		{4240 << 10, 4296 << 10},
		{5104 << 10, 5168 << 10},
	} {
		b := stageCredit{limit: int64(tc.capacity)}
		s := newReplyStage(&b, tc.wire, 0)
		if s == nil || b.used != int64(tc.capacity) {
			t.Fatalf("%d wire bytes: stage capacity %d, want %d", tc.wire, b.used, tc.capacity)
		}
		s.discard()
		if b.used != 0 {
			t.Fatal("discard retained stage credit")
		}
	}
	// One allocation unit covers the controller/first index, including
	// a conservative allocation header. Extra roots have less control.
	if unsafe.Sizeof(replyStage{})+64 > stageUnit || unsafe.Sizeof(stageRoot{})+64 > stageUnit || unsafe.Sizeof(stagePage{}) != stageUnit {
		t.Fatal("the page or index no longer fits its charged allocation unit")
	}
}

func TestReplyStageDeclinesFortyUnfundedReaderFloors(t *testing.T) {
	b := stageCredit{limit: (83<<20)/16 + 2<<20}
	const input = 40 * (128 << 10)
	if b.take(40*(64<<10), input) || b.used != 0 {
		t.Fatal("forty output floors were granted despite their input allocations")
	}
	if newReplyStage(&b, 8192, b.limit) != nil || b.used != 0 {
		t.Fatal("failed admission allocated a stage or changed credit")
	}
}

func TestReplyStageQuarantinesCancelledWorkerLoan(t *testing.T) {
	b := stageCredit{limit: stageCapacity(2 * stagePayload)}
	s := newReplyStage(&b, 2*stagePayload, 0)
	if s == nil || !s.borrow() {
		t.Fatal("the worker got no funded destination")
	}
	to := s.span(0, 100)
	charged := b.used
	s.discard()
	s.discard()
	if b.used != charged || newReplyStage(&b, 1, 0) != nil {
		t.Fatal("cancellation reused an in-flight destination's credit")
	}
	// The loan is still physically valid; a late worker may finish.
	copy(to, "late bytes")
	if !s.acknowledge() || s.acknowledge() {
		t.Fatal("the worker loan did not acknowledge exactly once")
	}
	if b.used != 0 || s.front() != nil {
		t.Fatal("acknowledgement did not release the quarantined destination")
	}
	s.discard()
	if b.used != 0 {
		t.Fatal("an idempotent release changed the ledger twice")
	}
}

func TestReplyStageCommitsOnlyCompleteAcknowledgedBytes(t *testing.T) {
	b := stageCredit{limit: 32 << 10}
	s := newReplyStage(&b, 7, 0)
	s.append([]byte("hello"))
	if s.commit() || s.front() != nil {
		t.Fatal("an incomplete message became output")
	}
	if !s.borrow() {
		t.Fatal("the worker could not borrow the private stage")
	}
	copy(s.span(5, 2), "!!")
	s.written += 2
	if s.commit() {
		t.Fatal("an unacknowledged worker's loan was published")
	}
	if !s.acknowledge() {
		t.Fatal("the worker loan was not acknowledged")
	}
	if !s.commit() || !bytes.Equal(s.front(), []byte("hello!!")) || s.commit() || s.borrow() {
		t.Fatal("the message did not commit exactly once into ordered output")
	}
	s.consume(7)
	if b.used != 0 {
		t.Fatal("completed output retained its stage")
	}
}

func TestReplyStagePartialDrainReleasesBackingPages(t *testing.T) {
	const n = stagePayload*600 + 1
	b := stageCredit{limit: stageCapacity(n)}
	s := newReplyStage(&b, n, 0)
	want := bytes.Repeat([]byte("v"), n)
	s.append(want)
	if !s.commit() {
		t.Fatal("the completed response was not committed")
	}
	before := b.used
	s.consume(100)
	if b.used != before {
		t.Fatal("a partial page was treated as free backing storage")
	}
	s.consume(stagePayload - 100)
	if b.used != before-stageUnit {
		t.Fatal("the consumed page's allocation was not released")
	}
	at := stagePayload
	for p := s.front(); len(p) != 0; p = s.front() {
		if !bytes.Equal(p, want[at:at+len(p)]) {
			t.Fatalf("draining changed the reply at byte %d", at)
		}
		at += len(p)
		s.consume(len(p))
	}
	if at != n || b.used != 0 {
		t.Fatal("the reply's last page or index remained live")
	}
}
