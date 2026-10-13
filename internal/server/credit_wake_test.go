package server

import "testing"

func TestCreditWakesOnlyForQueuedUpdate(t *testing.T) {
	c := testConn()
	defer c.cancel()
	c.credit(1024)
	if len(c.wake) != 0 || len(c.ctrl) != 0 || c.recvCredit != 1024 {
		t.Fatalf("small credit: wakes=%d controls=%d credit=%d", len(c.wake), len(c.ctrl), c.recvCredit)
	}
	c.credit(connWindow/4 - 1024)
	if len(c.wake) != 1 || len(c.ctrl) != 1 || c.recvCredit != 0 {
		t.Fatalf("threshold credit: wakes=%d controls=%d credit=%d", len(c.wake), len(c.ctrl), c.recvCredit)
	}
	if f := c.ctrl[0]; f.kind != ctrlWindowUpdate || f.id != 0 || f.val != connWindow/4 {
		t.Fatalf("wrong window update: %+v", f)
	}
}

func TestCreditBackpressureReleaseStillWakes(t *testing.T) {
	c := testConn()
	defer c.cancel()
	st := testCall(c, 1)
	st.replyHeld = maxHeld + 1
	c.held = st.replyHeld
	c.credit(connWindow / 4)
	if len(c.wake) != 0 || len(c.ctrl) != 0 || c.recvCredit != connWindow/4 {
		t.Fatalf("held credit: wakes=%d controls=%d credit=%d", len(c.wake), len(c.ctrl), c.recvCredit)
	}
	// Reset releases the last charge and must wake the writer for the
	// connection window update even when it sends no RST_STREAM frame.
	c.resetStream(st.id, 0)
	if len(c.wake) != 1 || len(c.ctrl) != 1 || c.recvCredit != 0 || c.held != 0 {
		t.Fatalf("released credit: wakes=%d controls=%d credit=%d held=%d", len(c.wake), len(c.ctrl), c.recvCredit, c.held)
	}
}

func TestCreditWakeSurvivesReaderBeforeWriterWait(t *testing.T) {
	c := testConn()
	defer c.cancel()
	// The buffered wake token must survive until the writer waits. No
	// reader-to-writer handshake or timing delay is needed.
	c.credit(connWindow / 4)
	select {
	case <-c.wake:
	default:
		t.Fatal("window update lost its wake before the writer waited")
	}
	c.mu.Lock()
	queued := len(c.ctrl)
	c.mu.Unlock()
	if queued != 1 {
		t.Fatalf("writer woke with %d queued controls", queued)
	}
}
