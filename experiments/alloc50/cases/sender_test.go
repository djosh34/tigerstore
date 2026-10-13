package main

import (
	"net"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

func fixtureSender(t *testing.T, conn net.Conn, planned uint64, timeout time.Duration) *slowSender {
	t.Helper()
	s := &slowSender{conn: conn, fr: http2.NewFramer(conn, conn), header: []byte{0x83}, data: []byte{0, 0, 0, 0, 1, 'v'}, planned: planned}
	if err := s.start(timeout); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.closeAndWait(time.Second) })
	return s
}

func TestSenderCompleteCounts(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	read := make(chan error, 1)
	go func() {
		fr := http2.NewFramer(server, server)
		for range 6 {
			if _, err := fr.ReadFrame(); err != nil {
				read <- err
				return
			}
		}
		read <- nil
	}()
	s := fixtureSender(t, client, 3, time.Second)
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not finish")
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	j, err := s.closeAndWait(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempted != 3 || j.Headers != 3 || j.Data != 3 || j.DataBytes != 18 || !j.AllDataSent {
		t.Fatalf("bad counters: %+v", j)
	}
	if err := validateSender(j, 3, false); err != nil {
		t.Fatal(err)
	}
}

func TestSenderBlockedPrefixJoinsOnIntentionalClose(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	read := make(chan error, 1)
	go func() {
		fr := http2.NewFramer(server, server)
		for range 2 {
			if _, err := fr.ReadFrame(); err != nil {
				read <- err
				return
			}
		}
		// Stop reading with the next HEADERS in progress.
		read <- nil
	}()
	s := fixtureSender(t, client, 4, time.Second)
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for s.attempted.Load() < 2 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	j, err := s.closeAndWait(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempted != 2 || j.Headers != 1 || j.Data != 1 || j.DataBytes != 6 || !j.ExpectedCloseError || j.ErrorBeforeClose {
		t.Fatalf("bad prefix: %+v", j)
	}
	if err := validateSender(j, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := validateSender(j, 2, true); err != nil {
		t.Fatal("last in-progress DATA allowance rejected:", err)
	}
	if err := validateSender(j, 3, true); err == nil {
		t.Fatal("commits beyond DATA+1 accepted")
	}
	if err := validateSender(j, 1, false); err == nil {
		t.Fatal("baseline prefix accepted")
	}
}

func TestSenderDeadlineBeforeCloseIsFailure(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	s := fixtureSender(t, client, 4, 20*time.Millisecond)
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("deadline did not stop blocked writer")
	}
	if err := s.check(); err == nil {
		t.Fatal("premature timeout accepted")
	}
	j, err := s.closeAndWait(time.Second)
	if err == nil || !j.ErrorBeforeClose || j.ExpectedCloseError {
		t.Fatalf("timeout masked by cleanup: %+v err=%v", j, err)
	}
	if err := validateSender(j, 1, true); err == nil {
		t.Fatal("timeout outcome accepted")
	}
}

func TestSenderCounterConsistency(t *testing.T) {
	j := senderSnapshot{Planned: 4096, Attempted: 4096, Headers: 4096, Data: 4096, DataBytes: 24576, MessageBytes: 6, Finished: true, AllDataSent: true, IntentionalClose: true}
	if err := validateSender(j, 256, true); err != nil {
		t.Fatal(err)
	}
	broken := j
	broken.Headers = 4097
	if err := validateSender(broken, 256, true); err == nil {
		t.Fatal("headers beyond attempts accepted")
	}
	broken = j
	broken.DataBytes--
	if err := validateSender(broken, 256, true); err == nil {
		t.Fatal("inconsistent bytes accepted")
	}
	broken = j
	broken.Error = "premature reset"
	broken.ErrorBeforeClose = true
	if err := validateSender(broken, 256, true); err == nil {
		t.Fatal("premature error accepted")
	}
}
