package logq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// stuck is a log output whose writes wait while it is shut, as a file on
// a disk that stalls, and that keeps what it was written.
type stuck struct {
	mu      sync.Mutex
	b       bytes.Buffer
	open    chan struct{}
	entered chan struct{}
	once    sync.Once
}

// newStuck returns a shut output.
func newStuck() *stuck {
	return &stuck{open: make(chan struct{}), entered: make(chan struct{})}
}

func (s *stuck) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.open
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *stuck) Sync() error { return nil }

// release lets the writes through.
func (s *stuck) release() { close(s.open) }

// waiting closes once a write waits.
func (s *stuck) waiting() <-chan struct{} { return s.entered }

// lines are the messages written, and the time of each.
func (s *stuck) lines(t *testing.T) ([]string, []time.Time) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var msgs []string
	var times []time.Time
	for line := range strings.SplitSeq(strings.TrimSpace(s.b.String()), "\n") {
		if line == "" {
			continue
		}
		var e struct {
			Msg string    `json:"msg"`
			TS  time.Time `json:"ts"`
		}
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("a log line %q: %v", line, err)
		}
		msgs = append(msgs, e.Msg)
		times = append(times, e.TS)
	}
	return msgs, times
}

func logger(w zapcore.WriteSyncer) *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	return zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.Lock(w), zapcore.InfoLevel))
}

// TestInfofDoesNotWait logs while the output is stuck: no line waits for
// it, and once it opens every line is written in order, stamped with the
// time it was logged.
func TestInfofDoesNotWait(t *testing.T) {
	out := newStuck()
	l := New(logger(out), 64)
	start := time.Now()
	for i := range 50 {
		l.Infof("line %d", i)
	}
	<-out.waiting()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("50 lines to a stuck output took %v", d)
	}
	time.Sleep(50 * time.Millisecond)
	opened := time.Now()
	out.release()
	l.Stop()
	msgs, times := out.lines(t)
	if len(msgs) != 50 {
		t.Fatalf("%d lines written, want 50: %q", len(msgs), msgs)
	}
	for i, m := range msgs {
		if m != fmt.Sprintf("line %d", i) || times[i].After(opened) {
			t.Fatalf("line %d is %q at %v, logged before %v", i, m, times[i], opened)
		}
	}
}

// TestDroppedLinesAreReported fills the queue while the output is stuck:
// the lines past it are dropped, and the report of how many is written
// where they were, before the next line queued, or at Stop.
func TestDroppedLinesAreReported(t *testing.T) {
	out := newStuck()
	l := New(logger(out), 4)
	l.Infof("line 0")
	// The writer holds line 0 in a write that waits; the queue takes 4
	// more, and drops the 5 after them.
	<-out.waiting()
	for i := 1; i < 10; i++ {
		l.Infof("line %d", i)
	}
	out.release()
	for deadline := time.Now().Add(5 * time.Second); ; {
		if msgs, _ := out.lines(t); len(msgs) == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the queued lines were not written")
		}
		time.Sleep(time.Millisecond)
	}
	l.Infof("line 10")
	// Dropped while the writer is stopped, reported at Stop.
	l.dropped.Add(2)
	l.Stop()
	msgs, _ := out.lines(t)
	drop := "dropped %d log lines while the log's output was slow"
	want := []string{"line 0", "line 1", "line 2", "line 3", "line 4", fmt.Sprintf(drop, 5), "line 10", fmt.Sprintf(drop, 2)}
	if strings.Join(msgs, "|") != strings.Join(want, "|") {
		t.Fatalf("written %q, want %q", msgs, want)
	}
	// After Stop, no line is written.
	l.Infof("late")
	if msgs, _ := out.lines(t); len(msgs) != len(want) {
		t.Fatalf("a line after Stop was written: %q", msgs)
	}
}

// TestLinesNameTheirCaller checks that a line names where it was logged,
// not this package.
func TestLinesNameTheirCaller(t *testing.T) {
	var b bytes.Buffer
	l := New(logger(zapcore.AddSync(&b)), 4)
	l.Infof("here")
	l.Stop()
	var e struct {
		Caller string `json:"caller"`
	}
	if err := json.Unmarshal(b.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(e.Caller, "logq/logq_test.go:") {
		t.Fatalf("the line names %q as its caller", e.Caller)
	}
}

// TestStopDoesNotWaitForever stops a Log whose output stays stuck.
func TestStopDoesNotWaitForever(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for stopWait")
	}
	out := newStuck()
	l := New(logger(out), 4)
	l.Infof("line 0")
	<-out.waiting()
	start := time.Now()
	l.Stop()
	if d := time.Since(start); d < stopWait || d > stopWait+time.Second {
		t.Fatalf("Stop took %v with a stuck output, want about %v", d, stopWait)
	}
	out.release()
}
