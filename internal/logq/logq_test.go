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

// written is a line as the output got it.
type written struct {
	Msg    string    `json:"msg"`
	TS     time.Time `json:"ts"`
	Caller string    `json:"caller"`
}

// lines are the lines written.
func (s *stuck) lines(t *testing.T) []written {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []written
	for line := range strings.SplitSeq(strings.TrimSpace(s.b.String()), "\n") {
		if line == "" {
			continue
		}
		var w written
		if err := json.Unmarshal([]byte(line), &w); err != nil {
			t.Fatalf("a log line %q: %v", line, err)
		}
		out = append(out, w)
	}
	return out
}

func msgs(ws []written) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.Msg)
	}
	return out
}

// base is a logger to w, with the caller, as etcd's is.
func base(w zapcore.WriteSyncer) *zap.Logger {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	return zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.Lock(w), zapcore.InfoLevel), zap.AddCaller())
}

// TestLinesDoNotWait logs while the output is stuck: no line waits for
// it, and once it opens every line is written in order, with the time and
// the caller of its call.
func TestLinesDoNotWait(t *testing.T) {
	out := newStuck()
	b := base(out)
	q := New(b, 64)
	lg := q.Logger(b).Sugar()
	start := time.Now()
	for i := range 50 {
		lg.Infof("line %d", i)
	}
	<-out.waiting()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("50 lines to a stuck output took %v", d)
	}
	time.Sleep(50 * time.Millisecond)
	opened := time.Now()
	out.release()
	q.Stop()
	ws := out.lines(t)
	if len(ws) != 50 {
		t.Fatalf("%d lines written, want 50: %q", len(ws), msgs(ws))
	}
	for i, w := range ws {
		if w.Msg != fmt.Sprintf("line %d", i) || w.TS.After(opened) || !strings.HasPrefix(w.Caller, "logq/logq_test.go:") {
			t.Fatalf("line %d is %+v, logged here before %v", i, w, opened)
		}
	}
}

// TestDroppedLinesAreReported fills the queue while the output is stuck:
// the lines past it are dropped, and the report of how many is written
// where they were, before the next line queued, or at Stop.
func TestDroppedLinesAreReported(t *testing.T) {
	out := newStuck()
	b := base(out)
	q := New(b, 4)
	lg := q.Logger(b).Sugar()
	lg.Infof("line 0")
	// The writer holds line 0 in a write that waits; the queue takes 4
	// more, and drops the 5 after them.
	<-out.waiting()
	for i := 1; i < 10; i++ {
		lg.Infof("line %d", i)
	}
	out.release()
	for deadline := time.Now().Add(5 * time.Second); len(out.lines(t)) < 5; {
		if time.Now().After(deadline) {
			t.Fatal("the queued lines were not written")
		}
		time.Sleep(time.Millisecond)
	}
	lg.Infof("line 10")
	// Dropped while the writer is stopped, reported at Stop.
	q.dropped.Add(2)
	q.Stop()
	drop := "dropped %d log lines while the log's output was slow"
	want := []string{"line 0", "line 1", "line 2", "line 3", "line 4", fmt.Sprintf(drop, 5), "line 10", fmt.Sprintf(drop, 2)}
	if got := msgs(out.lines(t)); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("written %q, want %q", got, want)
	}
	// After Stop, no line is written.
	lg.Infof("late")
	if got := out.lines(t); len(got) != len(want) {
		t.Fatalf("a line after Stop was written: %q", msgs(got))
	}
}

// blockedString waits in String until released, as a slow argument
// being formatted.
type blockedString struct {
	in, release chan struct{}
}

func (b blockedString) String() string {
	close(b.in)
	<-b.release
	return "late"
}

// TestStopDuringFormatting stops a Log while a line is being formatted:
// the line, which reaches the queue after Stop, is dropped and counted,
// never left in the queue (Astra's review of #681).
func TestStopDuringFormatting(t *testing.T) {
	var buf bytes.Buffer
	lg := base(zapcore.AddSync(&buf))
	q := New(lg, 4)
	arg := blockedString{in: make(chan struct{}), release: make(chan struct{})}
	logged := make(chan struct{})
	go func() {
		defer close(logged)
		q.Logger(lg).Sugar().Infof("%v", arg)
	}()
	<-arg.in
	q.Stop()
	close(arg.release)
	<-logged
	if len(q.lines) != 0 || q.dropped.Load() != 1 {
		t.Fatalf("a line logged across Stop: %d queued, %d dropped, want 0 and 1", len(q.lines), q.dropped.Load())
	}
}

// TestStopDoesNotWaitForever stops a Log whose output stays stuck.
func TestStopDoesNotWaitForever(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for stopWait")
	}
	out := newStuck()
	b := base(out)
	q := New(b, 4)
	q.Logger(b).Info("line 0")
	<-out.waiting()
	start := time.Now()
	q.Stop()
	if d := time.Since(start); d < stopWait || d > stopWait+time.Second {
		t.Fatalf("Stop took %v with a stuck output, want about %v", d, stopWait)
	}
	out.release()
}
