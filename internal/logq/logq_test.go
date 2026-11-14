package logq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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
// the caller of its call, and the standard log package's lines among them.
func TestLinesDoNotWait(t *testing.T) {
	out := newStuck()
	q := New(base(out), 64)
	std := log.New(q, "", 0)
	start := time.Now()
	for i := range 50 {
		q.Infof("line %d", i)
		if i == 25 {
			std.Print("from the log package")
		}
	}
	<-out.waiting()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("51 lines to a stuck output took %v", d)
	}
	time.Sleep(50 * time.Millisecond)
	opened := time.Now()
	out.release()
	q.Stop()
	ws := out.lines(t)
	if len(ws) != 51 {
		t.Fatalf("%d lines written, want 51: %q", len(ws), msgs(ws))
	}
	for i, w := range ws {
		want, caller := fmt.Sprintf("line %d", i), "logq/logq_test.go:"
		switch {
		case i == 26:
			want, caller = "from the log package", ""
		case i > 26:
			want = fmt.Sprintf("line %d", i-1)
		}
		if w.Msg != want || w.TS.After(opened) || !strings.HasPrefix(w.Caller, caller) {
			t.Fatalf("line %d is %+v, want %q logged at %q before %v", i, w, want, caller, opened)
		}
	}
}

// TestDroppedLinesAreReported fills the queue while the output is stuck:
// the lines past it are dropped, and the report of how many is written
// where they were, before the next line queued, or at Stop.
func TestDroppedLinesAreReported(t *testing.T) {
	out := newStuck()
	q := New(base(out), 4)
	q.Infof("line 0")
	// The writer holds line 0 in a write that waits; the queue takes 4
	// more, and drops the 5 after them.
	<-out.waiting()
	for i := 1; i < 10; i++ {
		q.Infof("line %d", i)
	}
	out.release()
	for deadline := time.Now().Add(5 * time.Second); len(out.lines(t)) < 5; {
		if time.Now().After(deadline) {
			t.Fatal("the queued lines were not written")
		}
		time.Sleep(time.Millisecond)
	}
	q.Infof("line 10")
	// Dropped after the last line, reported at Stop.
	q.mu.Lock()
	q.dropped += 2
	q.mu.Unlock()
	q.Stop()
	drop := "dropped %d log lines while the log's output was slow"
	want := []string{"line 0", "line 1", "line 2", "line 3", "line 4", fmt.Sprintf(drop, 5), "line 10", fmt.Sprintf(drop, 2)}
	if got := msgs(out.lines(t)); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("written %q, want %q", got, want)
	}
	// After Stop, no line is written.
	q.Infof("late")
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
	q := New(base(zapcore.AddSync(&buf)), 4)
	arg := blockedString{in: make(chan struct{}), release: make(chan struct{})}
	logged := make(chan struct{})
	go func() {
		defer close(logged)
		q.Infof("%v", arg)
	}()
	<-arg.in
	q.Stop()
	close(arg.release)
	<-logged
	if len(q.lines) != 0 || q.dropped != 1 {
		t.Fatalf("a line logged across Stop: %d queued, %d dropped, want 0 and 1", len(q.lines), q.dropped)
	}
}

// TestCoreChecksTheLines logs through a core that samples and a tee whose
// second output takes only errors: the core's Check decides, as for a
// line logged without the queue (Astra's and Opus's re-checks of #681).
func TestCoreChecksTheLines(t *testing.T) {
	var sampled, all, errs bytes.Buffer
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	sampler := zapcore.NewSamplerWithOptions(zapcore.NewCore(enc, zapcore.AddSync(&sampled), zapcore.InfoLevel), time.Hour, 100, 100)
	tee := zapcore.NewTee(zapcore.NewCore(enc, zapcore.AddSync(&all), zapcore.InfoLevel), zapcore.NewCore(enc, zapcore.AddSync(&errs), zapcore.ErrorLevel))
	q := New(zap.New(zapcore.NewTee(sampler, tee)), 2000)
	for range 1000 {
		q.Infof("the same line")
	}
	q.Stop()
	if n := strings.Count(sampled.String(), "\n"); n != 109 {
		t.Fatalf("%d of 1000 sampled lines written, want 109: the first 100 and every 100th after", n)
	}
	if n := strings.Count(all.String(), "\n"); n != 1000 {
		t.Fatalf("%d of 1000 lines written to the unsampled output", n)
	}
	if errs.Len() != 0 {
		t.Fatalf("an info line went to the output of errors: %q", errs.String()[:min(errs.Len(), 200)])
	}
}

// TestStopDoesNotWaitForever stops a Log whose output stays stuck.
func TestStopDoesNotWaitForever(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for stopWait")
	}
	out := newStuck()
	q := New(base(out), 4)
	q.Infof("line 0")
	<-out.waiting()
	start := time.Now()
	q.Stop()
	if d := time.Since(start); d < stopWait || d > stopWait+time.Second {
		t.Fatalf("Stop took %v with a stuck output, want about %v", d, stopWait)
	}
	out.release()
}
