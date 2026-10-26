package simulation

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/gosimlog"
)

// Per-machine globals initialized in setupUserspace:

var (
	linuxOS          *LinuxOS
	gosimOS          *GosimOS // XXX: elsewhere? in machine itself?
	currentMachineID int
)

func CurrentMachineID() int {
	return currentMachineID
}

type gosimSlogHandler struct {
	inner slog.Handler
}

func (w gosimSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return w.inner.Enabled(ctx, level)
}

func (w gosimSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(slog.Int("goroutine", gosimruntime.GetGoroutine()))
	hasStep := false
	for attr := range r.Attrs {
		if attr.Key == "step" {
			hasStep = true
		}
	}
	if !hasStep {
		r.AddAttrs(slog.Int("step", gosimruntime.Step()))
	}
	if gosimruntime.TraceStack.Enabled() {
		r.AddAttrs(gosimlog.Stack(0, r.PC))
	}
	return w.inner.Handle(ctx, r)
}

func (w gosimSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return gosimSlogHandler{
		inner: w.inner.WithAttrs(attrs),
	}
}

func (w gosimSlogHandler) WithGroup(name string) slog.Handler {
	return gosimSlogHandler{
		inner: w.inner.WithGroup(name),
	}
}

type gosimLogWriter struct{}

func (w gosimLogWriter) Write(b []byte) (n int, err error) {
	gosimruntime.WriteLog(b)
	return len(b), nil
}

var (
	logInitialized = false
	logSyscalls    = false
)

func makeBaseSlogHandler() slog.Handler {
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("GOSIM_LOG_LEVEL"))); err != nil {
		panic(err)
	}

	ho := slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}
	return slog.NewJSONHandler(gosimLogWriter{}, &ho)
}

func setupSlog(machineLabel string) {
	// We play a funny game with the logger. There exists a default slog.Logger in
	// every machine, since they all have their own set of globals. All loggers
	// point to the same LogOut writer and are set up using the configuration set
	// here.

	// TODO racedetector: make sure that using slog doesn't introduce sneaky
	// happens-before (this currently happens in the default handler which
	// uses both a pool and a lock)

	// stdout and stderr are currently captured with some special logic in
	// LinuxOS for writes to their file descriptors, see TestStdoutStderr.

	time.Local = time.UTC

	handler := makeBaseSlogHandler()

	// set short file flag so that we'll capture source info. see slog.SetDefault internals
	// XXX: test this?
	log.SetFlags(log.Lshortfile)
	slog.SetDefault(slog.New(gosimSlogHandler{inner: handler}).With("machine", machineLabel))

	logInitialized = true
	logSyscalls = gosimruntime.TraceSyscall.Enabled()
}

func setupUserspace(gosimOS_ *GosimOS, linuxOS_ *LinuxOS, machineID int, label string) {
	// initialize gosimOS etc. before invoking initializers so that init() calls
	// can make syscalls, see TestSyscallsDuringInit.
	gosimOS = gosimOS_
	linuxOS = linuxOS_
	currentMachineID = machineID

	gosimruntime.InitGlobals(false, false)

	// setupSlog only works once globals are initialized.  logs during init are
	// printed to stdout/stderr, see TestLogDuringInit.
	setupSlog(label)
}
