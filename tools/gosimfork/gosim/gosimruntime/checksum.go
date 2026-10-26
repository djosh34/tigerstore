package gosimruntime

import (
	"context"
	"encoding/binary"
	"log/slog"
)

type checksummer struct {
	step   int
	hash   fnv64
	logger *slog.Logger
}

func newChecksummer(logger *slog.Logger) *checksummer {
	return &checksummer{
		step:   0,
		hash:   newFnv64(),
		logger: logger,
	}
}

//go:generate go run golang.org/x/tools/cmd/stringer -type=checksumKey

type checksumKey byte

const (
	checksumKeyRunPick checksumKey = iota
	checksumKeyRunResult
	checksumKeyLogWrite
	checksumKeyCreating
	checksumKeyRunStarted
	checksumKeyRunFinished
	checksumKeyTimeNow
)

func (t *checksummer) recordIntInt(key checksumKey, a, b uint64) {
	if inControlledNondeterminism() {
		panic("help")
	}

	t.hash.Hash([]byte{byte(key)})
	t.hash.HashInt(a)
	t.hash.HashInt(b)

	level := slog.LevelDebug
	if *forceChecksum {
		level = slog.LevelError
	}
	if t.logger.Enabled(context.TODO(), level) {
		t.logger.LogAttrs(context.TODO(), level, "checksummer",
			slog.Int("step", t.step),
			slog.String("key", key.String()),
			slog.Int("a", int(a)),
			slog.Int("b", int(b)),
			slog.Int64("sum", int64(t.hash)))
	}
	t.step++
}

func (t *checksummer) recordBytes(key checksumKey, a []byte) {
	if inControlledNondeterminism() {
		panic("help")
	}

	t.hash.Hash([]byte{byte(key)})
	t.hash.Hash(a)

	level := slog.LevelDebug
	if *forceChecksum {
		level = slog.LevelError
	}
	if t.logger.Enabled(context.TODO(), level) {
		t.logger.LogAttrs(context.TODO(), level, "checksummer",
			slog.Int("step", t.step),
			slog.String("key", key.String()),
			slog.String("a", string(a)),
			slog.Int64("sum", int64(t.hash)))
	}
	t.step++
}

func (t *checksummer) finalize() []byte {
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(t.hash))
	return n[:]
}

type checksumWriter struct {
	checksummer *checksummer
}

func (t checksumWriter) Write(p []byte) (n int, err error) {
	t.checksummer.recordBytes(checksumKeyLogWrite, p)
	return len(p), nil
}
