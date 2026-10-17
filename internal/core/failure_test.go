package core

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func TestFailureOf(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Failure
	}{
		{fmt.Errorf("%w: %w", ErrDisk, io.ErrShortWrite), Stop},
		{ErrCommitStalled, Stop},
		{tooOld(3, 2), Stop},
		{errors.New("anything else"), Stop},
		// A damaged stream from the leader is the leader's, not ours.
		{fmt.Errorf("%w: %w", ErrFetch, errBadCheckpoint), Reload},
		{fmt.Errorf("%w: %w", ErrFetch, io.ErrUnexpectedEOF), Reload},
		{&wal.RecordError{Entry: 7, Segment: 1, Reason: "does not verify"}, Fence},
		{errBadCheckpoint, Fence},
		{fmt.Errorf("read: %w", mvcc.ErrBadValue), Fence},
		{ErrRemoved, Leave},
	} {
		if got := FailureOf(tc.err); got != tc.want {
			t.Errorf("FailureOf(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// LogSink marks a failed write or sync of its own disk ErrDisk, so both
// drivers stop on it, and anything wrong with the stream not, so both
// reload.
func TestLogSinkErrors(t *testing.T) {
	d := simfs.New()
	w, _, err := wal.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	sink := &LogSink{WAL: w}
	if err := sink.CheckpointDone(5); err == nil || errors.Is(err, ErrDisk) {
		t.Fatalf("a checkpoint with no data: %v, want a stream error", err)
	}
	d.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpWrite {
			return simfs.ErrIO
		}
		return nil
	}
	if err := sink.CheckpointChunk(5, []byte("chunk")); !errors.Is(err, ErrDisk) || !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("a failed checkpoint write: %v, want ErrDisk", err)
	}
	d.Fault = nil
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.CheckpointChunk(5, []byte("chunk")); !errors.Is(err, fs.ErrExist) || errors.Is(err, ErrDisk) {
		t.Fatalf("a checkpoint an earlier install wrote: %v, want fs.ErrExist alone", err)
	}
}

func TestFetchFailed(t *testing.T) {
	disk := fmt.Errorf("%w: %w", ErrDisk, io.ErrShortWrite)
	synced := 0
	sync := func() error { synced++; return nil }
	if err := FetchFailed(disk, sync); FailureOf(err) != Stop || synced != 0 {
		t.Fatalf("a failed write of our disk: %v, %d syncs; want Stop and none", err, synced)
	}
	if err := FetchFailed(io.ErrUnexpectedEOF, sync); FailureOf(err) != Reload || synced != 1 {
		t.Fatalf("a cut stream: %v, %d syncs; want Reload after one", err, synced)
	}
	err := FetchFailed(io.ErrUnexpectedEOF, func() error { return io.ErrClosedPipe })
	if FailureOf(err) != Stop || !errors.Is(err, ErrDisk) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a cut stream whose sync failed: %v, want Stop with both causes", err)
	}
}
