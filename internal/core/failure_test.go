package core

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/djosh34/tigerstore/internal/mvcc"
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
