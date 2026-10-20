//go:build planted

package sim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// TestDiag302SnapshotReload cuts seed 34 down to its damaged snapshot:
// entry 11's term byte changes from 4 to 0xa3. Its varint consumes
// the index byte, so it becomes term 1443, index 0, with its old CRC.
// Without B22 the receiver refuses it. With B22 it installs, then WAL recovery refuses
// the log. That refusal must count as a catch, just like a cold restart.
func TestDiag302SnapshotReload(t *testing.T) {
	src, _, err := wal.Open(simfs.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := src.Close(); err != nil {
			t.Error(err)
		}
	})
	var ents []*raftpb.Entry
	for i := uint64(1); i <= 24; i++ {
		ents = append(ents, &raftpb.Entry{Index: new(i), Term: new(uint64(4)), Data: []byte{1}})
	}
	src.Append(nil, ents)
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := core.ServeSnapshot(src, nil, 4, 24, &stream); err != nil {
		t.Fatal(err)
	}
	data := stream.Bytes()
	flipped := false
	for at := 0; at < len(data); {
		kind := data[at]
		n, k := binary.Uvarint(data[at+1:])
		if k <= 0 || n > uint64(len(data)) {
			t.Fatal("invalid generated stream")
		}
		payload := at + 1 + k
		if kind == 4 { // snapshot entry: term, index, type, data
			_, termBytes := binary.Uvarint(data[payload:])
			index, indexBytes := binary.Uvarint(data[payload+termBytes:])
			if index == 11 && indexBytes == 1 {
				data[payload] ^= 0xa7
				flipped = true
				break
			}
		}
		at = payload + int(n) + 4
	}
	if !flipped {
		t.Fatal("entry 11 was not in the stream")
	}
	for _, bug := range []string{"", "B22"} {
		t.Run("bug="+bug, func(t *testing.T) {
			planted.Active = bug
			t.Cleanup(func() { planted.Active = "" })
			disk := simfs.New()
			dst, _, err := wal.Open(disk)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := dst.Close(); err != nil {
					t.Error(err)
				}
			})
			dst.Append(nil, ents[:3])
			if err := dst.Sync(); err != nil {
				t.Fatal(err)
			}
			in := &core.Install{From: 1, Index: 24, Term: 4, Applied: 3}
			err = core.ReceiveSnapshot(bytes.NewReader(data), &core.LogSink{WAL: dst}, dst, in)
			if bug == "" {
				if err == nil || !strings.Contains(err.Error(), "an item does not match its checksum") {
					t.Fatalf("without B22, want a stream CRC refusal, got %v", err)
				}
				t.Logf("without B22: %v", err)
				return
			}
			if err != nil {
				t.Fatalf("with B22, receiver rejected the damaged stream: %v", err)
			}
			if err := dst.Sync(); err != nil {
				t.Fatal(err)
			}
			opened, _, err := wal.Open(disk.Durable())
			if opened != nil {
				if closeErr := opened.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if !errors.Is(err, wal.ErrCorrupt) || !strings.Contains(err.Error(), "entry 0 follows entry 10") {
				t.Fatalf("want seed 34's recovery refusal, got %v", err)
			}
			stopped := errors.Join(fmt.Errorf("%w at 24 from 1, checkpoint 0", core.ErrInstalled), err)
			t.Logf("with B22: %v", stopped)
			if !regexp.MustCompile(plantedCases["B22"].want).MatchString(stopped.Error()) {
				t.Fatalf("B22's matcher rejects its snapshot-reload recovery refusal: %v", stopped)
			}
		})
	}
}

// TestDiag302RecordedFailure uses the original artifact's exact error,
// and checks that accepting it does not also accept an unrelated stop.
func TestDiag302RecordedFailure(t *testing.T) {
	want := regexp.MustCompile(plantedCases["B22"].want)
	for _, tc := range []struct {
		message string
		caught  bool
	}{
		{"seed 34 at 16384 ms: node 3 stopped: core: installed a snapshot at 24 from 1, checkpoint 0\n0000000000000001.wal at byte 706: wal: corrupt log: entry 0 follows entry 10", true},
		{"node 3 stopped: core: installed a snapshot at 24 from 1, checkpoint 0\nsimfs: input/output error", false},
		{"node 3 stopped: wal: corrupt log: entry 0 follows entry 10", false},
		{"liveness: client 1 call timed out", false},
	} {
		if got := want.MatchString(tc.message); got != tc.caught {
			t.Errorf("caught %v, want %v: %s", got, tc.caught, tc.message)
		}
	}
}
