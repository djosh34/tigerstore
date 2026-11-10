package k8strace

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
)

// Trace is one recording window of one cluster shape, ready to replay: the
// store's keys and leases when the window opened, the watches the
// apiserver had open, and every call it sent in the window, with the
// earlier replies each call waited on.
//
// Keys are indices into Keys. Revisions and lease IDs are references to
// what the replay's own replies gave (Rev, LeaseRef), never the recorded
// numbers.
type Trace struct {
	Shape string
	// About says how the recording was made: the shape, the commit, the
	// Actions run and the window.
	About   string
	Seconds float64 // the window's length
	Keys    []string
	Leases  []int64 // TTLs of the leases alive when the window opened
	Preload []Item
	Watches []WatchStream
	Calls   []Call
}

// Item is a key alive when the window opened. Lease is a LeaseRef.
type Item struct {
	Key   int32
	Size  int32
	Lease LeaseRef
}

// WatchStream is one Watch call of the apiserver and the watches it
// created on it.
type WatchStream struct {
	Creates []Watch
}

// Watch is one watch: End is -1 for a single key.
type Watch struct {
	Key, End int32
	PrevKV   bool
	Progress bool
	Filters  []int32
}

// The kinds of a Call.
const (
	CallRange uint8 = iota
	CallRangeStream
	CallPut
	CallDelete
	CallTxn
	CallCompact
	CallLeaseGrant
	CallLeaseRevoke
	CallLeaseKeepAlive
	CallLeaseTTL
	// CallProgress is a progress request on a watch stream, which the
	// store answers with its revision.
	CallProgress
	CallKinds
)

// KindNames are the kinds' names, as reports print them.
var KindNames = [CallKinds]string{"range", "range-stream", "put", "delete", "txn", "compact", "lease-grant", "lease-revoke", "lease-keepalive", "lease-ttl", "watch-progress"}

// Call is one call of the window.
type Call struct {
	At   int64 // when it was sent, in ns since the window opened
	Kind uint8
	// Waits are the calls whose replies it waited on.
	Waits []int32
	// Op is the request of a Range, RangeStream, Put, Delete or Txn.
	Op Op
	// Rev is a Compact's revision.
	Rev Rev
	// TTL is a LeaseGrant's; Lease names the lease of the other lease
	// calls.
	TTL   int64
	Lease LeaseRef
	// Stream is a progress request's watch stream, in Watches.
	Stream int32
	// Failed is set when its Txn's compares failed when it was
	// recorded.
	Failed bool
}

// The kinds of an Op.
const (
	OpRange uint8 = iota
	OpPut
	OpDelete
	OpTxn
)

// The flags of an Op.
const (
	FlagSerializable uint8 = 1 << iota
	FlagKeysOnly
	FlagCountOnly
	FlagPrevKV
	FlagIgnoreValue
	FlagIgnoreLease
)

// Op is a request, or one operation of a Txn.
type Op struct {
	Kind  uint8
	Flags uint8
	Key   int32
	End   int32 // -1 for a single key
	Limit int64 // Range
	Rev   Rev   // Range: the revision it reads at
	Size  int32 // Put: the value's length
	Lease LeaseRef
	// Txn.
	Compare          []Compare
	Success, Failure []Op
}

// Compare is a Txn's compare. Rev is the number compared against for a
// mod or create revision; Value is it for a version or a value's length.
type Compare struct {
	Target, Result int32
	Key, End       int32
	Rev            Rev
	Value          int64
}

// Rev names a revision of the replay. Zero is revision 0; n > 0 is the
// revision of the reply to Calls[n-1]; n < 0 is the revision the replay's
// preload gave Preload[-n-1]. RevStale is a revision the recording learned
// before its window and that no key had any more when the window opened:
// it compares unequal to every live key's, as it did when it was
// recorded.
type Rev int64

// RevStale stands for a revision older than every key of the replay.
const RevStale Rev = 1 << 62

// LeaseRef names a lease of the replay. Zero is none; n > 0 is the lease
// Calls[n-1] granted; n < 0 is the preload's lease Leases[-n-1].
type LeaseRef int32

// Write writes t, gzipped, to path.
func (t *Trace) Write(path string) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	gz := gzip.NewWriter(f)
	if _, err := gz.Write(t.encode()); err != nil {
		return err
	}
	return gz.Close()
}

func readGzip(r io.Reader) ([]byte, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(gz)
}

// Read reads a trace that Write wrote.
func Read(path string) (*Trace, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b, err := readGzip(f)
	if err := errors.Join(err, f.Close()); err != nil {
		return nil, err
	}
	t, err := decode(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}
