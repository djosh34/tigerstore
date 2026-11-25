package wl

import (
	"context"
	"net"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
)

// Sys is the simulator's stand-in for the system calls a node makes that
// the simulator cannot run as they are: epoll, eventfd, O_DIRECT opens
// and the raw reads and writes of its sockets and files (sys_os.go). A
// real binary leaves Config.Sys nil and refuses one; a binary built with
// the tag sim or simio runs the node on it (sys_sim.go).
//
// File descriptors are the simulator's, one table for every node of a
// run: the calls that take only a descriptor go to the Sys of the first
// node started, and every node's Sys must share that table.
type Sys interface {
	EpollCreate() (int, error)
	EpollCtl(ep, op, fd int, events uint32, tag int32) error
	// EpollWait waits up to ms milliseconds, -1 for no limit, as
	// epoll_wait does, and fills events with what is ready.
	EpollWait(ep int, events []unix.EpollEvent, ms int) (int, error)
	Eventfd(nonblock bool) (int, error)
	Open(path string, flags int, mode uint32) (int, error)
	// OpenDirect opens a log segment for writes that are durable once
	// they return.
	OpenDirect(path string) (int, error)

	Read(fd int, p []byte) (int, error)
	Write(fd int, p []byte) (int, error)
	Pread(fd int, p []byte, off int64) (int, error)
	// PreadNoWait is Pread of what the page cache holds, as preadv2
	// with RWF_NOWAIT: it fails with EAGAIN, or reads short, where it
	// would wait for the disk.
	PreadNoWait(fd int, p []byte, off int64) (int, error)
	Pwrite(fd int, p []byte, off int64) (int, error)
	Fsync(fd int) error
	// Allocate allocates fd's space up to size and keeps its size, as
	// fallocate with FALLOC_FL_KEEP_SIZE does.
	Allocate(fd int, size int64) error
	Close(fd int) error
	// Seek moves fd's offset as lseek does.
	Seek(fd int, off int64, whence int) (int64, error)

	// The node's files are under its data dir; their errors are the
	// system's, so that a missing file is os.ErrNotExist.
	MkdirAll(path string) error
	// FreeSpace returns the room left on the file system of path, as
	// statfs's blocks available to an unprivileged user.
	FreeSpace(path string) (int64, error)
	ReadDir(dir string) ([]string, error)
	Remove(path string) error
	Rename(oldPath, newPath string) error
	Truncate(path string, size int64) error

	// Dial connects to a peer at addr.
	Dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error)
	// ConnFD takes nc, a connection the node dialed or accepted, as a
	// non-blocking descriptor; nc is the descriptor's from then on.
	ConnFD(nc net.Conn) (int, error)
	// Blocking makes socket fd's reads and writes block, failing after
	// timeout, or never for 0; Nonblocking makes them not block again.
	Blocking(fd int, timeout time.Duration) error
	Nonblocking(fd int) error
}

// Hooks, in a binary built for the simulator, let it see what a node
// recovers, applies and makes durable, and hold its loop, as a paused
// process (hook_sim.go). A real binary refuses them, and its calls of
// them are empty functions (hook_os.go), so they cost nothing.
type Hooks interface {
	// Recovered is what the node's start recovered: Raft's log to last,
	// and the hard state. values are the values the store it started
	// from names in the log (as Turn's).
	Recovered(last uint64, hs *raftpb.HardState, values func() []LogValue)
	// Append is an append handed to the log: its first entry, or that of
	// the snapshot it installs, which may replace entries already
	// durable, as Raft may replace entries that never committed, and its
	// last entry, or the snapshot's, or 0 for a hard state alone.
	Append(first, last uint64, install bool)
	// Applied is entry e, applied, and a hash of the changes it made to
	// the store: every key it put or deleted, by name, with its record at
	// that revision. Every node that applies e from the same store gets
	// the same hash, whatever its key ids. state is the hash of the
	// store after e, as a checkpoint at e would hold it, the same on
	// every node with the same state; it costs a pass over the store.
	Applied(e *raftpb.Entry, hash uint64, state func() uint64)
	// Durable is an append the log now holds durably: its entries to last
	// if it had any, the snapshot it installed to last if install, and
	// its hard state if any.
	Durable(last uint64, install bool, hs *raftpb.HardState)
	// Expired is lease id, which the node expires now: it proposes the
	// lease's revoke.
	Expired(id int64)
	// Checkpoint is a checkpoint that starts, of the store after entry
	// index, whose state has hash state (Applied): the node must have
	// applied every entry to index and none past it.
	Checkpoint(index, state uint64)
	// Installing is a snapshot at index that the loop starts to install,
	// and Installed the install done: the node answers no client in
	// between. An install that fails stops the node, with no Installed.
	Installing(index uint64)
	Installed(index uint64)
	// Loaded is a store loaded from the checkpoint at index, by a start or
	// a snapshot's install, whose state has hash state (Applied).
	Loaded(index, state uint64)
	// CheckpointDone is that checkpoint, done: its file, name in the
	// node's checkpoint directory, is durable and the newest.
	CheckpointDone(index uint64, name string)
	// Turn ends each turn of the loop with what Raft shows; it may hold
	// the loop. values are the values the store names in the log's
	// durable part, in the order of their place. A hook that keeps
	// values keeps the whole node: the simulator keeps only the state.
	Turn(st TurnState, values func() []LogValue)
	// SnapshotChunk is chunk p of a snapshot stream to node to, the
	// stream's first if first, before it is sent: the simulator may hold
	// the stream back or damage p in place, as a network that TLS does
	// not guard would.
	// It returns how many bytes of p to send: fewer cut the stream after
	// them, as its source crashing there would, and SnapshotCut follows.
	SnapshotChunk(to uint64, first bool, p []byte) (keep int)
	// SnapshotCut is a stream to node to that SnapshotChunk cut, once
	// the bytes it kept are written: the simulator crashes the source.
	SnapshotCut(to uint64)
	// Deliver is message m from a peer, before Raft steps it: the
	// simulator drops it when it returns false, as a gray node's links
	// lose its appends.
	Deliver(m *raftpb.Message) bool
}

// TurnState is what a node shows at the end of a turn of its loop.
type TurnState struct {
	Lead, Term, Commit, Applied uint64
	// First is the first entry Raft's storage holds.
	First uint64
	// Format is the cluster's format (format.go).
	Format uint8
	// Rev is the store's revision.
	Rev int64
	// Watchers counts the node's watchers.
	Watchers int
	// Silent says the node leads on a stalled disk and sends its peers
	// nothing (silent.go).
	Silent bool
	// Members are the members as of the applied index, in ID order, and
	// Removed the IDs of those the cluster removed, in order. Both are
	// the node's own and change only when the members do: a hook keeps
	// them as they are.
	Members []TurnMember
	Removed []uint64
}

// LogValue is a value the store names in the log: its Len bytes at Off.
type LogValue struct {
	Off int64
	Len int
}

// SegmentPlace is where log offset off lies: in segment k, at at.
func SegmentPlace(off int64) (k, at int64) { return off / segSize, off % segSize }

// TurnMember is a member as TurnState shows it.
type TurnMember struct {
	ID       uint64
	PeerURLs []string
	Learner  bool
}
