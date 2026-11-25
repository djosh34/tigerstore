//go:build !(sim || simio)

package wl

import pb "go.etcd.io/raft/v3/raftpb"

// The simulator's hooks (sys.go's Hooks, hook_sim.go). A real binary has
// none: each call below is an empty function, which the compiler drops.

func (*node) simRecovered()         {}
func (*node) simAppend(*pb.Message) {}
func (*node) simApplied(*pb.Entry)  {}
func (*node) simDurable(*walJob)    {}
func (*node) simTurn()              {}
func (*node) simInstalled(uint64)   {}
func (*node) simInstalling(uint64)  {}
func (*node) simInstallDone(uint64) {}
func (*node) simCheckpoint(uint64)  {}

func (*node) simCheckpointDone(uint64, string) {}
func (*node) simExpired(int64)                 {}

func (*node) simSnapshotChunk(_ uint64, _ bool, p []byte) int { return len(p) }
func (*node) simSnapshotCut(uint64)                           {}
func (*node) simDeliver(*pb.Message) bool                     { return true }
