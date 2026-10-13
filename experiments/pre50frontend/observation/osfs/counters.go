//go:build pre50frontend

package osfs

import "sync/atomic"

var writes, writeBytes, syncs atomic.Uint64

// Pre50Stats counts file calls only. Directory syncs keep their original path.
func Pre50Stats() map[string]uint64 {
	return map[string]uint64{"writes": writes.Load(), "write_bytes": writeBytes.Load(), "syncs": syncs.Load()}
}

func (f file) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)
	writes.Add(1)
	if n > 0 {
		writeBytes.Add(uint64(n))
	}
	return n, err
}

func (f file) Sync() error {
	err := f.File.Sync()
	syncs.Add(1)
	return err
}
