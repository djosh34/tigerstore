package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/djosh34/tigerstore/internal/wal"
)

// The log's directory keeps the number of the node's last run in runFile,
// which core.Config.Run takes, so that a node's runs are ordered (#174).
const runFile, runTmp = "run", "run.tmp"

// nextRun raises the run number that dir keeps and returns it, once it is
// durable.
func nextRun(dir wal.FS) (uint64, error) {
	var last uint64
	f, err := dir.Open(runFile)
	switch {
	case err == nil:
		b, err := f.ReadAll()
		if err = errors.Join(err, f.Close()); err != nil {
			return 0, err
		}
		// LazyFS can give a new file the size of a removed one, so the run
		// number comes back followed by zeros (#240).
		if len(b) < 8 || slices.ContainsFunc(b[8:], func(c byte) bool { return c != 0 }) {
			return 0, fmt.Errorf("node: %s holds %d bytes, not 8", runFile, len(b))
		}
		last = binary.LittleEndian.Uint64(b)
	case !errors.Is(err, fs.ErrNotExist):
		return 0, err
	}
	if err := dir.Remove(runTmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}
	t, err := dir.Create(runTmp)
	if err != nil {
		return 0, err
	}
	_, err = t.Write(binary.LittleEndian.AppendUint64(nil, last+1))
	if err = errors.Join(err, t.Sync(), t.Close()); err != nil {
		return 0, err
	}
	if err := dir.Rename(runTmp, runFile); err != nil {
		return 0, err
	}
	return last + 1, dir.SyncDir()
}
