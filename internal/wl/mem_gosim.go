//go:build sim

package wl

// mmapBytes is memory on Go's heap under gosim, which maps nothing.
func mmapBytes(n int) ([]byte, error) { return make([]byte, n), nil }

func unixMunmap([]byte) error { return nil }

// pageSize and dropPages are the system's page and its madvise(DONTNEED):
// the pages read as zeros after.
const pageSize = 4096

func dropPages(b []byte) { clear(b) }
