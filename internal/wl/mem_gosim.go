//go:build sim

package wl

// mmapBytes is memory on Go's heap under gosim, which maps nothing.
func mmapBytes(n int) ([]byte, error) { return make([]byte, n), nil }

func unixMunmap([]byte) error { return nil }
