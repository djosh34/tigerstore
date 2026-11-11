//go:build sim

package osfs

type direct struct{}

func openDirect(string) (*direct, error) { return nil, nil }

func (d *direct) writeAt([]byte, int64, func([]byte, int64) (int, error)) error { return nil }

func (d *direct) close() error { return nil }

func segmentFile(string) bool { return false }

func preallocate(int) error { return nil }

const directIO = false
