//go:build sim

package osfs

// DropCache does nothing under gosim, whose file system keeps no page
// cache that a failed writeback could leave pages in, and implements
// neither fadvise nor mincore. The simulator's own disks model the drop
// (simfs.Handle.DropCache).
func (f file) DropCache() error { return nil }
