//go:build !sim

package osfs

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// DropCache drops the file's clean pages from the page cache with
// fadvise(POSIX_FADV_DONTNEED), so that a later read returns what the
// disk holds: after a sync every page is clean, a page that a failed
// writeback marked clean among them (#372). Linux skips a page that is
// held, and fadvise still succeeds, so DropCache then maps the file and
// asks mincore whether any page stayed, which is ErrCached. A file system
// in memory has no disk to read back, and neither has a directory whose
// syncs are no-ops. The check holds on XFS and on ext4 in its default
// data=ordered mode; docs/design.md names where it does not.
func (f file) DropCache() error {
	if f.noSync {
		return nil
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	cerr := rc.Control(func(fd uintptr) {
		var sfs unix.Statfs_t
		if err = unix.Fstatfs(int(fd), &sfs); err != nil || sfs.Type == unix.TMPFS_MAGIC || sfs.Type == unix.RAMFS_MAGIC {
			return
		}
		if err = unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED); err != nil || st.Size() == 0 {
			return
		}
		err = cached(int(fd), st.Size())
	})
	return errors.Join(cerr, err)
}

// cached fails with ErrCached if a page of the size bytes of file fd is in
// the page cache. The mapping reads nothing, so it brings no page in.
func cached(fd int, size int64) error {
	m, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	vec := make([]byte, (size+int64(os.Getpagesize())-1)/int64(os.Getpagesize()))
	if err := errors.Join(mincore(m, vec), unix.Munmap(m)); err != nil {
		return err
	}
	n := 0
	for _, b := range vec {
		n += int(b & 1)
	}
	if n > 0 {
		return fmt.Errorf("%w: %d of %d pages", ErrCached, n, len(vec))
	}
	return nil
}

// mincore sets the low bit of vec[i] if page i of mapping m is in the page
// cache, as mincore(2) does; x/sys/unix has no wrapper for it.
func mincore(m, vec []byte) error {
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return errno
	}
	return nil
}
