// Copyright 2019 The Go Authors. All rights reserved.  Use of this source code
// is governed by a BSD-style license that can be found at
// https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

// Based on https://go.googlesource.com/go/+/refs/heads/master/src/internal/poll/errno_unix.go.

package syscallabi

import "syscall"

// Do the interface allocations only once for common
// Errno values.
var (
	errEAGAIN error = syscall.EAGAIN
	errEINVAL error = syscall.EINVAL
	errENOENT error = syscall.ENOENT
)

// ErrnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func ErrnoErr(e uintptr) error {
	switch syscall.Errno(e) {
	case 0:
		return nil
	case syscall.EAGAIN:
		return errEAGAIN
	case syscall.EINVAL:
		return errEINVAL
	case syscall.ENOENT:
		return errENOENT
	}
	return syscall.Errno(e)
}

func ErrErrno(err error) uintptr {
	if err == nil {
		return 0
	}
	syscallErr, ok := err.(syscall.Errno)
	if !ok {
		panic(syscallErr)
	}
	return uintptr(syscallErr)
}
