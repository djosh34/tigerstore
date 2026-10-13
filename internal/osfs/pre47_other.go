//go:build !linux

package osfs

import "os"

func pre47FileSync(f *os.File) error { return f.Sync() }
