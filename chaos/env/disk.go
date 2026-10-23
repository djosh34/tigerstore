package env

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Disk is one node's disk.
type Disk struct {
	// Name is the device-mapper name.
	Name string
	// Data is where the node keeps its files, a directory on Backing.
	Data string
	// Backing is the ext4 mount.
	Backing string
	img     string
	loop    string
	sectors int64
	created bool
	// mode is the device-mapper table in use: linear, error-writes,
	// delay or suspended, and stalled the one a suspension holds.
	mode, stalled string
}

// NewDisk makes a disk of size bytes under dir, named name.
func NewDisk(dir, name string, size int64) (_ *Disk, err error) {
	if dir, err = filepath.Abs(dir); err != nil {
		return nil, err
	}
	d := &Disk{Name: name, img: filepath.Join(dir, name+".img"), Backing: filepath.Join(dir, name+".ext4"), mode: "linear"}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(d.img)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(f.Truncate(size), f.Close()); err != nil {
		return nil, err
	}
	out, err := sudo("losetup", "-f", "--show", d.img)
	if err != nil {
		return nil, err
	}
	d.loop = strings.TrimSpace(out)
	out, err = sudo("blockdev", "--getsz", d.loop)
	if err != nil {
		return nil, err
	}
	if d.sectors, err = strconv.ParseInt(strings.TrimSpace(out), 10, 64); err != nil {
		return nil, err
	}
	if _, err := sudo("dmsetup", "create", name, "--table", d.table("linear")); err != nil {
		return nil, err
	}
	d.created = true
	// No blocks reserved for root: the node, not root, fills the disk.
	if _, err := sudo("mkfs.ext4", "-q", "-F", "-m", "0", d.device()); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(d.Backing, 0o755); err != nil {
		return nil, err
	}
	if _, err := sudo("mount", d.device(), d.Backing); err != nil {
		return nil, err
	}
	if _, err := sudo("chown", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), d.Backing); err != nil {
		return nil, err
	}
	d.Data = filepath.Join(d.Backing, "root")
	if err := os.MkdirAll(d.Data, 0o755); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Disk) device() string { return "/dev/mapper/" + d.Name }

func (d *Disk) table(kind string, args ...string) string {
	return strings.Join(append([]string{"0", strconv.FormatInt(d.sectors, 10), kind, d.loop, "0"}, args...), " ")
}

// FailWrites makes every write to the device fail, so an fsync fails.
func (d *Disk) FailWrites() error {
	return d.load("error-writes", d.table("flakey", "0", "3600", "1", "error_writes"))
}

// Delay delays every read and write by the given time.
func (d *Disk) Delay(read, write time.Duration) error {
	return d.load("delay", strings.Join([]string{"0", strconv.FormatInt(d.sectors, 10), "delay", d.loop, "0", strconv.FormatInt(read.Milliseconds(), 10), d.loop, "0", strconv.FormatInt(write.Milliseconds(), 10)}, " "))
}

// Healthy makes the device linear again and resumes it if stalled.
func (d *Disk) Healthy() error {
	if d.mode == "suspended" {
		if _, err := sudo("dmsetup", "resume", d.Name); err != nil {
			return err
		}
		d.mode = d.stalled
	}
	if d.mode == "linear" {
		return nil
	}
	return d.load("linear", d.table("linear"))
}

func (d *Disk) load(mode, table string) error {
	// Without flushing first: a table that fails writes would fail the
	// flush, and the switch with it.
	if d.mode != "suspended" {
		if _, err := sudo("dmsetup", "suspend", "--noflush", "--nolockfs", d.Name); err != nil {
			return err
		}
	}
	if _, err := sudo("dmsetup", "load", d.Name, "--table", table); err != nil {
		return err
	}
	if _, err := sudo("dmsetup", "resume", d.Name); err != nil {
		return err
	}
	d.mode = mode
	return nil
}

// Repair checks and mounts the file system again, as after failed
// writes, which may have made ext4 abort its journal and go read-only.
// The node must be stopped.
func (d *Disk) Repair() error {
	if ok, err := mounted(d.Backing); err != nil {
		return err
	} else if ok {
		if _, err := sudo("umount", d.Backing); err != nil {
			return err
		}
	}
	// e2fsck exits 1 or 2 when it fixed something.
	if out, err := sudo("e2fsck", "-fy", d.device()); err != nil {
		if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() > 2 {
			return fmt.Errorf("%w: %s", err, out)
		}
	}
	_, err := sudo("mount", d.device(), d.Backing)
	return err
}

// Stall stops every I/O to the device until Healthy.
func (d *Disk) Stall() error {
	// --nolockfs keeps the file system from being frozen first, which
	// would wait for the writes that the stall is to hold.
	if _, err := sudo("dmsetup", "suspend", "--nolockfs", d.Name); err != nil {
		return err
	}
	d.stalled, d.mode = d.mode, "suspended"
	return nil
}

// Status is the device-mapper status and the file system's free bytes.
func (d *Disk) Status() (string, int64, error) {
	out, err := sudo("dmsetup", "status", d.Name)
	if err != nil {
		return out, 0, err
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(d.Backing, &st); err != nil {
		return out, 0, err
	}
	return strings.TrimSpace(out), int64(st.Bavail) * st.Bsize, nil
}

// Fill takes every free byte of the file system with a file of its own,
// so the node's next allocation fails with ENOSPC; Unfill removes it.
func (d *Disk) Fill() (int64, error) {
	_, free, err := d.Status()
	if err != nil {
		return 0, err
	}
	f, err := os.Create(filepath.Join(d.Backing, "filler"))
	if err != nil {
		return 0, err
	}
	n := int64(0)
	for step := free; step >= 4096; step /= 2 {
		if err := syscall.Fallocate(int(f.Fd()), 0, n, step); err == nil {
			n += step
		}
	}
	return n, f.Close()
}

// Unfill removes the file Fill made.
func (d *Disk) Unfill() error {
	err := os.Remove(filepath.Join(d.Backing, "filler"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// DropCaches writes back and drops the kernel's page cache, so that the
// node's next reads come from the device. The node must be stopped.
func (d *Disk) DropCaches() error {
	if _, err := sudo("sync"); err != nil {
		return err
	}
	return writeRoot("/proc/sys/vm/drop_caches", "3")
}

// Erase removes every file the node had, as a new disk would have none.
// The node must be stopped.
func (d *Disk) Erase() error {
	entries, err := os.ReadDir(d.Data)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(d.Data, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Close takes the disk apart.
func (d *Disk) Close() error {
	var errs []error
	if d.mode != "linear" && d.created {
		errs = append(errs, d.Healthy())
	}
	ok, err := mounted(d.Backing)
	errs = append(errs, err)
	if ok {
		_, err := sudo("umount", d.Backing)
		errs = append(errs, err)
	}
	if d.created {
		if _, err := sudo("dmsetup", "remove", d.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if d.loop != "" {
		_, err := sudo("losetup", "-d", d.loop)
		errs = append(errs, err)
	}
	errs = append(errs, os.Remove(d.img))
	return errors.Join(errs...)
}
