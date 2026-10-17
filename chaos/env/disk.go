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

// Disk is one node's disk. Data is the node's data directory: the LazyFS
// mount, or the ext4 file system itself without LazyFS.
type Disk struct {
	// Name is the device-mapper name.
	Name string
	// Data is where the node keeps its files.
	Data string
	// Backing is the ext4 mount, and root the directory under it that
	// LazyFS shows at Data.
	Backing string
	root    string
	dir     string
	img     string
	loop    string
	sectors int64
	created bool
	// mode is the device-mapper table in use: linear, error-writes,
	// delay or suspended, and stalled the one a suspension holds.
	mode, stalled string

	lazyfs  string
	fifo    string
	logfile string
	cfgfile string
	proc    *exec.Cmd
	exited  chan error
}

// NewDisk makes a disk of size bytes under dir, named name, with LazyFS on
// top when lazyfs names its binary.
func NewDisk(dir, name string, size int64, lazyfs string) (_ *Disk, err error) {
	if dir, err = filepath.Abs(dir); err != nil {
		return nil, err
	}
	d := &Disk{Name: name, dir: dir, img: filepath.Join(dir, name+".img"), Backing: filepath.Join(dir, name+".ext4"), lazyfs: lazyfs, mode: "linear"}
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
	d.root = filepath.Join(d.Backing, "root")
	if err := os.MkdirAll(d.root, 0o755); err != nil {
		return nil, err
	}
	if lazyfs == "" {
		d.Data = d.root
		return d, nil
	}
	d.Data = filepath.Join(dir, name+".data")
	d.fifo = filepath.Join(dir, name+".fifo")
	d.logfile = filepath.Join(dir, name+".lazyfs.log")
	d.cfgfile = filepath.Join(dir, name+".lazyfs.toml")
	cfg := fmt.Sprintf("[faults]\nfifo_path=%q\n[cache]\napply_eviction=false\n[cache.simple]\ncustom_size=\"%dmb\"\nblocks_per_page=1\n[filesystem]\nlog_all_operations=false\nlogfile=%q\n",
		d.fifo, max(64, size>>20), d.logfile)
	if err := os.WriteFile(d.cfgfile, []byte(cfg), 0o644); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(d.Data, 0o755); err != nil {
		return nil, err
	}
	if err := d.mountLazyFS(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Disk) device() string { return "/dev/mapper/" + d.Name }

func (d *Disk) table(kind string, args ...string) string {
	return strings.Join(append([]string{"0", strconv.FormatInt(d.sectors, 10), kind, d.loop, "0"}, args...), " ")
}

// mountLazyFS starts LazyFS on Data and waits for the mount.
func (d *Disk) mountLazyFS() error {
	out, err := os.OpenFile(filepath.Join(d.dir, d.Name+".lazyfs.out"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// LazyFS ignores a config path with "-o" in it, in any case, and
	// looks for config/default.toml instead; a CI run's directory had
	// chaos-out in it. So it gets the file's name, relative to its working
	// directory, the disk's.
	cfg := filepath.Base(d.cfgfile)
	if strings.Contains(strings.ToLower(cfg), "-o") {
		return errors.Join(fmt.Errorf("LazyFS cannot take the config path %s, which has -o in it", cfg), out.Close())
	}
	cmd := exec.Command(d.lazyfs, d.Data, "--config-path", cfg, "-o", "modules=subdir", "-o", "subdir="+d.root, "-f")
	cmd.Dir = filepath.Dir(d.cfgfile)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		return errors.Join(err, out.Close())
	}
	d.proc = cmd
	exited := make(chan error, 1)
	d.exited = exited
	go func() {
		exited <- cmd.Wait()
		_ = out.Close()
	}()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if ok, err := mounted(d.Data); ok || err != nil {
			return err
		}
		if time.Now().After(deadline) {
			b, err := os.ReadFile(out.Name())
			if len(b) > 2000 {
				b = b[len(b)-2000:]
			}
			return errors.Join(fmt.Errorf("LazyFS did not mount %s within 10s; it wrote: %s", d.Data, strings.TrimSpace(string(b))), err)
		}
	}
}

// stopLazyFS unmounts LazyFS, lazily if it is gone already, and waits for
// its process.
func (d *Disk) stopLazyFS() error {
	if d.proc == nil {
		return nil
	}
	ok, err := mounted(d.Data)
	errs := []error{err}
	if ok {
		if _, err := runIn("", "fusermount3", "-uz", d.Data); err != nil {
			errs = append(errs, err)
		}
	}
	select {
	case <-d.exited:
	case <-time.After(10 * time.Second):
		errs = append(errs, d.proc.Process.Kill())
		<-d.exited
	}
	d.proc = nil
	return errors.Join(errs...)
}

// command sends a command to LazyFS's fault FIFO.
func (d *Disk) command(cmd string) error {
	f, err := os.OpenFile(d.fifo, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(cmd + "\n")
	return errors.Join(err, f.Close())
}

// lazyLog returns the LazyFS log.
func (d *Disk) lazyLog() string {
	b, _ := os.ReadFile(d.logfile)
	return string(b)
}

// PowerLoss drops every write that no fsync covered, as a power cut does.
// The node must be stopped first. It returns LazyFS's report of the
// unsynced bytes it dropped.
func (d *Disk) PowerLoss() (string, error) {
	if d.proc == nil {
		return "", errors.New("power loss needs LazyFS")
	}
	before := len(d.lazyLog())
	if err := d.command("lazyfs::clear-cache"); err != nil {
		return "", err
	}
	var report string
	if !waitFor(10*time.Second, func() bool {
		report = d.lazyLog()[before:]
		return strings.Contains(report, "cache is cleared")
	}) {
		return report, errors.New("LazyFS did not report the cache cleared within 10s")
	}
	return report, nil
}

// TornWrite makes the next write to file, a path under Data, persist only
// the first of three parts, after which LazyFS crashes, as a power cut in
// the middle of the write would. Remount brings the disk back.
func (d *Disk) TornWrite(file string) error {
	if d.proc == nil {
		return errors.New("a torn write needs LazyFS")
	}
	rel, err := filepath.Rel(d.Data, file)
	if err != nil {
		return err
	}
	return d.command(fmt.Sprintf("lazyfs::torn-op::file=%s::persist=1::parts=3::occurrence=1", filepath.Join(d.root, rel)))
}

// Crashed reports whether LazyFS crashed, as after a torn write.
func (d *Disk) Crashed() bool {
	if d.proc == nil {
		return false
	}
	select {
	case err := <-d.exited:
		d.exited <- err
		return true
	default:
		return false
	}
}

// Remount mounts LazyFS again after a crash, with what reached the disk.
func (d *Disk) Remount() error {
	if d.lazyfs == "" {
		return nil
	}
	if err := d.stopLazyFS(); err != nil {
		return err
	}
	return d.mountLazyFS()
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
	if err := d.stopLazyFS(); err != nil {
		return err
	}
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
	if _, err := sudo("mount", d.device(), d.Backing); err != nil {
		return err
	}
	if d.lazyfs == "" {
		return nil
	}
	return d.mountLazyFS()
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

// DropCaches drops what LazyFS holds, unsynced writes too, as a power
// loss would, and the kernel's page cache, so that the node's next reads
// come from the device. The node must be stopped.
func (d *Disk) DropCaches() error {
	if d.proc != nil {
		if _, err := d.PowerLoss(); err != nil {
			return err
		}
	}
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
	errs = append(errs, d.stopLazyFS())
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
