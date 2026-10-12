//go:build faultfs

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// One JSON request per connection. Paths must exactly match the opened file.
// Install is acknowledged under the same lock that claims the target operation.
// Rules are one-shot, IDs cannot be reused, and no lock is held during I/O/stall.
type request struct {
	Cmd    string `json:"cmd"`
	ID     string `json:"id,omitempty"`
	Path   string `json:"path,omitempty"`
	Op     string `json:"op,omitempty"`
	Action string `json:"action,omitempty"`
	Nth    int    `json:"nth,omitempty"`
	Cut    int    `json:"cut,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Data   string `json:"data,omitempty"`
}

type event struct {
	Seq    int    `json:"seq"`
	Phase  string `json:"phase"`
	ID     string `json:"id"`
	Path   string `json:"path"`
	Op     string `json:"op"`
	Action string `json:"action"`
	N      int    `json:"n"`
	Err    string `json:"error,omitempty"`
}

type reply struct {
	OK          bool    `json:"ok"`
	PID         int     `json:"pid"`
	Incarnation string  `json:"incarnation"`
	N           int     `json:"n"`
	Err         string  `json:"error,omitempty"`
	Errno       int     `json:"errno,omitempty"`
	Events      []event `json:"events,omitempty"`
}

type rule struct {
	request
	seen     int
	gate     chan struct{}
	released bool
}

type disk struct {
	f           *os.File
	mu          sync.Mutex
	active      *rule
	rules       map[string]*rule
	events      []event
	incarnation string
}

func text(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (d *disk) record(phase string, r *rule, n int, err error) {
	d.events = append(d.events, event{len(d.events) + 1, phase, r.ID, r.Path, r.Op, r.Action, n, text(err)})
}

func (d *disk) claim(op string) *rule {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.active
	if r == nil || r.Op != op {
		return nil
	}
	r.seen++
	if r.seen != r.Nth {
		return nil
	}
	d.active = nil
	return r
}

func (d *disk) fired(r *rule, n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("fired", r, n, err)
}

// WriteAt changes a real file before reporting the shortened result.
func (d *disk) WriteAt(p []byte, offset int64) (int, error) {
	r := d.claim("write")
	if r == nil {
		return d.f.WriteAt(p, offset)
	}
	cut := min(r.Cut, len(p))
	n, err := d.f.WriteAt(p[:cut], offset)
	if err == nil {
		if r.Action == "enospc" {
			err = syscall.ENOSPC
		} else {
			err = syscall.EIO
		}
	}
	d.fired(r, n, err)
	return n, err
}

func (d *disk) Sync() error {
	r := d.claim("sync")
	if r == nil {
		return d.f.Sync()
	}
	if r.Action == "error" {
		d.fired(r, 0, syscall.EIO)
		return syscall.EIO
	}
	// This blocks the disk worker before the real fsync syscall, not the kernel.
	d.fired(r, 0, nil)
	<-r.gate
	err := d.f.Sync()
	d.mu.Lock()
	d.record("completed", r, 0, err)
	d.mu.Unlock()
	return err
}

func (d *disk) handle(q request) reply {
	out := reply{PID: os.Getpid(), Incarnation: d.incarnation}
	switch q.Cmd {
	case "install":
		if q.ID == "" || q.Path != d.f.Name() || q.Nth < 1 || q.Cut < 0 ||
			!((q.Op == "write" && (q.Action == "tear" || q.Action == "enospc")) ||
				(q.Op == "sync" && (q.Action == "error" || q.Action == "stall"))) {
			out.Err = "invalid rule or path"
			break
		}
		d.mu.Lock()
		if d.active != nil {
			out.Err = "another rule is armed"
		} else if d.rules[q.ID] != nil {
			out.Err = "duplicate rule ID"
		} else {
			r := &rule{request: q, gate: make(chan struct{})}
			d.rules[q.ID], d.active = r, r
			d.record("installed", r, 0, nil)
		}
		d.mu.Unlock()
	case "status":
		d.mu.Lock()
		out.Events = append([]event(nil), d.events...)
		d.mu.Unlock()
	case "release":
		d.mu.Lock()
		r := d.rules[q.ID]
		if r == nil || r.Action != "stall" || r.seen < r.Nth || r.released {
			out.Err = "no fired unreleased stall with that ID"
		} else {
			close(r.gate)
			r.released = true
		}
		d.mu.Unlock()
	case "io":
		if q.Path != d.f.Name() {
			out.Err = "wrong path"
			break
		}
		var err error
		switch q.Op {
		case "write":
			out.N, err = d.WriteAt([]byte(q.Data), q.Offset)
		case "sync":
			err = d.Sync()
		default:
			err = errors.New("invalid I/O operation")
		}
		out.Err = text(err)
		if errors.Is(err, syscall.EIO) {
			out.Errno = int(syscall.EIO)
		}
		if errors.Is(err, syscall.ENOSPC) {
			out.Errno = int(syscall.ENOSPC)
		}
	default:
		out.Err = "unknown command"
	}
	out.OK = out.Err == ""
	return out
}

func serve(f *os.File, path string) error {
	// Refuse stale socket paths rather than removing an unrelated file.
	// Set permissions at creation. chmod on a Unix socket returns EINVAL on
	// this host's virtiofs work mount. This changes only the worker's umask.
	syscall.Umask(0077)
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer l.Close()
	d := &disk{f: f, rules: map[string]*rule{}, incarnation: fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())}
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer c.Close()
			var q request
			if err := json.NewDecoder(c).Decode(&q); err != nil {
				return
			}
			// The prototype's controller uses a hard timeout. A stall intentionally
			// has no server timeout; other connections still accept control calls.
			if err := json.NewEncoder(c).Encode(d.handle(q)); err != nil {
				fmt.Fprintln(os.Stderr, "reply:", err)
			}
		}()
	}
}
