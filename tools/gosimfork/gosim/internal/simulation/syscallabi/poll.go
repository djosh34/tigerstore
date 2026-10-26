package syscallabi

import (
	"sync"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

// TODO: clean/change this API?
// - document who owns what fields more clearly?
// - document which functions are called from os and which from userspace?
// - race.Disable/race.Enable to prevent dependency edges?
// - try to get rid of norace?

// PollDesc is a poll descriptor like the go runtime's netpoll descriptors.
//
// Blocking operations pass poll descriptors to the OS, which tracks those
// descriptors and marks them as readable or writable using a Pollers.
type PollDesc struct {
	fd              int
	registeredIndex int

	mu sync.Mutex

	readable gosimruntime.Uint32Futex
	writable gosimruntime.Uint32Futex

	readTimer   *time.Timer
	readTimeout time.Time

	writeTimer   *time.Timer
	writeTimeout time.Time // XXX: need this? implicit in timer somehow?
}

func (pd *PollDesc) FD() int {
	return pd.fd
}

var freePollDescs []*PollDesc

//go:norace
func AllocPollDesc(fd int) *PollDesc {
	if len(freePollDescs) == 0 {
		desc := &PollDesc{
			fd:              fd,
			registeredIndex: -1,
		}
		return desc
	} else {
		desc := freePollDescs[len(freePollDescs)-1]
		freePollDescs = freePollDescs[:len(freePollDescs)-1]
		desc.fd = fd
		return desc
	}
}

//go:norace
func (pd *PollDesc) Close() {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	if pd.registeredIndex != -1 {
		// make sure we are unhooked on the sim side
		panic(pd)
	}

	pd.fd = -1
	pd.readable.Set(0)
	pd.writable.Set(0)
	pd.readTimeout = time.Time{}
	if pd.readTimer != nil {
		pd.readTimer.Stop()
	}
	pd.writeTimeout = time.Time{}
	if pd.writeTimer != nil {
		pd.writeTimer.Stop()
	}

	freePollDescs = append(freePollDescs, pd)
}

//go:norace
func (pd *PollDesc) Reset(mode int) int {
	// called before wait

	var value uint32
	switch mode {
	case 'r':
		value = pd.readable.Get()
	case 'w':
		value = pd.writable.Get()
	default:
		panic(mode)
	}

	switch {
	case value&(1<<pollErrClosing) != 0:
		return pollErrClosing
	case value&(1<<pollErrTimeout) != 0:
		return pollErrTimeout
	}

	return pollNoError
}

//go:norace
func (pd *PollDesc) Wait(mode int) int {
	var value uint32
	switch mode {
	case 'r':
		value = pd.readable.WaitNonzero()
	case 'w':
		value = pd.writable.WaitNonzero()
	default:
		panic(mode)
	}

	switch {
	case value&(1<<pollErrClosing) != 0:
		return pollErrClosing
	case value&(1<<pollErrTimeout) != 0:
		return pollErrTimeout
	case value&(1<<pollNoError) != 0:
		return pollNoError
	default:
		panic(value)
	}
}

//go:norace
func (pd *PollDesc) SetDeadline(d int64, mode int) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	var deadline time.Time
	if d != 0 {
		deadline = time.Now().Add(time.Duration(d))
	}

	if mode == 'r' || mode == 'r'+'w' {
		pd.readTimeout = deadline
		pd.readable.SetBit(pollErrTimeout, d < 0)
		if d > 0 {
			if pd.readTimer == nil {
				pd.readTimer = time.AfterFunc(time.Duration(d), func() {
					pd.mu.Lock()
					defer pd.mu.Unlock()
					if !pd.readTimeout.IsZero() && !time.Now().Before(pd.readTimeout) {
						pd.readable.SetBit(pollErrTimeout, true)
					}
				})
			} else {
				pd.readTimer.Reset(time.Duration(d))
			}
		} else {
			if pd.readTimer != nil {
				pd.readTimer.Stop()
			}
		}
	}
	if mode == 'w' || mode == 'r'+'w' {
		pd.writeTimeout = deadline
		pd.writable.SetBit(pollErrTimeout, d < 0)
		if d > 0 {
			if pd.writeTimer == nil {
				pd.writeTimer = time.AfterFunc(time.Duration(d), func() {
					pd.mu.Lock()
					defer pd.mu.Unlock()
					if !pd.writeTimeout.IsZero() && !time.Now().Before(pd.writeTimeout) {
						pd.writable.SetBit(pollErrTimeout, true)
					}
				})
			} else {
				pd.writeTimer.Reset(time.Duration(d))
			}
		} else {
			if pd.writeTimer != nil {
				pd.writeTimer.Stop()
			}
		}
	}
}

// Error values returned by runtime_pollReset and runtime_pollWait.
// These must match the values in runtime/netpoll.go.
const (
	pollNoError        = 0
	pollErrClosing     = 1
	pollErrTimeout     = 2
	pollErrNotPollable = 3
)

//go:norace
func (pd *PollDesc) Unblock() {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// logf("unblock %d", ctx.fd)

	pd.readable.SetBit(pollErrClosing, true)
	pd.writable.SetBit(pollErrClosing, true)
}

type Pollers struct {
	canWrite bool
	canRead  bool
	pollers  []*PollDesc
}

//go:norace
func (ps *Pollers) Add(p *PollDesc) {
	if p.registeredIndex != -1 {
		panic("help poller already registered")
	}
	p.writable.SetBit(pollNoError, ps.canWrite)
	p.readable.SetBit(pollNoError, ps.canRead)
	p.registeredIndex = len(ps.pollers)
	ps.pollers = append(ps.pollers, p)
}

//go:norace
func (ps *Pollers) Remove(p *PollDesc) {
	if ps.pollers[p.registeredIndex] != p {
		panic("help")
	}
	last := len(ps.pollers) - 1
	if last != p.registeredIndex {
		ps.pollers[p.registeredIndex] = ps.pollers[last]
		ps.pollers[p.registeredIndex].registeredIndex = p.registeredIndex
	}
	ps.pollers[last] = nil
	ps.pollers = ps.pollers[:last]
	p.registeredIndex = -1
}

func (ps *Pollers) NotifyCanRead(can bool) {
	if ps.canRead == can {
		return
	}
	ps.canRead = can
	for _, p := range ps.pollers {
		p.readable.SetBit(pollNoError, ps.canRead)
	}
}

func (ps *Pollers) NotifyCanWrite(can bool) {
	if ps.canWrite == can {
		return
	}
	ps.canWrite = can
	for _, p := range ps.pollers {
		p.writable.SetBit(pollNoError, ps.canWrite)
	}
}
