package syscallabi

import (
	"runtime"
	"unsafe"

	"github.com/jellevandenhooff/gosim/gosimruntime"
	"github.com/jellevandenhooff/gosim/internal/race"
)

// Marked go:norace so simulation can read Goroutine.
//
//go:norace
func allocGoroutine(goroutineId int) unsafe.Pointer {
	return unsafe.Pointer(&Syscall{
		Goroutine: goroutineId,
	})
}

// Setup configures the gosim runtime to allocate a Syscall struct
// for each goroutine.
func Setup() {
	gosimruntime.SetSyscallAllocator(allocGoroutine)
}

// GetGoroutineLocalSyscall returns the per-goroutine pre-allocated
// Syscall struct.
//
// Sharing Syscalls is safe because each goroutine invokes at most
// one syscall at a time.
//
// TODO: Just pool them instead?
//
//go:norace
func GetGoroutineLocalSyscall() *Syscall {
	syscall := (*Syscall)(gosimruntime.GetGoroutineLocalSyscall())

	var pc [1]uintptr
	runtime.Callers(3, pc[:])
	syscall.PC = pc[0]

	return syscall
}

// Syscall holds the arguments and return values of gosim syscalls.
//
// Syscalls are calls from user code to high-level system calls in the os
// package. To prevent allocations, each goroutine has a single Syscall that
// gets reused and be retrieved using GetGoroutineLocalSyscall.
//
// Syscalls are different from upcalls, which calls in this package from user
// goroutines that execute code on the scheduler goroutine.
type Syscall struct {
	Trampoline func(*Syscall)
	OS         any

	Goroutine int
	PC        uintptr
	Step      int

	Int0, Int1, Int2, Int3, Int4, Int5 uintptr
	Ptr0, Ptr1, Ptr2, Ptr3, Ptr4       any
	R0, R1                             uintptr
	RPtr0                              any
	Errno                              uintptr

	Sema uint32
}

// Wait waits for the system call to be completed.
func (u *Syscall) Wait() {
	gosimruntime.Semacquire(&u.Sema, false)
}

// Complete marks the system call completed and lets Wait return.
func (u *Syscall) Complete() {
	gosimruntime.Semrelease(&u.Sema)
}

// The Dispatcher is the bridge between user space code and OS code. User space
// goroutines call Dispatcher.Dispatch and the dispatcher runs Syscall.Trap on
// the OS goroutine.
type Dispatcher struct {
	syscalls chan *Syscall
}

// NewDispatcher creates a new Dispatcher.
func NewDispatcher() Dispatcher {
	return Dispatcher{
		syscalls: make(chan *Syscall),
	}
}

// handleSyscall is a wrapper around HandleSyscall, extracted from Run
// to mark it norace.
//
//go:norace
func handleSyscall(syscall *Syscall) {
	syscall.Trampoline(syscall)
}

func (b Dispatcher) Run() {
	for syscall := range b.syscalls {
		// for-over-func erases go:norace inside the for loop, so
		// use a helper function
		handleSyscall(syscall)
	}
}

//go:norace
func (b Dispatcher) Dispatch(syscall *Syscall) {
	// XXX: how does this interact with the deps we set up (maybe) in yield?
	// XXX: should we instead not create the deps in Select?
	race.Disable()
	b.syscalls <- syscall
	// XXX: HELP what happens if this is reused from a crashed goroutine???
	syscall.Wait()
	race.Enable()
}

// BoolToUintptr stores a boolean as a uintptr for syscall arguments
// or return values.
func BoolToUintptr(v bool) uintptr {
	if v {
		return 1
	}
	return 0
}

// BoolFromUintptr extracts a boolean stored as a uintptr for syscall
// arguments or return values.
func BoolFromUintptr(v uintptr) bool {
	return v != 0
}

// TODO: for the Views below we could register read/write race detector deps in
// userspace?

// SliceView is a OS-wrapper around slices passed from user space.  It lets
// the OS access user space memory without triggering the race detector.
type SliceView[T any] struct {
	Ptr []T
}

func NewSliceView[T any](ptr *T, len uintptr) SliceView[T] {
	return SliceView[T]{
		Ptr: unsafe.Slice(ptr, len),
	}
}

func (b SliceView[T]) Len() int {
	return len(b.Ptr)
}

//go:norace
func (b SliceView[T]) Read(into []T) int {
	n := len(into)
	if len(b.Ptr) < n {
		n = len(b.Ptr)
	}
	for i := range b.Ptr[:n] {
		into[i] = b.Ptr[i]
	}
	return n
}

//go:norace
func (b SliceView[T]) Write(from []T) int {
	n := len(b.Ptr)
	if len(from) < n {
		n = len(from)
	}
	for i := range from[:n] {
		b.Ptr[i] = from[i]
	}
	return n
}

func (b SliceView[T]) SliceFrom(from int) SliceView[T] {
	return SliceView[T]{Ptr: b.Ptr[from:]}
}

func (b SliceView[T]) Slice(from, to int) SliceView[T] {
	return SliceView[T]{Ptr: b.Ptr[from:to]}
}

type ByteSliceView = SliceView[byte]

// ValueView is a OS-wrapper around pointers passed from user space. It lets the
// OS access user space memory without triggering the race detector.
type ValueView[T any] struct {
	underlying *T
}

func NewValueView[T any](ptr *T) ValueView[T] {
	return ValueView[T]{
		underlying: ptr,
	}
}

func (s ValueView[T]) UnsafePointer() unsafe.Pointer {
	return unsafe.Pointer(s.underlying)
}

//go:norace
func (s ValueView[T]) Get() T {
	return *s.underlying
}

//go:norace
func (s ValueView[T]) Set(v T) {
	*s.underlying = v
}
