// Copyright 2009 The Go Authors. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found at
// https://go.googlesource.com/go/+/refs/heads/master/LICENSE.

// Based on https://go.googlesource.com/go/+/refs/heads/master/src/runtime/sema.go.

package gosimruntime

import (
	"unsafe"
)

// TODO: use the go runtime treap plan instead?
type sema struct {
	ptr *uint32
	q   waitq

	next *sema
}

const semasSize = 251 // prime just like in runtime

var semas [semasSize]*sema

// TODO: store these structs on g instead or something?
var semaPool []*sema

// TODO: clean up freeing code (no need to do two lookups)

// cleanup/handle sema with empty sudog queue (can happen when crashing machines)
//
//go:norace
func maybefree(semaPtr **sema) {
	if *semaPtr == nil || (*semaPtr).q.first != nil {
		return
	}

	cur := *semaPtr
	for cur != nil && cur.q.first == nil {
		next := cur.next
		cur.next = nil
		cur.ptr = nil
		semaPool = append(semaPool, cur)
		cur = next
	}
	*semaPtr = cur
}

//go:norace
func getsema(addr *uint32, alloc bool) *sema {
	idx := (uintptr(unsafe.Pointer(addr)) >> 3) % semasSize

	maybefree(&semas[idx])
	cur := semas[idx]
	for cur != nil {
		if cur.ptr == addr {
			return cur
		}
		maybefree(&cur.next)
		cur = cur.next
	}

	if !alloc {
		return nil
	}

	if n := len(semaPool); n > 0 {
		cur = semaPool[n-1]
		semaPool = semaPool[:n-1]
	} else {
		cur = &sema{}
	}
	cur.ptr = addr
	cur.next = semas[idx]
	semas[idx] = cur

	return cur
}

//go:norace
func freesema(addr *uint32) {
	idx := (uintptr(unsafe.Pointer(addr)) >> 3) % semasSize

	if semas[idx].ptr == addr {
		freeing := semas[idx]
		semas[idx] = semas[idx].next
		freeing.ptr = nil
		freeing.next = nil
		semaPool = append(semaPool, freeing)
		return
	}

	cur := semas[idx]
	for {
		if cur.next.ptr == addr {
			freeing := cur.next
			cur.next = cur.next.next
			freeing.ptr = nil
			freeing.next = nil
			semaPool = append(semaPool, freeing)
			return
		}
		cur = cur.next
	}
}

// TODO: this needs tests

//go:norace
func Semacquire(addr *uint32, lifo bool) {
	g := getg()
	g.yield()

	if *addr > 0 {
		*addr--
		return
	}

	sema := getsema(addr, true)

	sudog := g.allocWaiter()
	if lifo {
		sema.q.enqueuelifo(sudog)
	} else {
		sema.q.enqueue(sudog)
	}
	g.wait()
	g.releaseWaiters()
}

//go:norace
func Semrelease(addr *uint32) {
	g := getg()
	g.yield()

	if *addr > 0 {
		*addr++
		return
	}

	sema := getsema(addr, false)
	if sema == nil {
		*addr++
		return
	}

	sema.q.dequeue()
	if sema.q.first == nil {
		freesema(addr)
	}
}

const AtomicYield = false

type NotifyList struct {
	wait, notify uint32
	q            waitq
	// TODO: nocopy?
}

//go:norace
func (l *NotifyList) Add() uint32 {
	// XXX: is the reordering important here?
	g := getg()
	g.yield()

	val := l.wait
	l.wait++
	return val
}

// less checks if a < b, considering a & b running counts that may overflow the
// 32-bit range, and that their "unwrapped" difference is always less than 2^31.
func less(a, b uint32) bool {
	return int32(a-b) < 0
}

//go:norace
func (l *NotifyList) Wait(t uint32) {
	g := getg()
	g.yield()

	if less(t, l.notify) {
		return
	}

	sudog := g.allocWaiter()
	sudog.index = int(t)
	l.q.enqueue(sudog)
	g.wait()
	g.releaseWaiters()
}

//go:norace
func (l *NotifyList) NotifyAll() {
	g := getg()
	g.yield()

	l.notify = l.wait
	for l.q.dequeue() != nil {
	}
}

//go:norace
func (l *NotifyList) NotifyOne() {
	g := getg()
	g.yield()

	if l.notify == l.wait {
		return
	}

	t := l.notify
	l.notify++

	for c := l.q.first; c != nil; c = c.next {
		if c.index == int(t) {
			l.q.remove(c)

			if c.goroutine.waiters.waitersNext != nil || c.goroutine.selected != nil {
				// other waiters should not exist, only happens in select {}
				panic(c)
			}
			c.goroutine.selected = c

			g.upcall(func() {
				c.goroutine.setWaiting(false)
			})
			break
		}
	}
}

type Uint32Futex struct {
	val uint32
	q   waitq
	// TODO: nocopy?
}

//go:norace
func (w *Uint32Futex) Get() uint32 {
	g := getg()
	g.yield()

	return w.val
}

//go:norace
func (w *Uint32Futex) Set(val uint32) {
	g := getg()
	g.yield()

	w.val = val
	if val != 0 {
		for sudog := w.q.dequeue(); sudog != nil; sudog = w.q.dequeue() {
			sudog.index = int(w.val)
		}
	}
}

//go:norace
func (w *Uint32Futex) SetBit(index int, value bool) {
	g := getg()
	g.yield()

	if value {
		w.val |= 1 << index
	} else {
		w.val &= ^(1 << index)
	}
	if w.val != 0 {
		for sudog := w.q.dequeue(); sudog != nil; sudog = w.q.dequeue() {
			sudog.index = int(w.val)
		}
	}
}

//go:norace
func (w *Uint32Futex) WaitNonzero() uint32 {
	g := getg()
	g.yield()

	if w.val != 0 {
		return w.val
	}
	sudog := g.allocWaiter()
	w.q.enqueue(sudog)
	g.wait()
	ret := uint32(sudog.index)
	g.releaseWaiters()
	return ret
}
