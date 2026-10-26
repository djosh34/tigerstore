//go:build !linkname

package coro

import (
	_ "unsafe"
)

// Coro is an implementation of Coro in coro_linkname.go without using linkname
// to access the go runtime's internals.
type Coro struct {
	runWaitCh chan struct{}
}

//go:norace
func (c *Coro) Start(f func()) {
	c.runWaitCh = make(chan struct{})
	go f()
	<-c.runWaitCh
}

//go:norace
func (c *Coro) Next() {
	c.runWaitCh <- struct{}{}
	<-c.runWaitCh
}

//go:norace
func (c *Coro) Yield() {
	c.runWaitCh <- struct{}{}
	<-c.runWaitCh
}

//go:norace
func (c *Coro) Finish() {
	c.runWaitCh <- struct{}{}
	select {}
	panic("wtf")
}
